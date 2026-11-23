use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};
use usb_direct_transfer::{
    Command, ConnectionMonitor, Event, Options, benchmark, discard, doctor, pending, start,
};

#[derive(Parser)]
#[command(about = "Transfer files and folders over a direct USB-C cable")]
struct Cli {
    #[command(subcommand)]
    command: Action,
}

#[derive(Args)]
struct Link {
    /// Use this cable interface (see doctor).
    #[arg(long)]
    interface: Option<String>,
    /// Connect to this cable IP address. For IPv6 use the local interface scope if needed.
    #[arg(long)]
    peer: Option<String>,
}

impl From<Link> for Options {
    fn from(link: Link) -> Self {
        Self {
            interface: link.interface,
            peer: link.peer,
        }
    }
}

#[derive(Args)]
struct Diagnose {
    #[command(flatten)]
    link: Link,
    /// Watch USB, Thunderbolt, and cable network changes until interrupted.
    #[arg(long)]
    watch: bool,
    /// Stop watching after this many seconds.
    #[arg(long, requires = "watch", value_parser = clap::value_parser!(u64).range(1..=3600))]
    seconds: Option<u64>,
    /// Save the latest report and connection activity to this file.
    #[arg(long)]
    report: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Action {
    /// Inspect cable interfaces and test a peer address if supplied.
    Doctor(Diagnose),
    /// Measure raw TCP goodput on the cable. Run on both computers with the same size.
    Bench {
        #[command(flatten)]
        link: Link,
        #[arg(long, default_value_t = 512, value_parser = clap::value_parser!(u64).range(1..=16384))]
        mib: u64,
    },
    /// Pair and open a session for sending and receiving.
    Pair {
        #[command(flatten)]
        link: Link,
        #[arg(long)]
        to: PathBuf,
    },
    /// List unfinished transfers in a destination.
    Pending {
        #[arg(long)]
        to: PathBuf,
    },
    /// Remove the staged data for one unfinished transfer.
    Discard {
        id: String,
        #[arg(long)]
        to: PathBuf,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Action::Doctor(args) => diagnose(args)?,
        Action::Bench { link, mib } => {
            let result = benchmark(
                &link.into(),
                &AtomicBool::new(false),
                mib * 1024 * 1024,
                |status| {
                    println!("{status}");
                },
            )?;
            println!("{result}");
        }
        Action::Pair { link, to } => interactive(link.into(), &to)?,
        Action::Pending { to } => list_pending(&to)?,
        Action::Discard { id, to } => {
            discard(&to, &id)?;
            println!("Discarded {id}");
        }
    }
    Ok(())
}

fn diagnose(args: Diagnose) -> Result<()> {
    if !args.watch {
        let report = doctor(&args.link.into())?;
        if let Some(path) = args.report {
            std::fs::write(path, &report)?;
        }
        println!("{report}");
        return Ok(());
    }
    let options: Options = args.link.into();
    if options.peer.is_some() || options.interface.is_some() {
        println!("{}", doctor(&options)?);
        println!("\nLive monitoring covers all direct cable interfaces. Peer probes run once.");
    }
    let mut monitor = ConnectionMonitor::default();
    let started = Instant::now();
    let mut previous = None;
    println!("Watching USB and Thunderbolt connections. Reconnect the Mac; press Ctrl+C to stop.");
    loop {
        let report = monitor.poll().text();
        if previous.as_ref() != Some(&report) {
            println!("\n{report}");
            io::stdout().flush()?;
            if let Some(path) = &args.report {
                std::fs::write(path, &report)?;
            }
            previous = Some(report);
        }
        let wait = if let Some(seconds) = args.seconds {
            let remaining = Duration::from_secs(seconds).saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            remaining.min(Duration::from_secs(1))
        } else {
            Duration::from_secs(1)
        };
        thread::sleep(wait);
    }
    Ok(())
}

fn list_pending(to: &Path) -> Result<()> {
    let entries = pending(to)?;
    if entries.is_empty() {
        println!("No pending transfers.");
    }
    for entry in entries {
        println!("{}  {} bytes stored", entry.id, entry.bytes);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Prompt {
    Connecting,
    Pairing,
    Ready,
    Incoming,
}

fn interactive(options: Options, to: &Path) -> Result<()> {
    let to = to.canonicalize()?;
    if !to.is_dir() {
        bail!("Choose an existing destination directory.");
    }
    let session = start(options, to.clone());
    let (input, lines) = mpsc::channel();
    thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            match line {
                Ok(line) if input.send(line.clone()).is_ok() => {}
                _ => return,
            }
        }
    });

    let mut prompt = Prompt::Connecting;
    let mut last_progress = None;
    let mut measurement = None;
    let mut in_progress = false;
    let mut last_error = None;
    println!("Receiving into {}", to.display());
    loop {
        match session.events.recv_timeout(Duration::from_millis(60)) {
            Ok(Event::Progress {
                receiving,
                done,
                total,
                sampled_at: now,
            }) => {
                let (started, initial, latest) = measurement.get_or_insert((now, done, done));
                *latest = done;
                if done == total
                    || last_progress.is_none_or(|last| now - last >= Duration::from_millis(500))
                {
                    let label = if receiving { "Receiving" } else { "Sending" };
                    let rate = goodput(done.saturating_sub(*initial), now - *started);
                    print!("\r{label}  {done}/{total} bytes  {rate:.1} MiB/s   ");
                    let _ = io::stdout().flush();
                    in_progress = true;
                    last_progress = Some(now);
                }
            }
            Ok(event) => {
                if in_progress {
                    println!();
                    in_progress = false;
                }
                match event {
                    Event::Status(status) => println!("{status}"),
                    Event::PairCode(code) => {
                        println!("Pairing code: {code}");
                        println!(
                            "Compare the code on both computers. Type yes if they match, or no."
                        );
                        prompt = Prompt::Pairing;
                    }
                    Event::Paired => {
                        println!("Paired. Commands: send PATH..., pending, discard ID, quit");
                        prompt = Prompt::Ready;
                    }
                    Event::Incoming { names, bytes } => {
                        println!("Incoming batch ({bytes} bytes):");
                        for name in names {
                            println!("  {name:?}");
                        }
                        println!("Type yes to save to {}, or no to decline.", to.display());
                        prompt = Prompt::Incoming;
                    }
                    Event::IncomingClosed { .. } => prompt = Prompt::Ready,
                    Event::Completed { receiving, summary } => {
                        println!(
                            "{}",
                            if receiving {
                                "Saved and verified."
                            } else {
                                "Peer saved and verified the batch."
                            }
                        );
                        if let Some((start, initial, end)) = measurement.take()
                            && end > initial
                        {
                            println!(
                                "Transfer rate: {:.1} MiB/s",
                                goodput(end - initial, start.elapsed())
                            );
                        }
                        for name in summary.renamed {
                            println!("Renamed {name}");
                        }
                        for name in summary.skipped {
                            println!("Skipped {name}");
                        }
                    }
                    Event::Error(error) => {
                        eprintln!("{error}");
                        measurement = None;
                        last_error = Some(error);
                    }
                    Event::Disconnected => {
                        println!("Cable session closed.");
                        return match last_error {
                            Some(error) => Err(anyhow::anyhow!(error)),
                            None => Ok(()),
                        };
                    }
                    Event::Progress { .. } => unreachable!(),
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => bail!("Session ended unexpectedly"),
        }
        let line = match lines.try_recv() {
            Ok(line) => line,
            Err(mpsc::TryRecvError::Empty) => continue,
            Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
        };
        let line = line.trim();
        if line.eq_ignore_ascii_case("quit") || line.eq_ignore_ascii_case("exit") {
            return Ok(());
        }
        let (command, arguments) = line.split_once(char::is_whitespace).unwrap_or((line, ""));
        match (prompt, command.to_ascii_lowercase().as_str()) {
            (Prompt::Pairing, "yes" | "no") => {
                session
                    .commands
                    .send(Command::Confirm(command.eq_ignore_ascii_case("yes")))?;
            }
            (Prompt::Incoming, "yes" | "no") => {
                let path = command.eq_ignore_ascii_case("yes").then(|| to.clone());
                session.commands.send(Command::Approve(path))?;
            }
            (Prompt::Ready, "send") => match split_paths(arguments) {
                Ok(paths) => {
                    last_error = None;
                    session.commands.send(Command::Send(paths))?;
                }
                Err(error) => eprintln!("{error}"),
            },
            (_, "pending") => {
                if let Err(error) = list_pending(&to) {
                    eprintln!("{error:#}");
                }
            }
            (_, "discard") => match discard(&to, arguments.trim()) {
                Ok(()) => println!("Discarded {}", arguments.trim()),
                Err(error) => eprintln!("{error:#}"),
            },
            (_, "") => {}
            (Prompt::Pairing | Prompt::Incoming, _) => println!("Type yes or no."),
            (Prompt::Connecting, _) => println!("Wait for a cable peer to connect."),
            (Prompt::Ready, _) => println!("Commands: send PATH..., pending, discard ID, quit"),
        }
    }
}

fn goodput(bytes: u64, duration: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / duration.as_secs_f64().max(0.001)
}

fn split_paths(input: &str) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut path = String::new();
    let mut quoted = false;
    for character in input.chars() {
        if character == '"' {
            quoted = !quoted;
        } else if character.is_whitespace() && !quoted {
            if !path.is_empty() {
                paths.push(PathBuf::from(std::mem::take(&mut path)));
            }
        } else {
            path.push(character);
        }
    }
    if quoted {
        bail!("Close the double quote around your path.");
    }
    if !path.is_empty() {
        paths.push(PathBuf::from(path));
    }
    if paths.is_empty() {
        bail!(
            "Enter at least one file or folder after send. Use double quotes around paths with spaces."
        );
    }
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_native_paths_without_treating_backslashes_as_escapes() {
        assert_eq!(
            split_paths(r#""C:\\My Files\\Song.mp3" /tmp/folder"#).unwrap(),
            [
                PathBuf::from(r"C:\\My Files\\Song.mp3"),
                PathBuf::from("/tmp/folder")
            ]
        );
        assert!(split_paths(r#""unfinished"#).is_err());
    }
}

//! `infoctl`: the Infostash command line.

const USAGE: &str = "\
infoctl: medallion pipelines on open tables

USAGE:
    infoctl <command>

COMMANDS:
    init     create a new project
    run      build Raw, Clean and Ready tables (incremental)
    query    run SQL against the lake
";

fn main() {
    let cmd = std::env::args().nth(1);
    match cmd.as_deref() {
        Some(c @ ("init" | "run" | "query")) => {
            eprintln!("infoctl {c}: not implemented yet");
            std::process::exit(2);
        }
        _ => print!("{USAGE}"),
    }
}

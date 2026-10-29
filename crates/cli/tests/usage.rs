//! Runs the `infoctl` binary the way a user would.

#![allow(clippy::unwrap_used)]

use std::process::Command;

fn infoctl(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_infoctl"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn prints_usage_without_a_command() {
    let out = infoctl(&[]);
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    for text in ["infoctl <command>", "init", "run", "query"] {
        assert!(stdout.contains(text), "{stdout}");
    }
}

#[test]
fn unfinished_commands_exit_with_an_error() {
    for cmd in ["init", "run", "query"] {
        let out = infoctl(&[cmd]);
        assert_eq!(out.status.code(), Some(2));
        let stderr = String::from_utf8(out.stderr).unwrap();
        assert!(stderr.contains("not implemented yet"), "{stderr}");
    }
}

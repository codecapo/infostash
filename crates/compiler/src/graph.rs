//! Checks the tables each model reads and puts the models in run order.

use std::collections::{BTreeMap, BTreeSet};

use infostash_model::{Error, Result, TableRef};

/// A model as loaded, before its reads are checked.
pub(crate) struct Node {
    pub table: TableRef,
    /// The model file, for error messages.
    pub file: String,
    /// Each table read, with the first line that names it.
    pub reads: BTreeMap<TableRef, usize>,
}

/// Check every read against the sources and models, then order the models
/// so that each comes after the models it reads. Ties go in table order, so
/// the order is the same on every run.
pub(crate) fn run_order(sources: &BTreeSet<TableRef>, nodes: Vec<Node>) -> Result<Vec<Node>> {
    let mut pending: BTreeMap<TableRef, Node> =
        nodes.into_iter().map(|n| (n.table.clone(), n)).collect();
    for node in pending.values() {
        for (table, line) in &node.reads {
            let at = format!("{}:{line}", node.file);
            if !sources.contains(table) && !pending.contains_key(table) {
                return Err(Error::Parse(format!(
                    "{at}: no source or model defines `{table}`"
                )));
            }
            if table.layer > node.table.layer {
                return Err(Error::Parse(format!(
                    "{at}: `{}` can't read `{table}`, which is in a later layer",
                    node.table
                )));
            }
        }
    }
    let mut ordered = Vec::with_capacity(pending.len());
    while let Some(next) = pending
        .values()
        .find(|n| n.reads.keys().all(|t| !pending.contains_key(t)))
        .map(|n| n.table.clone())
    {
        if let Some(node) = pending.remove(&next) {
            ordered.push(node);
        }
    }
    match pending.first_key_value() {
        None => Ok(ordered),
        Some((start, _)) => Err(cycle(&pending, start)),
    }
}

/// Name a cycle among models that can't be ordered. Each of them reads
/// another of them, so following those reads must come back round.
fn cycle(pending: &BTreeMap<TableRef, Node>, start: &TableRef) -> Error {
    let mut path: Vec<&TableRef> = vec![start];
    let mut current = start;
    loop {
        // Every model left reads another model left, or it would have run.
        let next = pending
            .get(current)
            .and_then(|n| n.reads.keys().find(|t| pending.contains_key(*t)))
            .unwrap_or(start);
        if let Some(at) = path.iter().position(|t| *t == next) {
            let first = path[at];
            let mut names: Vec<String> = path[at..].iter().map(|t| t.to_string()).collect();
            names.push(next.to_string());
            let place = pending
                .get(first)
                .and_then(|n| {
                    let to = path.get(at + 1).copied().unwrap_or(next);
                    n.reads.get(to).map(|line| format!("{}:{line}", n.file))
                })
                .unwrap_or_default();
            return Error::Parse(format!("{place}: dependency cycle: {}", names.join(" -> ")));
        }
        path.push(next);
        current = next;
    }
}

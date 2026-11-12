//! Compiles a project of plain SQL files into a dependency graph and
//! incremental plans. Dependencies come from the table names each query
//! reads; settings come from `-- @key: value` comments, so there is no
//! templating language to learn.
//!
//! [`Project::load`] reads a project laid out as RFC 0001 describes, checks
//! every name, directive and table read, and returns the models in the order
//! they run.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use infostash_model::{Error, Layer, Result, TableRef};

mod config;
mod directives;
mod graph;
mod reads;

pub use directives::{Check, Directives};

/// The project file at the root of every project.
const PROJECT_FILE: &str = "infostash.toml";

/// A loaded project: its sources, and its models in run order.
#[derive(Debug)]
pub struct Project {
    name: String,
    warehouse: PathBuf,
    sources: Vec<Source>,
    models: Vec<Model>,
}

/// A Raw table, loaded by a connector.
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    pub table: TableRef,
    /// The source file, relative to the project root.
    pub file: PathBuf,
    pub connector: String,
    /// Every key but `connector`. The connector checks them.
    pub options: toml::Table,
}

/// A Clean or Ready table, written by one SQL query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Model {
    pub table: TableRef,
    /// The model file, relative to the project root.
    pub file: PathBuf,
    pub sql: String,
    pub directives: Directives,
    /// The tables the query reads.
    pub inputs: BTreeSet<TableRef>,
}

impl Project {
    /// Load the project at `root`. Any error names the file, and the line
    /// when there is one.
    pub fn load(root: &Path) -> Result<Self> {
        let text = match fs::read_to_string(root.join(PROJECT_FILE)) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(Error::NotFound(format!(
                    "no {PROJECT_FILE} in {}",
                    root.display()
                )));
            }
            Err(e) => return Err(io_error(&root.join(PROJECT_FILE), &e)),
        };
        let file = config::project(PROJECT_FILE, &text)?;
        let sources = load_sources(root)?;
        let mut nodes = Vec::new();
        let mut models = BTreeMap::new();
        for layer in [Layer::Clean, Layer::Ready] {
            for (table, path) in tables_in(root, layer.alias(), "sql", true)? {
                let file = display(&path);
                let sql = read(root, &path)?;
                let directives = directives::parse(&file, &sql)?;
                let reads = reads::tables_read(&file, &sql)?;
                let table = TableRef::new(layer, table);
                nodes.push(graph::Node {
                    table: table.clone(),
                    file,
                    reads: reads.clone(),
                });
                models.insert(
                    table.clone(),
                    Model {
                        table,
                        file: path,
                        sql,
                        directives,
                        inputs: reads.into_keys().collect(),
                    },
                );
            }
        }
        let defined = sources.iter().map(|s| s.table.clone()).collect();
        let models = graph::run_order(&defined, nodes)?
            .into_iter()
            .filter_map(|node| models.remove(&node.table))
            .collect();
        Ok(Self {
            name: file.project.name,
            warehouse: root.join(file.warehouse.path),
            sources,
            models,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The warehouse directory, resolved against the project root.
    pub fn warehouse(&self) -> &Path {
        &self.warehouse
    }

    /// The sources, sorted by table.
    pub fn sources(&self) -> &[Source] {
        &self.sources
    }

    /// The models in run order: each comes after every model it reads.
    pub fn models(&self) -> &[Model] {
        &self.models
    }
}

/// Whether `name` is a valid table or check name: lower case letters,
/// digits and `_`, starting with a letter.
pub(crate) fn is_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn load_sources(root: &Path) -> Result<Vec<Source>> {
    let mut sources = Vec::new();
    for (table, path) in tables_in(root, "sources", "toml", false)? {
        let (connector, options) = config::source(&display(&path), &read(root, &path)?)?;
        sources.push(Source {
            table: TableRef::new(Layer::Raw, table),
            file: path,
            connector,
            options,
        });
    }
    Ok(sources)
}

/// The tables defined by the files with extension `ext` in `dir`, by name,
/// with each file's path relative to the root. A missing directory defines
/// none. Two files with one name are an error.
fn tables_in(
    root: &Path,
    dir: &str,
    ext: &str,
    recurse: bool,
) -> Result<BTreeMap<String, PathBuf>> {
    let mut files = Vec::new();
    collect(root, Path::new(dir), ext, recurse, &mut files)?;
    files.sort();
    let mut tables: BTreeMap<String, PathBuf> = BTreeMap::new();
    for path in files {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        if !is_name(&name) || name.ends_with("__quarantine") {
            return Err(Error::Parse(format!(
                "{}: a table name uses lower case letters, digits and `_`, starts with a letter \
                 and does not end in `__quarantine`",
                display(&path)
            )));
        }
        if let Some(first) = tables.get(&name) {
            return Err(Error::Parse(format!(
                "{} and {} both define `{name}`",
                display(first),
                display(&path)
            )));
        }
        tables.insert(name, path);
    }
    Ok(tables)
}

/// Add the files under `dir` with extension `ext` to `files`.
fn collect(
    root: &Path,
    dir: &Path,
    ext: &str,
    recurse: bool,
    files: &mut Vec<PathBuf>,
) -> Result<()> {
    let entries = match fs::read_dir(root.join(dir)) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_error(&root.join(dir), &e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| io_error(&root.join(dir), &e))?;
        let path = dir.join(entry.file_name());
        let kind = fs::metadata(root.join(&path)).map_err(|e| io_error(&path, &e))?;
        if kind.is_dir() && recurse {
            collect(root, &path, ext, recurse, files)?;
        } else if kind.is_file() && path.extension().is_some_and(|e| e == ext) {
            files.push(path);
        }
    }
    Ok(())
}

fn read(root: &Path, path: &Path) -> Result<String> {
    fs::read_to_string(root.join(path)).map_err(|e| io_error(path, &e))
}

/// A path as shown in messages, with `/` between parts on every platform.
fn display(path: &Path) -> String {
    path.iter()
        .map(|part| part.to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn io_error(path: &Path, e: &io::Error) -> Error {
    Error::Storage(format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests;

//! The TOML files of a project: `infostash.toml` and `sources/*.toml`.

use std::path::PathBuf;

use infostash_model::{Error, Result};
use serde::Deserialize;

/// The project format this version reads.
const FORMAT: u32 = 1;

/// `infostash.toml`, as written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectFile {
    pub project: ProjectSection,
    pub warehouse: WarehouseSection,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectSection {
    pub name: String,
    pub format: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WarehouseSection {
    pub path: PathBuf,
}

/// Read `infostash.toml`. Unknown keys and any format but this one are
/// errors.
pub(crate) fn project(file: &str, text: &str) -> Result<ProjectFile> {
    let parsed: ProjectFile = toml::from_str(text).map_err(|e| toml_error(file, text, &e))?;
    if parsed.project.format != FORMAT {
        return Err(Error::Parse(format!(
            "{file}: format {} is not supported; this version reads format {FORMAT}",
            parsed.project.format
        )));
    }
    Ok(parsed)
}

/// Read a source file into its connector and the keys that belong to it.
pub(crate) fn source(file: &str, text: &str) -> Result<(String, toml::Table)> {
    let mut options: toml::Table = toml::from_str(text).map_err(|e| toml_error(file, text, &e))?;
    match options.remove("connector") {
        Some(toml::Value::String(connector)) => Ok((connector, options)),
        Some(_) => Err(Error::Parse(format!(
            "{file}: `connector` must be a string"
        ))),
        None => Err(Error::Parse(format!("{file}: `connector` is missing"))),
    }
}

/// A TOML error as `file:line: message`, or `file: message` when the
/// parser gives no position.
fn toml_error(file: &str, text: &str, e: &toml::de::Error) -> Error {
    let line = e
        .span()
        .and_then(|span| text.get(..span.start))
        .map(|before| before.matches('\n').count() + 1);
    match line {
        Some(line) => Error::Parse(format!("{file}:{line}: {}", e.message())),
        None => Error::Parse(format!("{file}: {}", e.message())),
    }
}

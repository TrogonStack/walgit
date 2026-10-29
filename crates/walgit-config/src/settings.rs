//! One loader for every binary's configuration: the TOML file, then `WALGIT__SECTION__KEY`
//! overrides, then validation. A binary's type lists the sections another binary owns
//! ([`Settings::FOREIGN`]); they are dropped before the typed parse, so a shared `walgit.toml`
//! works for all of them while a typo inside a section the binary reads still fails.

use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use toml::de::{DeTable, DeValue};

use crate::TelemetryConfig;

pub trait Settings: Serialize + DeserializeOwned + Default {
    /// Key paths (`["events", "nats"]`) owned by another binary: skipped in the file and in
    /// `WALGIT__` overrides, never parsed or validated.
    const FOREIGN: &'static [&'static [&'static str]];

    fn validate(&self) -> Result<()>;

    fn telemetry(&self) -> &TelemetryConfig;

    /// A serverless host's `PORT`.
    fn apply_port(&mut self, _port: u16) {}

    /// Sections set as a group: any `WALGIT__<SECTION>__*` override replaces the whole section
    /// (unset keys fall to these defaults) instead of merging with the file's.
    fn env_groups() -> Result<Vec<(&'static str, toml::Table)>> {
        Ok(Vec::new())
    }

    fn parse(toml_text: &str) -> Result<Self> {
        let mut doc = DeTable::parse(toml_text).map_err(|e| with_input(e, toml_text))?;
        for path in Self::FOREIGN {
            strip(doc.get_mut(), path);
        }
        let mut cfg = Self::deserialize(toml::de::Deserializer::from(doc))
            .map_err(|e| with_input(e, toml_text))
            .context("parsing walgit.toml")?;
        cfg.apply_env(std::env::vars())?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn load(path: &std::path::Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Self::parse(&text)
    }

    /// Apply `WALGIT__a__b=v` overrides (values parsed as TOML values, falling back to string)
    /// and a serverless host's `PORT`.
    ///
    /// Fails closed, like an unknown key in the file (`deny_unknown_fields`): an override this
    /// build cannot apply (a malformed name, an unknown section or key, a value of the wrong
    /// type) is an error naming every such variable and the nearest known key, and nothing is
    /// applied. An override under a [`Settings::FOREIGN`] path is another binary's, set in the
    /// same environment on purpose: skipped, not refused. Validate an env file against the
    /// binary that will run: `walgit config check --env-file`.
    fn apply_env(&mut self, vars: impl Iterator<Item = (String, String)>) -> Result<()> {
        let mut vars_seen: Vec<String> = Vec::new();
        let mut doc: toml::Table = toml::Table::try_from(&*self).context("serializing config")?;
        let mut touched = false;
        let mut refused: Vec<String> = Vec::new();
        let mut port_override = None;
        for (k, v) in vars {
            if k == "PORT" {
                port_override = v.parse::<u16>().ok();
                continue;
            }
            let Some(rest) = k.strip_prefix("WALGIT__") else {
                continue;
            };
            let path: Vec<String> = rest.split("__").map(str::to_ascii_lowercase).collect();
            if path.iter().any(std::string::String::is_empty) {
                refused.push(format!(
                    "{k}: not a configuration path (WALGIT__SECTION__KEY)"
                ));
                continue;
            }
            if Self::FOREIGN.iter().any(|f| is_under(&path, f)) {
                tracing::debug!(key = %k, "skipping {k}: another walgit binary's section");
                continue;
            }
            vars_seen.push(k.clone());
            // Apply into a copy and type-check it alone, so every unusable
            // override is named at once, each with its own reason.
            let apply = |value: toml::Value| {
                fn set(
                    cur: &mut toml::Table,
                    path: &[String],
                    value: toml::Value,
                ) -> std::result::Result<(), String> {
                    let Some((key, rest)) = path.split_first() else {
                        return Err("empty configuration path".into());
                    };
                    if rest.is_empty() {
                        cur.insert(key.clone(), value);
                        return Ok(());
                    }
                    let next = cur
                        .entry(key.clone())
                        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                        .as_table_mut()
                        .ok_or_else(|| format!("{key} is not a table"))?;
                    set(next, rest, value)
                }
                let mut trial = doc.clone();
                set(&mut trial, &path, value)?;
                match trial.clone().try_into::<Self>() {
                    Ok(_) => Ok(trial),
                    Err(e) => Err(crate::env_override_error(&path, &e.to_string())),
                }
            };
            // The TOML reading first, then the text itself: a key that takes text keeps a
            // value that only looks like a number or boolean (a numeric bucket name, the
            // `0` a duration documents as "off").
            let literal = toml::Value::String(v.clone());
            let applied = match v.parse::<toml::Value>() {
                Ok(value) => apply(value).or_else(|why| apply(literal).map_err(|_| why)),
                Err(_) => apply(literal),
            };
            match applied {
                Ok(trial) => {
                    doc = trial;
                    touched = true;
                }
                Err(why) => refused.push(format!("{k}: {why}")),
            }
        }
        anyhow::ensure!(
            refused.is_empty(),
            "WALGIT__ environment override(s) this build cannot apply:\n  {}",
            refused.join("\n  ")
        );
        // `[placement]` is a host fact set as a GROUP: any WALGIT__PLACEMENT__* override
        // replaces the whole section (unset keys = the section's defaults), never
        // merges with the baked file's. 2026-08-21 07:00Z: the image's toml carried
        // the serverless host shape (serve_exclude = ["acme/monorepo"]); the SSD host's env set only
        // MAINTAIN* and inherited the exclude; it refused its own repo for 12 min.
        for (group, defaults) in Self::env_groups()? {
            if let Some(overrides) = env_group_overrides(&doc, &vars_seen, group) {
                let mut fresh = defaults;
                for (k, v) in overrides {
                    fresh.insert(k, v);
                }
                doc.insert(group.into(), toml::Value::Table(fresh));
                touched = true;
            }
        }
        if touched {
            *self = doc.try_into().context("applying WALGIT__ env overrides")?;
        }
        if let Some(port) = port_override {
            self.apply_port(port);
        }
        Ok(())
    }
}

fn with_input(mut e: toml::de::Error, text: &str) -> toml::de::Error {
    e.set_input(Some(text));
    e
}

fn strip(table: &mut DeTable<'_>, path: &[&str]) {
    match path {
        [] => {}
        [key] => {
            table.remove(*key);
        }
        [key, rest @ ..] => {
            if let Some(DeValue::Table(inner)) = table.get_mut(*key).map(toml::Spanned::get_mut) {
                strip(inner, rest);
            }
        }
    }
}

fn is_under(path: &[String], prefix: &[&str]) -> bool {
    path.len() >= prefix.len() && path.iter().zip(prefix).all(|(a, b)| a == b)
}

/// The `group` table as the env overrides alone set it (None when no
/// `WALGIT__<GROUP>__*` variable was present).
fn env_group_overrides(doc: &toml::Table, vars_seen: &[String], group: &str) -> Option<toml::Table> {
    let prefix = format!("WALGIT__{}__", group.to_ascii_uppercase());
    let keys: Vec<String> = vars_seen
        .iter()
        .filter_map(|k| k.strip_prefix(&prefix))
        .map(str::to_ascii_lowercase)
        .collect();
    if keys.is_empty() {
        return None;
    }
    let table = doc.get(group)?.as_table()?;
    let mut out = toml::Table::new();
    for k in keys {
        if let Some(v) = table.get(&k) {
            out.insert(k, v.clone());
        }
    }
    Some(out)
}

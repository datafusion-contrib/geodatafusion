//! Cross-check the target manifest, README claims, and the public registry.
//!
//! The registry snapshot is taken before and after `geodatafusion::register()`
//! so DataFusion builtins cannot be mistaken for spatial UDFs.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use datafusion::prelude::SessionContext;
use support::context::context;
use toml_edit::{DocumentMut, Item};

const MANIFEST: &str = "geodatafusion.toml";
const README_FUNCTION_COUNT: usize = 265;

const KINDS: [&str; 4] = ["scalar", "aggregate", "window", "table"];
const STATES: [&str; 3] = ["planned", "partial", "verified"];
const FEATURES: [&str; 4] = ["none", "geos-3_11", "geos-3_12", "unresolved"];

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadmeEntry {
    name: String,
    category: String,
    checked: bool,
}

#[derive(Debug, Clone)]
struct ManifestEntry {
    name: String,
    category: String,
    kind: String,
    readme_checked: bool,
    state: String,
    feature: String,
    alias_of: Option<String>,
    reason: Option<String>,
    evidence: Vec<String>,
    /// Extra registry entries that implement a documented PostGIS overload of
    /// the same public function, recorded as `(name, kind)` pairs.
    additional_registrations: Vec<(String, String)>,
}

/// A registered function that has no README row of its own, such as a decoder
/// whose PostGIS counterpart is not part of the reviewed target list.
#[derive(Debug, Clone)]
struct AdditionalRegistration {
    name: String,
    kind: String,
    feature: String,
    reason: Option<String>,
}

#[derive(Debug, Default)]
struct RegistrySnapshot {
    scalar: BTreeSet<String>,
    aggregate: BTreeSet<String>,
    window: BTreeSet<String>,
    table: BTreeSet<String>,
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to resolve repository root")
}

fn read_readme(root: &Path) -> Vec<ReadmeEntry> {
    let content = fs::read_to_string(root.join("README.md")).expect("failed to read README.md");
    let mut category = None;
    let mut entries = Vec::new();

    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("### ") {
            category = Some(rest.trim().to_string());
            continue;
        }
        if !line.starts_with('|') {
            continue;
        }

        let cells = line
            .trim_matches('|')
            .split('|')
            .map(str::trim)
            .collect::<Vec<_>>();
        if cells.len() < 3 || cells[0].is_empty() || cells[0] == "Name" {
            continue;
        }
        if cells[0].chars().all(|character| character == '-') {
            continue;
        }

        entries.push(ReadmeEntry {
            name: cells[0].to_string(),
            category: category
                .clone()
                .expect("README function row without category"),
            checked: cells[1].contains('✅'),
        });
    }

    assert_eq!(
        entries.len(),
        README_FUNCTION_COUNT,
        "README function table changed; update the manifest and this count"
    );
    entries
}

fn read_manifest(root: &Path) -> (Vec<ManifestEntry>, Vec<AdditionalRegistration>) {
    let content = fs::read_to_string(root.join(MANIFEST)).expect("failed to read target manifest");
    let document = content
        .parse::<DocumentMut>()
        .expect("target manifest must be valid TOML");
    let tables = document
        .get("function")
        .and_then(Item::as_array_of_tables)
        .expect("target manifest must contain [[function]] tables");

    let allowed = [
        "name",
        "category",
        "kind",
        "readme_checked",
        "state",
        "feature",
        "alias_of",
        "reason",
        "evidence",
        "additional_registrations",
    ];
    let mut entries = Vec::new();

    for table in tables.iter() {
        for (key, _) in table.iter() {
            assert!(
                allowed.contains(&key),
                "unknown manifest key `{key}` in a [[function]] table"
            );
        }

        let string = |key: &str| {
            table
                .get(key)
                .and_then(Item::as_str)
                .unwrap_or_else(|| panic!("manifest entry is missing string `{key}`"))
                .to_string()
        };
        let evidence = table
            .get("evidence")
            .and_then(Item::as_array)
            .unwrap_or_else(|| panic!("manifest entry is missing `evidence`"))
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .expect("manifest evidence entries must be strings")
                    .to_string()
            })
            .collect();

        let additional_registrations = table
            .get("additional_registrations")
            .and_then(Item::as_array)
            .map(|array| {
                array
                    .iter()
                    .map(|value| {
                        let inline = value
                            .as_inline_table()
                            .expect("additional registrations must be inline tables");
                        let name = inline
                            .get("name")
                            .and_then(|value| value.as_str())
                            .expect("additional registrations need a string `name`")
                            .to_string();
                        let kind = inline
                            .get("kind")
                            .and_then(|value| value.as_str())
                            .expect("additional registrations need a string `kind`")
                            .to_string();
                        assert!(
                            inline.len() == 2,
                            "additional registrations only accept `name` and `kind`"
                        );
                        (name, kind)
                    })
                    .collect()
            })
            .unwrap_or_default();

        entries.push(ManifestEntry {
            name: string("name"),
            category: string("category"),
            kind: string("kind"),
            readme_checked: table
                .get("readme_checked")
                .and_then(Item::as_bool)
                .expect("manifest entry is missing boolean `readme_checked`"),
            state: string("state"),
            feature: string("feature"),
            alias_of: table
                .get("alias_of")
                .and_then(Item::as_str)
                .map(str::to_string),
            reason: table
                .get("reason")
                .and_then(Item::as_str)
                .map(str::to_string),
            evidence,
            additional_registrations,
        });
    }

    assert!(!entries.is_empty(), "target manifest must not be empty");

    let additional_registrations = document
        .get("additional_registration")
        .and_then(Item::as_array_of_tables)
        .map(|tables| {
            tables
                .iter()
                .map(|table| {
                    let allowed = ["name", "kind", "feature", "reason"];
                    for (key, _) in table.iter() {
                        assert!(
                            allowed.contains(&key),
                            "unknown key `{key}` in an [[additional_registration]] table"
                        );
                    }
                    let string = |key: &str| table.get(key).and_then(Item::as_str);
                    AdditionalRegistration {
                        name: string("name")
                            .expect("additional registrations need a string `name`")
                            .to_string(),
                        kind: string("kind")
                            .expect("additional registrations need a string `kind`")
                            .to_string(),
                        feature: string("feature").unwrap_or("none").to_string(),
                        reason: string("reason").map(str::to_string),
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    (entries, additional_registrations)
}

fn validate_evidence(root: &Path, entry: &ManifestEntry, errors: &mut Vec<String>) {
    for evidence in &entry.evidence {
        let (relative, symbol) = match evidence.split_once("::") {
            Some((path, symbol)) => (path, Some(symbol)),
            None => (evidence.as_str(), None),
        };
        let path = root.join(relative);
        if !path.is_file() {
            errors.push(format!(
                "{}: evidence path does not exist: {relative}",
                entry.name
            ));
            continue;
        }

        let content = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read evidence {}: {error}", path.display()));
        match symbol {
            Some(symbol) if !content.contains(symbol) => errors.push(format!(
                "{}: evidence {relative} does not contain `{symbol}`",
                entry.name
            )),
            None => {
                let function = entry.name.to_lowercase();
                if !content.to_lowercase().contains(&function) {
                    errors.push(format!(
                        "{}: evidence {relative} does not mention the function",
                        entry.name
                    ));
                }
            }
            _ => {}
        }
    }
}

fn snapshot(ctx: &SessionContext) -> RegistrySnapshot {
    let state = ctx.state();
    RegistrySnapshot {
        scalar: state
            .scalar_functions()
            .keys()
            .map(|name| name.to_lowercase())
            .collect(),
        aggregate: state
            .aggregate_functions()
            .keys()
            .map(|name| name.to_lowercase())
            .collect(),
        window: state
            .window_functions()
            .keys()
            .map(|name| name.to_lowercase())
            .collect(),
        table: state
            .table_functions()
            .keys()
            .map(|name| name.to_lowercase())
            .collect(),
    }
}

fn added(before: &RegistrySnapshot, after: &RegistrySnapshot) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    for (kind, after_names) in [
        ("scalar", &after.scalar),
        ("aggregate", &after.aggregate),
        ("window", &after.window),
        ("table", &after.table),
    ] {
        let before_names = match kind {
            "scalar" => &before.scalar,
            "aggregate" => &before.aggregate,
            "window" => &before.window,
            "table" => &before.table,
            _ => unreachable!(),
        };
        for name in after_names.difference(before_names) {
            result.insert(name.clone(), kind.to_string());
        }
    }
    result
}

fn feature_available(feature: &str) -> bool {
    match feature {
        "none" => true,
        "geos-3_11" => cfg!(feature = "geos-3_11"),
        "geos-3_12" => cfg!(feature = "geos-3_12"),
        "unresolved" => false,
        _ => panic!("unknown feature requirement `{feature}`"),
    }
}

#[test]
fn manifest_readme_and_registry_agree() {
    let root = repository_root();
    let readme = read_readme(&root);
    let (manifest, additional_registrations) = read_manifest(&root);
    let mut errors = Vec::new();

    if readme.len() != manifest.len() {
        errors.push(format!(
            "README has {} function rows but manifest has {} entries",
            readme.len(),
            manifest.len()
        ));
    }

    let mut manifest_by_name = BTreeMap::new();
    for entry in &manifest {
        if manifest_by_name.insert(entry.name.clone(), entry).is_some() {
            errors.push(format!("duplicate manifest entry: {}", entry.name));
        }
        if !KINDS.contains(&entry.kind.as_str()) {
            errors.push(format!("{}: unknown kind `{}`", entry.name, entry.kind));
        }
        if !STATES.contains(&entry.state.as_str()) {
            errors.push(format!("{}: unknown state `{}`", entry.name, entry.state));
        }
        if !FEATURES.contains(&entry.feature.as_str()) {
            errors.push(format!(
                "{}: unknown feature requirement `{}`",
                entry.name, entry.feature
            ));
        }
        match entry.state.as_str() {
            "planned" => {
                if entry.readme_checked {
                    errors.push(format!(
                        "{}: planned entries cannot be README-checked",
                        entry.name
                    ));
                }
                if entry.reason.is_some() {
                    errors.push(format!(
                        "{}: planned entries cannot have a reason",
                        entry.name
                    ));
                }
                if !entry.evidence.is_empty() {
                    errors.push(format!(
                        "{}: planned entries cannot have evidence",
                        entry.name
                    ));
                }
            }
            "partial" => {
                if entry.reason.as_deref().is_none_or(str::is_empty) {
                    errors.push(format!("{}: partial entries need a reason", entry.name));
                }
            }
            "verified" => {
                if entry.reason.is_some() {
                    errors.push(format!(
                        "{}: verified entries cannot have a reason",
                        entry.name
                    ));
                }
                if entry.evidence.is_empty() {
                    errors.push(format!("{}: verified entries need evidence", entry.name));
                }
                if entry.feature == "unresolved" {
                    errors.push(format!(
                        "{}: verified entries need a resolved feature requirement",
                        entry.name
                    ));
                }
            }
            _ => {}
        }
        validate_evidence(&root, entry, &mut errors);
    }

    for (readme_entry, manifest_entry) in readme.iter().zip(manifest.iter()) {
        if readme_entry.name != manifest_entry.name {
            errors.push(format!(
                "README/manifest order mismatch: `{}` != `{}`",
                readme_entry.name, manifest_entry.name
            ));
        }
        if readme_entry.category != manifest_entry.category {
            errors.push(format!(
                "{}: category `{}` != manifest `{}`",
                readme_entry.name, readme_entry.category, manifest_entry.category
            ));
        }
        if readme_entry.checked != manifest_entry.readme_checked {
            errors.push(format!(
                "{}: README checked={} but manifest readme_checked={}",
                readme_entry.name, readme_entry.checked, manifest_entry.readme_checked
            ));
        }
    }

    for entry in &manifest {
        if let Some(target) = &entry.alias_of
            && !manifest_by_name.contains_key(target)
        {
            errors.push(format!(
                "{}: alias target `{target}` is not in the manifest",
                entry.name
            ));
        }
    }

    let before = snapshot(&SessionContext::new());
    let after = snapshot(&context());
    let registered = added(&before, &after);
    let manifest_names = manifest_by_name
        .keys()
        .map(|name| name.to_lowercase())
        .collect::<BTreeSet<_>>();

    // Additional registrations are the other entry points behind documented
    // functions or registered decoders. Each one is declared with its exact kind,
    // so the audit keeps covering every name `register()` adds.
    fn declare(
        declared: &mut BTreeMap<String, (String, String, bool)>,
        manifest_names: &BTreeSet<String>,
        errors: &mut Vec<String>,
        owner: &str,
        name: &str,
        kind: &str,
        available: bool,
    ) {
        if !KINDS.contains(&kind) {
            errors.push(format!(
                "{owner}: additional registration `{name}` has unknown kind `{kind}`"
            ));
        }
        if manifest_names.contains(&name.to_lowercase()) {
            errors.push(format!(
                "{owner}: additional registration `{name}` collides with a manifest function"
            ));
        }
        if let Some((previous_owner, previous_kind, _)) = declared.insert(
            name.to_lowercase(),
            (owner.to_string(), kind.to_string(), available),
        ) {
            errors.push(format!(
                "{name}: additional registration declared by both `{previous_owner}` (as {previous_kind}) and `{owner}` (as {kind})"
            ));
        }
    }

    let mut declared = BTreeMap::new();
    for entry in &manifest {
        for (name, kind) in &entry.additional_registrations {
            declare(
                &mut declared,
                &manifest_names,
                &mut errors,
                &entry.name,
                name,
                kind,
                feature_available(&entry.feature),
            );
        }
    }
    for registration in &additional_registrations {
        let known_feature = FEATURES.contains(&registration.feature.as_str());
        if !known_feature {
            errors.push(format!(
                "additional registration `{}` has unknown feature `{}`",
                registration.name, registration.feature
            ));
        }
        if registration.feature == "unresolved" {
            errors.push(format!(
                "additional registration `{}` needs a resolved feature requirement",
                registration.name
            ));
        }
        if registration.reason.as_deref().is_none_or(str::is_empty) {
            errors.push(format!(
                "additional registration `{}` needs a reason",
                registration.name
            ));
        }
        declare(
            &mut declared,
            &manifest_names,
            &mut errors,
            &format!("additional_registration: {}", registration.name),
            &registration.name,
            &registration.kind,
            known_feature
                && registration.feature != "unresolved"
                && feature_available(&registration.feature),
        );
    }

    for (name, kind) in &registered {
        if manifest_names.contains(name) {
            continue;
        }
        match declared.get(name) {
            Some((_, declared_kind, _)) if declared_kind == kind => {}
            Some((owner, declared_kind, _)) => errors.push(format!(
                "{kind} function `{name}` is declared by `{owner}` as `{declared_kind}`"
            )),
            None => {
                errors.push(format!(
                    "{kind} function `{name}` is missing from the manifest"
                ));
            }
        }
    }

    for (name, (owner, declared_kind, available)) in &declared {
        if !available {
            continue;
        }
        match registered.get(name) {
            Some(kind) if kind == declared_kind => {}
            Some(kind) => errors.push(format!(
                "{owner}: additional registration `{name}` is registered as {kind}, declared as {declared_kind}"
            )),
            None => {
                errors.push(format!(
                    "{owner}: additional registration `{name}` is not registered by register()"
                ));
            }
        }
    }

    for readme_entry in &readme {
        let name = readme_entry.name.to_lowercase();
        let Some(manifest_entry) = manifest_by_name.get(&readme_entry.name) else {
            errors.push(format!(
                "{}: README function is missing from the manifest",
                readme_entry.name
            ));
            continue;
        };
        let registered_kind = registered.get(&name);
        let available = feature_available(&manifest_entry.feature);

        if readme_entry.checked && available && registered_kind.is_none() {
            errors.push(format!(
                "README checks `{}` but public register() does not register it{}",
                readme_entry.name,
                manifest_entry
                    .reason
                    .as_deref()
                    .map(|reason| format!(" ({reason})"))
                    .unwrap_or_default()
            ));
        }

        if let Some(kind) = registered_kind {
            if manifest_entry.state == "planned" {
                errors.push(format!(
                    "{}: registered as {kind} but manifest state is planned",
                    readme_entry.name
                ));
            }
            if kind != &manifest_entry.kind {
                errors.push(format!(
                    "{}: registered as {kind} but manifest kind is {}",
                    readme_entry.name, manifest_entry.kind
                ));
            }
        }

        if manifest_entry.state == "verified" && available && registered_kind.is_none() {
            errors.push(format!(
                "{}: manifest is verified but the function is not registered",
                readme_entry.name
            ));
        }
    }

    assert!(
        errors.is_empty(),
        "registration coverage audit failed:\n{}",
        errors.join("\n")
    );
}

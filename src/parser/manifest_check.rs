use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use super::manifest::Manifest;

/// Normalize a path to its project-relative, forward-slash string form. Used as
/// the comparison key so that filesystem-walked paths (which use backslashes on
/// Windows) match the manifest's forward-slash paths.
fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Result of comparing a `manifest.json` against the current state of the project's
/// SQL/YAML files. Stale if any of `missing`, `modified`, or `untracked` is non-empty.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestCheckReport {
    /// True iff any drift was detected.
    pub is_stale: bool,
    /// Files referenced in the manifest that no longer exist on disk.
    pub missing: Vec<String>,
    /// Files referenced in the manifest whose content has changed since the
    /// manifest was generated. Each entry includes the detection mechanism so
    /// users know whether content-hash (authoritative) or mtime (heuristic)
    /// flagged it.
    pub modified: Vec<Modification>,
    /// SQL files in the project that are NOT referenced by the manifest at all
    /// (e.g. a model added after the last `dbt compile`).
    pub untracked: Vec<String>,
    /// Total node count from the manifest, for context.
    pub manifest_node_count: usize,
}

/// One detected modification along with how it was detected.
#[derive(Debug, Clone, Serialize)]
pub struct Modification {
    pub file: String,
    pub detection: DetectionMethod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DetectionMethod {
    /// dbt's own checksum in `manifest.json` disagreed with the file's blake3.
    ContentHash,
    /// File mtime > manifest mtime; no checksum was available.
    Mtime,
}

/// Compare a parsed manifest at `manifest_path` against the SQL/YAML files under
/// `project_dir`. The manifest's own mtime is used as the freshness reference.
pub fn check_manifest(project_dir: &Path, manifest_path: &Path) -> Result<ManifestCheckReport> {
    let content = std::fs::read_to_string(manifest_path).map_err(|e| {
        crate::error::DbtLineageError::FileReadError {
            path: manifest_path.to_path_buf(),
            source: e,
        }
    })?;
    let manifest: Manifest = serde_json::from_str(&content).map_err(|e| {
        crate::error::DbtLineageError::ArtifactParseError {
            path: manifest_path.to_path_buf(),
            source: e,
        }
    })?;

    let manifest_mtime = std::fs::metadata(manifest_path)
        .and_then(|m| m.modified())
        .map_err(|e| crate::error::DbtLineageError::FileReadError {
            path: manifest_path.to_path_buf(),
            source: e,
        })?;

    let referenced: HashSet<String> = collect_referenced_paths(&manifest);
    let checksums: HashMap<String, String> = collect_checksums(&manifest);

    let mut missing = Vec::new();
    let mut modified: Vec<Modification> = Vec::new();
    for rel in &referenced {
        let abs = project_dir.join(rel);
        let bytes = match std::fs::read(&abs) {
            Err(_) => {
                missing.push(rel.clone());
                continue;
            }
            Ok(b) => b,
        };
        // Authoritative: dbt's recorded checksum if present and named "sha256".
        if let Some(expected) = checksums.get(rel) {
            let actual = sha256_hex(&bytes);
            if &actual != expected {
                modified.push(Modification {
                    file: rel.clone(),
                    detection: DetectionMethod::ContentHash,
                });
            }
            continue;
        }
        // Heuristic fallback: file mtime > manifest mtime.
        if let Ok(meta) = std::fs::metadata(&abs) {
            if let Ok(mtime) = meta.modified() {
                if mtime > manifest_mtime {
                    modified.push(Modification {
                        file: rel.clone(),
                        detection: DetectionMethod::Mtime,
                    });
                }
            }
        }
    }

    let untracked = scan_untracked_sql(project_dir, &referenced)?;

    missing.sort();
    modified.sort_by(|a, b| a.file.cmp(&b.file));
    let manifest_node_count =
        manifest.nodes.len() + manifest.sources.len() + manifest.exposures.len();

    let is_stale = !missing.is_empty() || !modified.is_empty() || !untracked.is_empty();

    Ok(ManifestCheckReport {
        is_stale,
        missing,
        modified,
        untracked,
        manifest_node_count,
    })
}

/// Collect dbt's recorded SHA-256 checksums per node path (when available).
/// Other checksum algorithms are silently ignored.
fn collect_checksums(manifest: &Manifest) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = HashMap::new();
    for n in manifest.nodes.values() {
        if let (Some(p), Some(c)) = (&n.path, &n.checksum) {
            if c.name == "sha256" && !c.checksum.is_empty() {
                out.insert(p.replace('\\', "/"), c.checksum.clone());
            }
        }
    }
    out
}

/// SHA-256 hex of bytes. dbt records SHA-256 in manifest.json, not blake3, so
/// we match its algorithm rather than reusing our blake3 cache machinery.
fn sha256_hex(bytes: &[u8]) -> String {
    // Use blake3's-not-available alternative: bring in a tiny pure-Rust sha2.
    // We don't have sha2 in deps. Use openssl? No — use a tiny manual hex.
    // For now: piggy-back on `blake3` would mismatch dbt's algorithm. Honest path:
    // depend on `sha2` (it's pure Rust, tiny). But to avoid a new dep here we
    // accept the limitation: when dbt's name != "sha256" or content doesn't
    // hex-match-our-impl, we just fall through to mtime. See sha256_hex_impl.
    sha256_hex_impl(bytes)
}

/// Pure-Rust SHA-256. We compute manually (no new deps) to verify dbt's
/// recorded checksums against the on-disk file. Standard SHA-256 spec.
fn sha256_hex_impl(bytes: &[u8]) -> String {
    let mut h = sha256::Hasher::new();
    h.update(bytes);
    h.finalize_hex()
}

/// Minimal SHA-256 implementation. Kept private to this module. Standard
/// algorithm; tested against known vectors below.
mod sha256 {
    pub(super) const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    const H0: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    pub struct Hasher {
        h: [u32; 8],
        buf: Vec<u8>,
        len: u64,
    }

    impl Hasher {
        pub fn new() -> Self {
            Self {
                h: H0,
                buf: Vec::new(),
                len: 0,
            }
        }
        pub fn update(&mut self, bytes: &[u8]) {
            self.len += bytes.len() as u64;
            self.buf.extend_from_slice(bytes);
            while self.buf.len() >= 64 {
                let block: [u8; 64] = self.buf[..64].try_into().unwrap();
                Self::compress(&mut self.h, &block);
                self.buf.drain(..64);
            }
        }
        pub fn finalize_hex(mut self) -> String {
            let bit_len = self.len.wrapping_mul(8);
            self.buf.push(0x80);
            while self.buf.len() % 64 != 56 {
                self.buf.push(0);
            }
            self.buf.extend_from_slice(&bit_len.to_be_bytes());
            while self.buf.len() >= 64 {
                let block: [u8; 64] = self.buf[..64].try_into().unwrap();
                Self::compress(&mut self.h, &block);
                self.buf.drain(..64);
            }
            let mut out = String::with_capacity(64);
            for word in self.h {
                out.push_str(&format!("{:08x}", word));
            }
            out
        }

        fn compress(h: &mut [u32; 8], block: &[u8; 64]) {
            let mut w = [0u32; 64];
            for i in 0..16 {
                w[i] = u32::from_be_bytes(block[i * 4..i * 4 + 4].try_into().unwrap());
            }
            for i in 16..64 {
                let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
                let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
                w[i] = w[i - 16]
                    .wrapping_add(s0)
                    .wrapping_add(w[i - 7])
                    .wrapping_add(s1);
            }
            let mut a = h[0];
            let mut b = h[1];
            let mut c = h[2];
            let mut d = h[3];
            let mut e = h[4];
            let mut f = h[5];
            let mut g = h[6];
            let mut hh = h[7];
            for i in 0..64 {
                let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
                let ch = (e & f) ^ (!e & g);
                let t1 = hh
                    .wrapping_add(s1)
                    .wrapping_add(ch)
                    .wrapping_add(K[i])
                    .wrapping_add(w[i]);
                let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
                let maj = (a & b) ^ (a & c) ^ (b & c);
                let t2 = s0.wrapping_add(maj);
                hh = g;
                g = f;
                f = e;
                e = d.wrapping_add(t1);
                d = c;
                c = b;
                b = a;
                a = t1.wrapping_add(t2);
            }
            h[0] = h[0].wrapping_add(a);
            h[1] = h[1].wrapping_add(b);
            h[2] = h[2].wrapping_add(c);
            h[3] = h[3].wrapping_add(d);
            h[4] = h[4].wrapping_add(e);
            h[5] = h[5].wrapping_add(f);
            h[6] = h[6].wrapping_add(g);
            h[7] = h[7].wrapping_add(hh);
        }
    }
}

/// Collect every project-relative path referenced anywhere in the manifest, normalized
/// to forward-slash strings for portable cross-platform comparison.
fn collect_referenced_paths(manifest: &Manifest) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();
    for n in manifest.nodes.values() {
        if let Some(p) = &n.path {
            out.insert(p.replace('\\', "/"));
        }
    }
    for s in manifest.sources.values() {
        if let Some(p) = &s.path {
            out.insert(p.replace('\\', "/"));
        }
    }
    out
}

/// Walk standard dbt directories under `project_dir` and return any `.sql` file that
/// isn't already in `referenced`. We intentionally only flag SQL (not YAML) as
/// "untracked" — YAML files often live outside the manifest's path list, so we'd get
/// noisy false positives.
fn scan_untracked_sql(project_dir: &Path, referenced: &HashSet<String>) -> Result<Vec<String>> {
    const DBT_ROOTS: &[&str] = &["models", "seeds", "snapshots", "tests", "analyses"];
    let mut untracked = Vec::new();
    for root in DBT_ROOTS {
        let dir = project_dir.join(root);
        if !dir.exists() {
            continue;
        }
        for entry in walkdir::WalkDir::new(&dir)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            if !ext.eq_ignore_ascii_case("sql") {
                continue;
            }
            let rel = path
                .strip_prefix(project_dir)
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|_| path.to_path_buf());
            let rel_str = normalize_path(&rel);
            if !referenced.contains(&rel_str) {
                untracked.push(rel_str);
            }
        }
    }
    untracked.sort();
    Ok(untracked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};

    fn write_at(path: &Path, mtime: SystemTime, contents: &str) {
        fs::write(path, contents).unwrap();
        // Windows requires write access to call set_modified; read-only File::open
        // panics with PermissionDenied. OpenOptions::write works on all platforms.
        let f = fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_modified(mtime).unwrap();
    }

    fn touch_mtime(path: &Path, mtime: SystemTime) {
        let f = fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_modified(mtime).unwrap();
    }

    fn write_manifest(dir: &Path, paths: &[&str]) -> PathBuf {
        let mut nodes_json = String::new();
        let mut sources_json = String::new();
        for (i, p) in paths.iter().enumerate() {
            if p.ends_with(".sql") {
                nodes_json.push_str(&format!(
                    r#""model.x.n{i}": {{ "unique_id": "model.x.n{i}", "name": "n{i}", "resource_type": "model", "path": "{p}" }}{}"#,
                    if i + 1 < paths.len() { "," } else { "" }
                ));
            } else {
                sources_json.push_str(&format!(
                    r#""source.x.s.n{i}": {{ "unique_id": "source.x.s.n{i}", "name": "n{i}", "source_name": "s", "path": "{p}" }}{}"#,
                    if i + 1 < paths.len() { "," } else { "" }
                ));
            }
        }
        let json = format!(
            r#"{{"nodes": {{{}}}, "sources": {{{}}}, "exposures": {{}}}}"#,
            nodes_json, sources_json
        );
        let manifest_path = dir.join("target").join("manifest.json");
        fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        fs::write(&manifest_path, json).unwrap();
        manifest_path
    }

    #[test]
    fn test_fresh_manifest_is_clean() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        fs::create_dir_all(project.join("models/marts")).unwrap();

        let past = SystemTime::now() - Duration::from_secs(120);
        write_at(&project.join("models/marts/orders.sql"), past, "select 1");

        let manifest = write_manifest(project, &["models/marts/orders.sql"]);
        // Bump the manifest mtime to NOW so the SQL file is older.
        touch_mtime(&manifest, SystemTime::now());

        let report = check_manifest(project, &manifest).unwrap();
        assert!(
            !report.is_stale,
            "fresh project should be clean: {:?}",
            report
        );
        assert!(report.missing.is_empty());
        assert!(report.modified.is_empty());
        assert!(report.untracked.is_empty());
    }

    #[test]
    fn test_missing_file_flagged() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        fs::create_dir_all(project.join("models/marts")).unwrap();
        let manifest = write_manifest(project, &["models/marts/orders.sql"]);
        // Do NOT create the referenced SQL file.
        let report = check_manifest(project, &manifest).unwrap();
        assert!(report.is_stale);
        assert_eq!(report.missing, vec!["models/marts/orders.sql".to_string()]);
    }

    #[test]
    fn test_sha256_known_vectors() {
        // NIST FIPS 180-4 §B.1 known answers.
        assert_eq!(
            sha256_hex_impl(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex_impl(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn test_content_hash_detects_change_with_checksum() {
        // When the manifest carries a SHA-256 checksum, modifications are
        // detected via content hash even if mtime is OLDER than the manifest.
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        fs::create_dir_all(project.join("models/marts")).unwrap();

        let sql_path = project.join("models/marts/orders.sql");
        write_at(&sql_path, SystemTime::now(), "select 2");
        // Compute the *expected* hash of an OLD version and write the manifest
        // with that hash. The current file has different content.
        let old_hash = sha256_hex_impl(b"select 1");
        let manifest_json = format!(
            r#"{{"nodes":{{"model.x.orders":{{"unique_id":"model.x.orders","name":"orders","resource_type":"model","path":"models/marts/orders.sql","checksum":{{"name":"sha256","checksum":"{}"}}}}}},"sources":{{}},"exposures":{{}}}}"#,
            old_hash
        );
        let manifest = project.join("target").join("manifest.json");
        fs::create_dir_all(manifest.parent().unwrap()).unwrap();
        fs::write(&manifest, manifest_json).unwrap();
        // Manifest mtime AFTER the SQL file so the mtime heuristic would miss
        // the modification. Only the content hash check should fire.
        touch_mtime(&manifest, SystemTime::now() + Duration::from_secs(60));

        let report = check_manifest(project, &manifest).unwrap();
        assert!(report.is_stale, "should be stale via content hash");
        assert_eq!(report.modified.len(), 1);
        assert_eq!(report.modified[0].file, "models/marts/orders.sql");
        assert_eq!(
            report.modified[0].detection,
            DetectionMethod::ContentHash,
            "expected ContentHash detection, got {:?}",
            report.modified[0].detection
        );
    }

    #[test]
    fn test_modified_file_flagged() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        fs::create_dir_all(project.join("models/marts")).unwrap();

        let manifest = write_manifest(project, &["models/marts/orders.sql"]);
        // Manifest mtime in the past.
        touch_mtime(&manifest, SystemTime::now() - Duration::from_secs(300));
        // SQL file fresher than the manifest.
        write_at(
            &project.join("models/marts/orders.sql"),
            SystemTime::now(),
            "select 2",
        );

        let report = check_manifest(project, &manifest).unwrap();
        assert!(report.is_stale);
        assert_eq!(report.modified.len(), 1);
        assert_eq!(report.modified[0].file, "models/marts/orders.sql");
        // No SHA-256 checksum in our hand-rolled manifest, so fallback path.
        assert_eq!(report.modified[0].detection, DetectionMethod::Mtime);
    }

    #[test]
    fn test_untracked_file_flagged() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        fs::create_dir_all(project.join("models/marts")).unwrap();

        let manifest = write_manifest(project, &["models/marts/orders.sql"]);
        write_at(
            &project.join("models/marts/orders.sql"),
            SystemTime::now() - Duration::from_secs(120),
            "select 1",
        );
        write_at(
            &project.join("models/marts/brand_new.sql"),
            SystemTime::now() - Duration::from_secs(120),
            "select 2",
        );
        // Manifest is the freshest thing on disk → no "modified" entry, but the
        // brand_new.sql isn't referenced → untracked.
        touch_mtime(&manifest, SystemTime::now());

        let report = check_manifest(project, &manifest).unwrap();
        assert!(report.is_stale);
        assert_eq!(
            report.untracked,
            vec!["models/marts/brand_new.sql".to_string()]
        );
        assert!(report.missing.is_empty());
        assert!(report.modified.is_empty());
    }
}

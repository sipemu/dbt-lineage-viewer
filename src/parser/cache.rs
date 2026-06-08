//! On-disk cache of parsed-SQL outputs keyed by file content hash. Skips
//! re-parsing unchanged files on warm runs (#34). Used by the graph builder's
//! parallel parse pipeline (#33).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::sql::{RefCall, SourceCall, SqlConfig};

/// Cached parse outputs for one SQL file. Everything here is deterministically
/// derivable from the file's bytes, so hash-keyed caching is safe.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParsedSqlFile {
    pub refs: Vec<RefCall>,
    pub sources: Vec<SourceCall>,
    /// Calls to detected wrapper macros, materialized as additional ref/source calls.
    pub wrapped_refs: Vec<RefCall>,
    pub wrapped_sources: Vec<SourceCall>,
    pub columns: Vec<String>,
    pub config: SqlConfigSerde,
}

/// Mirror of `SqlConfig` with Serialize/Deserialize. Kept here so we don't have
/// to leak serde derives into the SQL parser module.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SqlConfigSerde {
    pub materialized: Option<String>,
    pub tags: Vec<String>,
}

impl From<SqlConfig> for SqlConfigSerde {
    fn from(value: SqlConfig) -> Self {
        Self {
            materialized: value.materialized,
            tags: value.tags,
        }
    }
}

impl From<SqlConfigSerde> for SqlConfig {
    fn from(value: SqlConfigSerde) -> Self {
        Self {
            materialized: value.materialized,
            tags: value.tags,
        }
    }
}

/// Persisted cache layout. Versioned so format changes invalidate stale caches.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CacheFile {
    pub version: u32,
    pub entries: HashMap<String, CacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// blake3 hash of the file's raw bytes.
    pub content_hash: [u8; 32],
    pub parsed: ParsedSqlFile,
}

const CACHE_VERSION: u32 = 1;
const CACHE_REL_PATH: &str = ".dbt-lineage/cache.bin";

/// Thread-safe parse cache. Constructed once per build, reused across all
/// parse-time file reads. After parsing finishes, call `save` to persist.
pub struct ParseCache {
    by_path: Mutex<HashMap<String, CacheEntry>>,
    /// Cache file path; None means "memory only" (used in tests).
    cache_path: Option<PathBuf>,
}

impl ParseCache {
    /// Load from `<project_dir>/.dbt-lineage/cache.bin` if it exists. Returns an
    /// empty cache on any error (corrupt file, version mismatch, etc.) — caching
    /// is opportunistic, never load-bearing.
    pub fn load(project_dir: &Path) -> Self {
        let cache_path = project_dir.join(CACHE_REL_PATH);
        let by_path = std::fs::read(&cache_path)
            .ok()
            .and_then(|bytes| bincode::deserialize::<CacheFile>(&bytes).ok())
            .filter(|cache| cache.version == CACHE_VERSION)
            .map(|cache| cache.entries)
            .unwrap_or_default();
        Self {
            by_path: Mutex::new(by_path),
            cache_path: Some(cache_path),
        }
    }

    /// In-memory only. Used by `--no-cache` and by tests.
    pub fn ephemeral() -> Self {
        Self {
            by_path: Mutex::new(HashMap::new()),
            cache_path: None,
        }
    }

    /// Look up a parsed file by absolute path + content hash. Returns the cached
    /// value when both match.
    pub fn get(&self, path: &Path, hash: &[u8; 32]) -> Option<ParsedSqlFile> {
        let key = path.to_string_lossy().to_string();
        let guard = self.by_path.lock().ok()?;
        let entry = guard.get(&key)?;
        if entry.content_hash == *hash {
            Some(entry.parsed.clone())
        } else {
            None
        }
    }

    /// Insert or overwrite an entry.
    pub fn put(&self, path: &Path, hash: [u8; 32], parsed: ParsedSqlFile) {
        let key = path.to_string_lossy().to_string();
        if let Ok(mut guard) = self.by_path.lock() {
            guard.insert(
                key,
                CacheEntry {
                    content_hash: hash,
                    parsed,
                },
            );
        }
    }

    /// Persist to disk. Best-effort — failures are logged to stderr but never
    /// propagated; correctness doesn't depend on the cache.
    pub fn save(&self) {
        let Some(path) = &self.cache_path else {
            return;
        };
        let Ok(guard) = self.by_path.lock() else {
            return;
        };
        let cache = CacheFile {
            version: CACHE_VERSION,
            entries: guard.clone(),
        };
        let bytes = match bincode::serialize(&cache) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("dbt-lineage: cache save (serialize) failed: {}", e);
                return;
            }
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(e) = std::fs::write(path, bytes) {
            eprintln!("dbt-lineage: cache save (write) failed: {}", e);
        }
    }
}

/// Hash file contents with blake3. Used as the cache key.
pub fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn sample() -> ParsedSqlFile {
        ParsedSqlFile {
            refs: vec![RefCall {
                package: None,
                name: "orders".into(),
            }],
            sources: vec![],
            wrapped_refs: vec![],
            wrapped_sources: vec![],
            columns: vec!["order_id".into()],
            config: SqlConfigSerde::default(),
        }
    }

    #[test]
    fn test_get_returns_cached_on_hash_match() {
        let cache = ParseCache::ephemeral();
        let path = Path::new("/tmp/x.sql");
        let hash = hash_bytes(b"select 1");
        cache.put(path, hash, sample());
        let hit = cache.get(path, &hash).unwrap();
        assert_eq!(hit.refs[0].name, "orders");
    }

    #[test]
    fn test_get_misses_on_hash_mismatch() {
        let cache = ParseCache::ephemeral();
        let path = Path::new("/tmp/x.sql");
        cache.put(path, hash_bytes(b"old"), sample());
        let miss = cache.get(path, &hash_bytes(b"new"));
        assert!(miss.is_none());
    }

    #[test]
    fn test_persist_and_reload_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ParseCache::load(tmp.path());
        let path = Path::new("/tmp/x.sql");
        let hash = hash_bytes(b"select 1");
        cache.put(path, hash, sample());
        cache.save();
        assert!(tmp.path().join(CACHE_REL_PATH).exists());

        let reloaded = ParseCache::load(tmp.path());
        let hit = reloaded.get(path, &hash).unwrap();
        assert_eq!(hit.refs[0].name, "orders");
    }

    #[test]
    fn test_version_mismatch_clears_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_path = tmp.path().join(CACHE_REL_PATH);
        fs::create_dir_all(cache_path.parent().unwrap()).unwrap();
        // Write a corrupt / wrong-version cache.
        let stale = CacheFile {
            version: 999,
            entries: HashMap::new(),
        };
        fs::write(&cache_path, bincode::serialize(&stale).unwrap()).unwrap();

        let reloaded = ParseCache::load(tmp.path());
        let any = reloaded.get(Path::new("/anything"), &hash_bytes(b"x"));
        assert!(any.is_none());
    }
}

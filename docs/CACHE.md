# Parse cache

`dbt-lineage` stores parsed-SQL outputs on disk at
`<project_dir>/.dbt-lineage/cache.bin`. The cache lets warm runs skip
file parsing entirely when nothing has changed.

## Contract

- **Best-effort, never load-bearing.** Correctness never depends on the
  cache. Any failure (read error, version mismatch, corrupted bytes) is
  silently treated as a cache miss.
- **Keyed by content hash.** Each entry stores a [blake3](https://github.com/BLAKE3-team/BLAKE3)
  hash of the file's raw bytes. Cache hits require an exact byte match.
- **Format version bumps invalidate.** The cache file carries a `version:
  u32` field. When the format changes (new field on `ParsedSqlFile`, etc.)
  the version bumps; older caches are treated as empty.
- **Per-project.** The cache lives inside the project directory, not in a
  user-global location. Different projects don't share entries.

## File format

Serialized with [bincode](https://github.com/bincode-org/bincode) v1. The
on-disk schema:

```rust
struct CacheFile {
    version: u32,            // currently 1
    entries: HashMap<String, CacheEntry>,
}

struct CacheEntry {
    content_hash: [u8; 32],  // blake3 hash of file bytes
    parsed: ParsedSqlFile,   // refs, sources, columns, config (serde)
}
```

Keys are absolute file paths as strings. Entries that no longer correspond
to existing files accumulate until the next cache rewrite.

## Operations

- **Reading**: at the start of a `build_graph` call, the cache is loaded
  once. Each per-file parse first hashes the bytes and consults the cache.
  Cache hits skip parsing entirely.
- **Writing**: cache misses populate the in-memory map. After the build
  finishes, the whole map is written back to disk in a single bincode
  serialization.
- **Concurrency**: the in-memory map is protected by a `Mutex`. Multiple
  rayon worker threads can populate it in parallel. Only one process
  should write to a given project's cache at a time; concurrent writers
  may overwrite each other's updates (last writer wins) but won't corrupt
  the file thanks to atomic write semantics on most platforms.

## Practical guidance

- **Safe to delete.** `rm .dbt-lineage/cache.bin` at any time. The next
  run rebuilds it from scratch.
- **Recommend gitignoring.** It's a per-machine build artifact. Add
  `.dbt-lineage/` to your `.gitignore`.
- **`--no-cache` bypasses.** Use when benchmarking parser changes or when
  you suspect the cache contains stale data.
- **Doesn't apply in manifest mode.** The cache is only consulted by the
  SQL-parse pipeline. `--manifest <path>` reads `manifest.json` directly
  without involving the cache.

## When the cache helps

- Most: large monorepos with hundreds of SQL files, where parsing is the
  bottleneck.
- Less: small projects (parsing finishes in single-digit milliseconds
  anyway) or runs where most files have changed (cache misses on every file).

`BENCHMARKS.md` has baseline cold-vs-warm numbers on a 500-model fixture.

## Version history

| Cache version | dbt-lineage version | Schema change |
|---|---|---|
| 1 | 0.5.0 + | initial |

#!/usr/bin/env node
// Download the dbt-lineage release binary for one VS Code target into bin/.
//
//   node scripts/fetch-binary.mjs <vscode-target> [cli-version]
//
// <vscode-target>: linux-x64 | linux-arm64 | darwin-x64 | darwin-arm64 | win32-x64
// [cli-version]:   defaults to "dbtLineageVersion" in package.json.
//
// The archive's .sha256 is verified before extraction. Downloads are cached in
// .cache/. bin/ is emptied first so a package never carries two binaries.
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const TARGETS = {
  "linux-x64": "x86_64-unknown-linux-gnu",
  "linux-arm64": "aarch64-unknown-linux-gnu",
  "darwin-x64": "x86_64-apple-darwin",
  "darwin-arm64": "aarch64-apple-darwin",
  "win32-x64": "x86_64-pc-windows-msvc",
};

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));

const target = process.argv[2];
const version = process.argv[3] ?? pkg.dbtLineageVersion;
if (!TARGETS[target] || !version) {
  console.error(
    `usage: fetch-binary.mjs <${Object.keys(TARGETS).join("|")}> [cli-version]`,
  );
  process.exit(2);
}

const triple = TARGETS[target];
const windows = target.startsWith("win32");
const stem = `dbt-lineage-v${version}-${triple}`;
const archive = `${stem}.${windows ? "zip" : "tar.gz"}`;
const base = `https://github.com/sipemu/dbt-lineage-viewer/releases/download/v${version}`;

async function download(url) {
  const res = await fetch(url, { redirect: "follow" });
  if (!res.ok) {
    throw new Error(`GET ${url}: HTTP ${res.status}`);
  }
  return Buffer.from(await res.arrayBuffer());
}

const sha256 = (buf) => createHash("sha256").update(buf).digest("hex");

const cache = path.join(root, ".cache");
fs.mkdirSync(cache, { recursive: true });
const archivePath = path.join(cache, archive);

const expected = (await download(`${base}/${archive}.sha256`))
  .toString("utf8")
  .trim()
  .split(/\s+/)[0]
  .toLowerCase();
if (!/^[0-9a-f]{64}$/.test(expected)) {
  throw new Error(`malformed checksum file for ${archive}`);
}

let data = fs.existsSync(archivePath) ? fs.readFileSync(archivePath) : undefined;
if (!data || sha256(data) !== expected) {
  console.log(`downloading ${base}/${archive}`);
  data = await download(`${base}/${archive}`);
  const actual = sha256(data);
  if (actual !== expected) {
    throw new Error(`checksum mismatch for ${archive}: ${actual} != ${expected}`);
  }
  fs.writeFileSync(archivePath, data);
}
console.log(`sha256 ok: ${expected}`);

const tmp = fs.mkdtempSync(path.join(cache, "extract-"));
try {
  if (windows) {
    execFileSync("unzip", ["-q", "-o", archivePath, "-d", tmp], { stdio: "inherit" });
  } else {
    execFileSync("tar", ["-xzf", archivePath, "-C", tmp], { stdio: "inherit" });
  }
  const exe = windows ? "dbt-lineage.exe" : "dbt-lineage";
  const src = [path.join(tmp, stem, exe), path.join(tmp, exe)].find((p) =>
    fs.existsSync(p),
  );
  if (!src) {
    throw new Error(`${exe} not found in ${archive}`);
  }
  const bin = path.join(root, "bin");
  fs.rmSync(bin, { recursive: true, force: true });
  fs.mkdirSync(bin);
  const dest = path.join(bin, exe);
  fs.copyFileSync(src, dest);
  fs.chmodSync(dest, 0o755);
  console.log(`bin/${exe} <- ${archive}`);
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}

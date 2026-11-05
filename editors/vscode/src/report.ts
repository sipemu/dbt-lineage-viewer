// Runs `dbt-lineage ci report`. No `vscode` import (see render.ts).
import { execFile } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

function run(
  cmd: string,
  args: string[],
  cwd: string,
): Promise<{ code: number; stdout: string; stderr: string; spawnError?: Error }> {
  return new Promise((resolve) => {
    execFile(cmd, args, { cwd, maxBuffer: 64 * 1024 * 1024 }, (err, stdout, stderr) => {
      const e = err as (NodeJS.ErrnoException & { code?: number | string }) | null;
      if (e && typeof e.code === "string") {
        // ENOENT / EACCES: the program could not be started at all.
        resolve({ code: -1, stdout, stderr, spawnError: e });
        return;
      }
      resolve({ code: e ? Number(e.code ?? 1) : 0, stdout, stderr });
    });
  });
}

/**
 * The ref to compare against. `auto` (or empty): the remote's default branch
 * (`origin/HEAD`), else a local `main`, else `master`, else `main` (which the
 * CLI also tries as `origin/main`).
 */
export async function resolveBase(setting: string | undefined, cwd: string): Promise<string> {
  const value = (setting ?? "").trim();
  if (value && value !== "auto") {
    return value;
  }
  const head = await run("git", ["symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD"], cwd);
  if (head.code === 0 && head.stdout.trim()) {
    return head.stdout.trim();
  }
  for (const candidate of ["main", "master"]) {
    const r = await run("git", ["rev-parse", "--verify", "--quiet", `${candidate}^{commit}`], cwd);
    if (r.code === 0) {
      return candidate;
    }
  }
  return "main";
}

export interface ReportResult {
  markdown: string;
  base: string;
  /** User-facing problems (diff skipped, …); the report is still shown. */
  notices: string[];
  /** Raw stderr of the CLI, for the output channel. */
  stderr: string;
}

export class ReportError extends Error {}

/** Turn the CLI's "diff skipped: …" line into an actionable message. */
export function explainSkip(reason: string, base: string): string {
  if (/not a git repository/i.test(reason)) {
    return (
      "This dbt project is not inside a git repository, so there is nothing to compare. " +
      "The impact report needs git history; only lint and the project summary are shown."
    );
  }
  if (/invalid git ref/i.test(reason)) {
    return (
      `Base ref "${base}" was not found (also tried origin/${base}). ` +
      'Set "dbtLineage.baseRef" to the branch your changes will be merged into.'
    );
  }
  return `Comparison against "${base}" was skipped: ${reason}`;
}

export async function runReport(binary: string, projectDir: string, base: string): Promise<ReportResult> {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "dbt-lineage-impact-"));
  const out = path.join(tmp, "report.md");
  try {
    const r = await run(
      binary,
      ["ci", "report", "--base", base, "--fail-on", "never", "--out", out, "-p", projectDir],
      projectDir,
    );
    if (r.spawnError) {
      throw new ReportError(`could not run "${binary}": ${r.spawnError.message}`);
    }
    if (r.code !== 0 || !fs.existsSync(out)) {
      const msg = r.stderr.trim() || `exited with code ${r.code}`;
      if (/unrecognized subcommand|unexpected argument/i.test(msg)) {
        throw new ReportError(
          `"${binary}" does not support \`ci report\`; it needs dbt-lineage 0.10 or newer (${msg}).`,
        );
      }
      throw new ReportError(msg);
    }
    const notices: string[] = [];
    for (const line of r.stderr.split(/\r?\n/)) {
      const m = /(\w+) skipped: (.*)$/.exec(line);
      if (m) {
        notices.push(m[1] === "diff" ? explainSkip(m[2], base) : `${m[1]} skipped: ${m[2]}`);
      }
    }
    return { markdown: fs.readFileSync(out, "utf8"), base, notices, stderr: r.stderr };
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

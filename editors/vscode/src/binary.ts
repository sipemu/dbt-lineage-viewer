import * as fs from "fs";
import * as path from "path";
import * as vscode from "vscode";

export type BinarySource = "setting" | "bundled" | "path";

export interface Binary {
  command: string;
  source: BinarySource;
}

/**
 * The dbt-lineage binary to run: `dbtLineage.path` when set, else the binary
 * shipped in this platform's package (bin/), else `dbt-lineage` on PATH (the
 * universal package carries no binary).
 */
export function resolveBinary(context: vscode.ExtensionContext): Binary {
  const configured = vscode.workspace
    .getConfiguration("dbtLineage")
    .get<string>("path")
    ?.trim();
  if (configured) {
    return { command: configured, source: "setting" };
  }
  const exe = process.platform === "win32" ? "dbt-lineage.exe" : "dbt-lineage";
  const bundled = path.join(context.extensionPath, "bin", exe);
  if (fs.existsSync(bundled)) {
    ensureExecutable(bundled);
    return { command: bundled, source: "bundled" };
  }
  return { command: "dbt-lineage", source: "path" };
}

/** VSIX extraction does not always keep the mode bits; restore +x on unix. */
function ensureExecutable(file: string): void {
  if (process.platform === "win32") {
    return;
  }
  try {
    fs.accessSync(file, fs.constants.X_OK);
  } catch {
    try {
      fs.chmodSync(file, 0o755);
    } catch {
      // Read-only install location: the spawn error will say so.
    }
  }
}

export function installHint(binary: Binary): string {
  switch (binary.source) {
    case "setting":
      return 'Check the "dbtLineage.path" setting, or clear it to use the bundled binary.';
    case "bundled":
      return 'The bundled binary could not be started; set "dbtLineage.path" to your own dbt-lineage.';
    case "path":
      return (
        "This package has no bundled binary for your platform. Install dbt-lineage " +
        "(`pip install dbt-lineage-cli`, `brew install sipemu/tap/dbt-lineage` or " +
        '`cargo binstall dbt-lineage`) or set "dbtLineage.path".'
      );
  }
}

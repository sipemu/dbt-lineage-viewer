import { execFile } from "child_process";
import * as fs from "fs";
import * as path from "path";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
} from "vscode-languageclient/node";

let client: LanguageClient | undefined;

function binary(): string {
  return (
    vscode.workspace.getConfiguration("dbtLineage").get<string>("path") ||
    "dbt-lineage"
  );
}

async function startClient(): Promise<void> {
  const command = binary();
  const serverOptions: ServerOptions = { command, args: ["lsp"] };
  const clientOptions: LanguageClientOptions = {
    documentSelector: [
      { scheme: "file", language: "sql" },
      { scheme: "file", language: "jinja-sql" },
      { scheme: "file", language: "yaml" },
    ],
  };
  client = new LanguageClient(
    "dbtLineage",
    "dbt-lineage",
    serverOptions,
    clientOptions,
  );
  try {
    await client.start();
  } catch (err) {
    client = undefined;
    vscode.window.showErrorMessage(
      `dbt-lineage: could not start "${command} lsp" (${err}). Install it with ` +
        "`brew install sipemu/tap/dbt-lineage` or `cargo binstall dbt-lineage`, " +
        "or set dbtLineage.path.",
    );
  }
}

/** Nearest ancestor directory of `file` containing dbt_project.yml. */
function projectDir(file: string): string | undefined {
  let dir = path.dirname(file);
  for (;;) {
    if (fs.existsSync(path.join(dir, "dbt_project.yml"))) {
      return dir;
    }
    const parent = path.dirname(dir);
    if (parent === dir) {
      return undefined;
    }
    dir = parent;
  }
}

function showLineage(model?: string): void {
  const file = vscode.window.activeTextEditor?.document.uri.fsPath;
  const name = model ?? (file ? path.parse(file).name : undefined);
  const dir =
    (file && projectDir(file)) ??
    vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
  if (!name || !dir) {
    vscode.window.showWarningMessage(
      "dbt-lineage: open a model file inside a dbt project first.",
    );
    return;
  }
  const depth = String(
    vscode.workspace.getConfiguration("dbtLineage").get<number>("lineageDepth") ?? 3,
  );
  execFile(
    binary(),
    [name, "-p", dir, "-u", depth, "-d", depth, "-o", "html"],
    { maxBuffer: 64 * 1024 * 1024 },
    (err, stdout, stderr) => {
      if (err) {
        vscode.window.showErrorMessage(
          `dbt-lineage: ${stderr.trim() || err.message}`,
        );
        return;
      }
      const panel = vscode.window.createWebviewPanel(
        "dbtLineage",
        `Lineage: ${name}`,
        vscode.ViewColumn.Beside,
        { enableScripts: true },
      );
      panel.webview.html = stdout;
    },
  );
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  context.subscriptions.push(
    vscode.commands.registerCommand("dbt-lineage.showLineage", showLineage),
    vscode.commands.registerCommand("dbt-lineage.restartServer", async () => {
      await client?.stop();
      await startClient();
    }),
  );
  await startClient();
}

export async function deactivate(): Promise<void> {
  await client?.stop();
}

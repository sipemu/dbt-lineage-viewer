import { execFile } from "child_process";
import * as fs from "fs";
import * as path from "path";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
} from "vscode-languageclient/node";
import { Binary, installHint, resolveBinary } from "./binary";
import { ImpactView } from "./impact";

let client: LanguageClient | undefined;
let extensionContext: vscode.ExtensionContext;
let output: vscode.OutputChannel;

function binary(): Binary {
  return resolveBinary(extensionContext);
}

async function startClient(): Promise<void> {
  const bin = binary();
  const serverOptions: ServerOptions = { command: bin.command, args: ["lsp"] };
  const clientOptions: LanguageClientOptions = {
    documentSelector: [
      { scheme: "file", language: "sql" },
      { scheme: "file", language: "jinja-sql" },
      { scheme: "file", language: "yaml" },
    ],
    outputChannel: output,
  };
  client = new LanguageClient("dbtLineage", "dbt-lineage", serverOptions, clientOptions);
  try {
    await client.start();
  } catch (err) {
    client = undefined;
    vscode.window.showErrorMessage(
      `dbt-lineage: could not start "${bin.command} lsp" (${err}). ${installHint(bin)}`,
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

/** dbt project of the active file, else the first workspace folder with one. */
function activeProject(): string | undefined {
  const doc = vscode.window.activeTextEditor?.document;
  const fromFile = doc?.uri.scheme === "file" ? projectDir(doc.uri.fsPath) : undefined;
  if (fromFile) {
    return fromFile;
  }
  return vscode.workspace.workspaceFolders
    ?.map((f) => f.uri.fsPath)
    .find((d) => fs.existsSync(path.join(d, "dbt_project.yml")));
}

function showLineage(model?: string): void {
  const file = vscode.window.activeTextEditor?.document.uri.fsPath;
  const name = model ?? (file ? path.parse(file).name : undefined);
  const dir =
    (file && projectDir(file)) ?? vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
  if (!name || !dir) {
    vscode.window.showWarningMessage(
      "dbt-lineage: open a model file inside a dbt project first.",
    );
    return;
  }
  const depth = String(
    vscode.workspace.getConfiguration("dbtLineage").get<number>("lineageDepth") ?? 3,
  );
  const bin = binary();
  execFile(
    bin.command,
    [name, "-p", dir, "-u", depth, "-d", depth, "-o", "html"],
    { maxBuffer: 64 * 1024 * 1024 },
    (err, stdout, stderr) => {
      if (err) {
        const spawnFailed = typeof (err as NodeJS.ErrnoException).code === "string";
        vscode.window.showErrorMessage(
          `dbt-lineage: ${stderr.trim() || err.message}${spawnFailed ? ` ${installHint(bin)}` : ""}`,
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
  extensionContext = context;
  output = vscode.window.createOutputChannel("dbt-lineage");
  const impact = new ImpactView(context, binary, activeProject, output);
  context.subscriptions.push(
    output,
    impact,
    vscode.commands.registerCommand("dbt-lineage.showImpact", () => impact.show()),
    vscode.commands.registerCommand("dbt-lineage.showLineage", showLineage),
    vscode.commands.registerCommand("dbt-lineage.restartServer", async () => {
      await client?.stop();
      await startClient();
    }),
    vscode.workspace.onDidChangeConfiguration(async (e) => {
      if (e.affectsConfiguration("dbtLineage.path")) {
        await client?.stop();
        await startClient();
      }
    }),
  );
  const bin = binary();
  output.appendLine(`dbt-lineage binary: ${bin.command} (${bin.source})`);
  await startClient();
}

export async function deactivate(): Promise<void> {
  await client?.stop();
}

import * as crypto from "crypto";
import * as path from "path";
import * as vscode from "vscode";
import { Binary, installHint } from "./binary";
import { changedCount, escapeHtml, page, renderReport } from "./render";
import { ReportError, resolveBase, runReport } from "./report";

/** Panel + status-bar item for "dbt-lineage: Show impact of my changes". */
export class ImpactView implements vscode.Disposable {
  private panel: vscode.WebviewPanel | undefined;
  private projectDir: string | undefined;
  private running = false;
  private rerun = false;
  private saveTimer: NodeJS.Timeout | undefined;
  private readonly status: vscode.StatusBarItem;
  private readonly disposables: vscode.Disposable[] = [];

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly binary: () => Binary,
    private readonly findProject: () => string | undefined,
    private readonly output: vscode.OutputChannel,
  ) {
    this.status = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 50);
    this.status.command = "dbt-lineage.showImpact";
    this.setStatus("idle");
    this.status.show();
    this.disposables.push(
      this.status,
      vscode.workspace.onDidSaveTextDocument((doc) => this.onSave(doc)),
    );
  }

  dispose(): void {
    clearTimeout(this.saveTimer);
    this.panel?.dispose();
    this.disposables.forEach((d) => d.dispose());
  }

  /** Command entry point: pick the project of the active file and run. */
  async show(): Promise<void> {
    const dir = this.findProject();
    if (!dir) {
      vscode.window.showWarningMessage(
        "dbt-lineage: open a file inside a dbt project (a folder with dbt_project.yml) first.",
      );
      return;
    }
    this.projectDir = dir;
    this.ensurePanel().reveal(undefined, true);
    await this.refresh();
  }

  private ensurePanel(): vscode.WebviewPanel {
    if (this.panel) {
      return this.panel;
    }
    const roots = [
      vscode.Uri.joinPath(this.context.extensionUri, "media"),
      vscode.Uri.joinPath(this.context.extensionUri, "out"),
    ];
    const panel = vscode.window.createWebviewPanel(
      "dbtLineageImpact",
      "dbt impact",
      { viewColumn: vscode.ViewColumn.Beside, preserveFocus: true },
      { enableScripts: true, localResourceRoots: roots, retainContextWhenHidden: true },
    );
    panel.iconPath = vscode.Uri.joinPath(this.context.extensionUri, "icon.png");
    panel.webview.onDidReceiveMessage(
      (msg) => msg?.type === "refresh" && this.refresh(),
      undefined,
      this.disposables,
    );
    panel.onDidDispose(() => (this.panel = undefined), undefined, this.disposables);
    this.panel = panel;
    return panel;
  }

  private onSave(doc: vscode.TextDocument): void {
    if (!this.panel || !this.projectDir) {
      return;
    }
    if (!vscode.workspace.getConfiguration("dbtLineage").get<boolean>("impactOnSave")) {
      return;
    }
    const rel = path.relative(this.projectDir, doc.uri.fsPath);
    if (rel.startsWith("..") || path.isAbsolute(rel)) {
      return;
    }
    clearTimeout(this.saveTimer);
    this.saveTimer = setTimeout(() => this.refresh(), 400);
  }

  private async refresh(): Promise<void> {
    const dir = this.projectDir;
    const panel = this.panel;
    if (!dir || !panel) {
      return;
    }
    if (this.running) {
      this.rerun = true;
      return;
    }
    this.running = true;
    this.setStatus("running");
    const binary = this.binary();
    const setting = vscode.workspace.getConfiguration("dbtLineage").get<string>("baseRef");
    try {
      const base = await resolveBase(setting, dir);
      this.output.appendLine(`[impact] ${binary.command} ci report --base ${base} (in ${dir})`);
      const result = await runReport(binary.command, dir, base);
      if (result.stderr.trim()) {
        this.output.appendLine(result.stderr.trim());
      }
      for (const notice of result.notices) {
        const actions = notice.includes("dbtLineage.baseRef") ? ["Settings"] : [];
        vscode.window.showWarningMessage(`dbt-lineage: ${notice}`, ...actions).then((pick) => {
          if (pick) {
            vscode.commands.executeCommand("workbench.action.openSettings", "dbtLineage.baseRef");
          }
        });
      }
      const name = path.basename(dir);
      panel.title = `dbt impact: ${name}`;
      panel.webview.html = this.html(
        panel.webview,
        renderReport(result.markdown),
        `${name} · working tree vs ${base}`,
        result.notices,
      );
      const n = result.notices.length ? undefined : changedCount(result.markdown);
      this.setStatus("done", n);
    } catch (err) {
      const msg = err instanceof ReportError ? err.message : String(err);
      this.output.appendLine(`[impact] error: ${msg}`);
      const hint = /could not run/.test(msg) ? ` ${installHint(binary)}` : "";
      vscode.window.showErrorMessage(`dbt-lineage: ${msg}${hint}`);
      panel.webview.html = this.html(
        panel.webview,
        `<p>The impact report could not be produced.</p><pre>${escapeHtml(msg + hint)}</pre>`,
        path.basename(dir),
        [],
      );
      this.setStatus("error");
    } finally {
      this.running = false;
      if (this.rerun) {
        this.rerun = false;
        void this.refresh();
      }
    }
  }

  private html(webview: vscode.Webview, body: string, subtitle: string, notices: string[]): string {
    const uri = (...p: string[]) =>
      webview.asWebviewUri(vscode.Uri.joinPath(this.context.extensionUri, ...p)).toString();
    return page({
      body,
      cspSource: webview.cspSource,
      nonce: crypto.randomBytes(16).toString("base64"),
      cssUri: uri("media", "impact.css"),
      scriptUri: uri("media", "impact.js"),
      mermaidUri: uri("out", "mermaid.min.js"),
      subtitle,
      notices,
    });
  }

  private setStatus(state: "idle" | "running" | "done" | "error", changed?: number): void {
    const s = this.status;
    s.backgroundColor = undefined;
    switch (state) {
      case "idle":
        s.text = "$(git-compare) dbt impact";
        s.tooltip = "dbt-lineage: show what your changes affect downstream";
        break;
      case "running":
        s.text = "$(sync~spin) dbt impact";
        s.tooltip = "dbt-lineage: computing impact…";
        break;
      case "done":
        s.text =
          changed === undefined ? "$(git-compare) dbt impact" : `$(git-compare) dbt impact: ${changed} changed`;
        s.tooltip = "dbt-lineage: click to refresh the impact report";
        break;
      case "error":
        s.text = "$(warning) dbt impact";
        s.tooltip = "dbt-lineage: the impact report failed (see the dbt-lineage output)";
        s.backgroundColor = new vscode.ThemeColor("statusBarItem.warningBackground");
        break;
    }
  }
}

// Markdown report -> webview HTML. No `vscode` import, so scripts/ can reuse
// it outside the editor (tests, README screenshot).
import MarkdownIt from "markdown-it";

const md = new MarkdownIt({ html: false, linkify: false, typographer: false });

// ```mermaid fences become <pre class="mermaid">, rendered by media/impact.js.
const defaultFence = md.renderer.rules.fence!;
md.renderer.rules.fence = (tokens, idx, options, env, self) => {
  const token = tokens[idx];
  if (token.info.trim() === "mermaid") {
    return `<pre class="mermaid">${md.utils.escapeHtml(token.content)}</pre>\n`;
  }
  return defaultFence(tokens, idx, options, env, self);
};

const DETAILS_OPEN = /^<details><summary>(.*)<\/summary>\s*$/;
const DETAILS_CLOSE = /^<\/details>\s*$/;

/**
 * Render the `ci report` Markdown. Raw HTML stays disabled; the only HTML the
 * report emits (an HTML comment marker and `<details>` around the project
 * summary) is handled here line by line.
 */
export function renderReport(markdown: string): string {
  const parts: string[] = [];
  let buf: string[] = [];
  const flush = () => {
    if (buf.length) {
      parts.push(md.render(buf.join("\n")));
      buf = [];
    }
  };
  let inFence = false;
  for (const line of markdown.split(/\r?\n/)) {
    if (/^\s*(```|~~~)/.test(line)) {
      inFence = !inFence;
    }
    if (!inFence) {
      if (/^<!--.*-->\s*$/.test(line)) {
        continue;
      }
      const open = DETAILS_OPEN.exec(line);
      if (open) {
        flush();
        parts.push(`<details><summary>${md.renderInline(open[1])}</summary>`);
        continue;
      }
      if (DETAILS_CLOSE.test(line)) {
        flush();
        parts.push("</details>");
        continue;
      }
    }
    buf.push(line);
  }
  flush();
  return parts.join("\n");
}

export function escapeHtml(s: string): string {
  return md.utils.escapeHtml(s);
}

export interface PageOptions {
  /** Rendered report body (from renderReport). */
  body: string;
  /** webview.cspSource, or "" outside VS Code. */
  cspSource: string;
  nonce: string;
  cssUri: string;
  scriptUri: string;
  mermaidUri: string;
  /** Header line, e.g. "jaffle_demo · working tree vs main". */
  subtitle: string;
  /** Warnings shown above the report (escaped here). */
  notices: string[];
}

export function page(o: PageOptions): string {
  const csp = [
    "default-src 'none'",
    `img-src ${o.cspSource} data:`,
    // Mermaid writes <style> into the SVGs it generates.
    `style-src ${o.cspSource} 'unsafe-inline'`,
    `font-src ${o.cspSource}`,
    `script-src 'nonce-${o.nonce}'`,
  ].join("; ");
  const notices = o.notices
    .map((n) => `<div class="notice">${escapeHtml(n)}</div>`)
    .join("\n");
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Content-Security-Policy" content="${csp}">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="${o.cssUri}">
<title>dbt impact</title>
</head>
<body>
<header class="bar">
  <div>
    <div class="title">Impact of my changes</div>
    <div class="subtitle">${escapeHtml(o.subtitle)}</div>
  </div>
  <button id="refresh" type="button" title="Run the report again">Refresh</button>
</header>
${notices}
<main class="report">
${o.body}
</main>
<script nonce="${o.nonce}" src="${o.mermaidUri}"></script>
<script nonce="${o.nonce}" src="${o.scriptUri}"></script>
</body>
</html>`;
}

/** Changed-node count from the "Changes vs." summary line, if present. */
export function changedCount(markdown: string): number | undefined {
  const m = /\*\*(\d+)\*\* added · \*\*(\d+)\*\* modified · \*\*(\d+)\*\* removed nodes/.exec(
    markdown,
  );
  return m ? Number(m[1]) + Number(m[2]) + Number(m[3]) : undefined;
}

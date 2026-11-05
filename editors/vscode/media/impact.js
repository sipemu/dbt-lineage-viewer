// Webview script for the impact panel: renders the Mermaid diagram and wires
// the Refresh button. Runs under a nonce-only CSP; no inline handlers.
(function () {
  const vscode = typeof acquireVsCodeApi === "function" ? acquireVsCodeApi() : undefined;

  const refresh = document.getElementById("refresh");
  if (refresh) {
    refresh.addEventListener("click", () => vscode && vscode.postMessage({ type: "refresh" }));
  }

  if (typeof mermaid === "undefined") {
    return;
  }
  const cls = document.body.classList;
  const dark = cls.contains("vscode-dark") || cls.contains("vscode-high-contrast");
  mermaid.initialize({
    startOnLoad: false,
    securityLevel: "strict",
    theme: dark ? "dark" : "default",
    flowchart: { htmlLabels: false, useMaxWidth: true },
  });
  mermaid
    .run({ querySelector: "pre.mermaid" })
    .catch((err) => {
      document.querySelectorAll("pre.mermaid").forEach((el) => {
        el.classList.remove("mermaid");
        el.setAttribute("title", "Mermaid could not render this diagram: " + err);
      });
    })
    .finally(() => document.body.setAttribute("data-rendered", "1"));
})();

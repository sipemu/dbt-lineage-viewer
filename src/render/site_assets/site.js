// Client-side search over the JSON index embedded in index.html.
(function () {
  var data = document.getElementById("index");
  var input = document.getElementById("q");
  var list = document.getElementById("results");
  var more = document.getElementById("more");
  if (!data || !input || !list) return;
  var items = JSON.parse(data.textContent);
  var chips = Array.prototype.slice.call(document.querySelectorAll("#types .chip"));
  var LIMIT = 300;

  function esc(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function activeTypes() {
    return chips.filter(function (c) { return c.getAttribute("aria-pressed") === "true"; })
      .map(function (c) { return c.dataset.type; });
  }

  function score(it, terms) {
    var name = it.n.toLowerCase();
    var hay = (name + " " + it.d + " " + it.g.join(" ") + " " + it.p).toLowerCase();
    var s = 0;
    for (var i = 0; i < terms.length; i++) {
      var t = terms[i];
      if (hay.indexOf(t) === -1) return -1;
      if (name === t) s += 100;
      else if (name.indexOf(t) === 0) s += 50;
      else if (name.indexOf(t) !== -1) s += 20;
      else s += 1;
    }
    return s;
  }

  function render() {
    var terms = input.value.toLowerCase().split(/\s+/).filter(Boolean);
    var types = activeTypes();
    var hits = [];
    for (var i = 0; i < items.length; i++) {
      var it = items[i];
      if (types.length && types.indexOf(it.t) === -1) continue;
      var s = terms.length ? score(it, terms) : 0;
      if (s >= 0) hits.push([s, it]);
    }
    if (terms.length) {
      hits.sort(function (a, b) { return b[0] - a[0] || a[1].n.localeCompare(b[1].n); });
    }
    var html = "";
    for (var j = 0; j < hits.length && j < LIMIT; j++) {
      var h = hits[j][1];
      html += '<li><a href="nodes/' + encodeURIComponent(h.u) + '.html"><span class="badge t-' +
        esc(h.t) + '">' + esc(h.t) + "</span> " + esc(h.n) +
        (h.d ? '<span class="d">' + esc(h.d) + "</span>" : "") + "</a></li>";
    }
    list.innerHTML = html || '<li class="muted">No matches</li>';
    if (more) {
      more.hidden = hits.length <= LIMIT;
      more.textContent = "Showing " + LIMIT + " of " + hits.length +
        " matches. Refine the search to see more.";
    }
  }

  chips.forEach(function (c) {
    c.addEventListener("click", function () {
      var on = c.getAttribute("aria-pressed") === "true";
      c.setAttribute("aria-pressed", on ? "false" : "true");
      render();
    });
  });
  input.addEventListener("input", render);
  document.addEventListener("keydown", function (e) {
    if (e.key === "/" && document.activeElement !== input) {
      e.preventDefault();
      input.focus();
    }
  });
  render();
})();

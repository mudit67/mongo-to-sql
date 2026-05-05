import {
  buildSelect,
  buildInsert,
  buildUpdate,
  buildDelete,
} from "./mongoToSqlite.js";
import { buildAggregate } from "./pipelineToSqlite.js";

const $ = (id) => document.getElementById(id);

const els = {
  tableName: $("tableName"),
  schemaHint: $("schemaHint"),
  tabs: document.querySelectorAll(".tab"),
  panels: {
    select: $("panel-select"),
    insert: $("panel-insert"),
    update: $("panel-update"),
    delete: $("panel-delete"),
    aggregate: $("panel-aggregate"),
  },
  selectFilter: $("selectFilter"),
  selectProjection: $("selectProjection"),
  selectSort: $("selectSort"),
  selectLimit: $("selectLimit"),
  selectSkip: $("selectSkip"),
  insertDocs: $("insertDocs"),
  insertInferColumns: $("insertInferColumns"),
  updateFilter: $("updateFilter"),
  updateBody: $("updateBody"),
  updateAllowEmptyFilter: $("updateAllowEmptyFilter"),
  deleteFilter: $("deleteFilter"),
  deleteAllowEmptyFilter: $("deleteAllowEmptyFilter"),
  aggregatePipeline: $("aggregatePipeline"),
  btnGenerate: $("btnGenerate"),
  btnRunSql: $("btnRunSql"),
  btnCopy: $("btnCopy"),
  btnDownload: $("btnDownload"),
  message: $("message"),
  sqlOut: $("sqlOut"),
  queryResult: $("queryResult"),
};

const sampleButtonContainers = {
  select: $("sampleButtonsSelect"),
  insert: $("sampleButtonsInsert"),
  update: $("sampleButtonsUpdate"),
  delete: $("sampleButtonsDelete"),
  aggregate: $("sampleButtonsAggregate"),
};

function prettyJson(obj) {
  return JSON.stringify(obj, null, 2);
}

/**
 * @param {unknown} data
 */
function wireSampleButtons(data) {
  if (!data || typeof data !== "object") return;
  /** @type {("select"|"insert"|"update"|"delete"|"aggregate")[]} */
  const ops = ["select", "insert", "update", "delete", "aggregate"];
  for (const op of ops) {
    const container = sampleButtonContainers[op];
    if (!container) continue;
    const list = /** @type {unknown} */ (data)[op];
    if (!Array.isArray(list) || list.length === 0) {
      container.replaceChildren();
      continue;
    }
    container.replaceChildren();
    list.forEach((item, i) => {
      if (!item || typeof item !== "object") return;
      const sample = /** @type {Record<string, unknown>} */ (item);
      const n = i + 1;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "btn";
      btn.textContent = `Load sample ${n}`;
      const label = sample.label;
      if (typeof label === "string" && label) btn.title = label;
      btn.addEventListener("click", () => {
        const table = sample.table;
        if (typeof table === "string") els.tableName.value = table;

        if (op === "select") {
          if ("schemaHint" in sample)
            els.schemaHint.value =
              typeof sample.schemaHint === "string" ? sample.schemaHint : "";
          if ("filter" in sample)
            els.selectFilter.value = prettyJson(sample.filter);
          if ("projection" in sample)
            els.selectProjection.value = prettyJson(sample.projection);
          else els.selectProjection.value = "{}";
          if ("sort" in sample) els.selectSort.value = prettyJson(sample.sort);
          else els.selectSort.value = "{}";
          const lim = sample.limit;
          els.selectLimit.value =
            lim != null && lim !== "" ? String(/** @type {number|string} */ (lim)) : "";
          const sk = sample.skip;
          els.selectSkip.value =
            sk != null && sk !== "" ? String(/** @type {number|string} */ (sk)) : "";
        } else if (op === "insert") {
          if ("docs" in sample)
            els.insertDocs.value = prettyJson(sample.docs);
          if ("inferColumns" in sample)
            els.insertInferColumns.checked = Boolean(sample.inferColumns);
        } else if (op === "update") {
          if ("filter" in sample)
            els.updateFilter.value = prettyJson(sample.filter);
          if ("update" in sample)
            els.updateBody.value = prettyJson(sample.update);
          if ("allowEmptyFilter" in sample)
            els.updateAllowEmptyFilter.checked = Boolean(
              sample.allowEmptyFilter,
            );
        } else if (op === "delete") {
          if ("filter" in sample)
            els.deleteFilter.value = prettyJson(sample.filter);
          if ("allowEmptyFilter" in sample)
            els.deleteAllowEmptyFilter.checked = Boolean(
              sample.allowEmptyFilter,
            );
        } else if (op === "aggregate") {
          if ("pipeline" in sample)
            els.aggregatePipeline.value = prettyJson(sample.pipeline);
        }

        setTab(op);
        setMessage("", "");
      });
      container.appendChild(btn);
    });
  }
}

fetch(new URL("sample.json", import.meta.url).href)
  .then((r) => {
    if (!r.ok) throw new Error(String(r.status));
    return r.json();
  })
  .then(wireSampleButtons)
  .catch(() => {
    console.warn("Could not load sample.json (serve the app over HTTP if using a file URL).");
  });

/** @type {"select"|"insert"|"update"|"delete"|"aggregate"} */
let activeTab = "select";

function setTab(name) {
  activeTab = name;
  els.tabs.forEach((btn) => {
    const on = btn.dataset.tab === name;
    btn.classList.toggle("active", on);
    btn.setAttribute("aria-selected", on ? "true" : "false");
  });
  for (const [key, panel] of Object.entries(els.panels)) {
    const on = key === name;
    panel.hidden = !on;
    panel.classList.toggle("active", on);
  }
}

els.tabs.forEach((btn) => {
  btn.addEventListener("click", () => {
    const t = /** @type {"select"|"insert"|"update"|"delete"|"aggregate"} */ (btn.dataset.tab);
    if (t) setTab(t);
  });
});

function setMessage(text, kind) {
  els.message.textContent = text;
  els.message.className = "message" + (kind ? ` ${kind}` : "");
}

function setQueryResult(value) {
  els.queryResult.textContent = value || "";
}

function currentOperation() {
  return activeTab;
}

function generate() {
  const table = els.tableName.value;
  const schemaHint = els.schemaHint.value.trim() || null;
  const op = currentOperation();

  setMessage("", "");

  /** @type {import("./mongoToSqlite.js").SqlResult | undefined} */
  let result;

  try {
    if (op === "select") {
      result = buildSelect({
        table,
        filter: els.selectFilter.value,
        projection: els.selectProjection.value,
        sort: els.selectSort.value,
        limit: els.selectLimit.value,
        skip: els.selectSkip.value,
        schemaHint,
      });
    } else if (op === "insert") {
      result = buildInsert({
        table,
        docs: els.insertDocs.value,
        inferColumns: els.insertInferColumns.checked,
      });
    } else if (op === "update") {
      result = buildUpdate({
        table,
        filter: els.updateFilter.value,
        update: els.updateBody.value,
        allowEmptyFilter: els.updateAllowEmptyFilter.checked,
      });
    } else if (op === "delete") {
      result = buildDelete({
        table,
        filter: els.deleteFilter.value,
        allowEmptyFilter: els.deleteAllowEmptyFilter.checked,
      });
    } else {
      result = buildAggregate({
        table,
        pipeline: els.aggregatePipeline.value,
      });
    }
  } catch (e) {
    const msg = /** @type {Error} */ (e).message || String(e);
    setMessage(msg, "error");
    els.sqlOut.textContent = "";
    setQueryResult("");
    return;
  }

  if (!result) return;

  if (result.error) {
    setMessage(result.error, "error");
    els.sqlOut.textContent = "";
    setQueryResult("");
    return;
  }

  const warnText =
    result.warnings.length > 0
      ? `Warnings (${result.warnings.length}): ${result.warnings.join(" ")}`
      : "OK.";
  setMessage(warnText, result.warnings.length ? "warn" : "ok");
  els.sqlOut.textContent = result.sql;
  setQueryResult("");
}

els.btnGenerate.addEventListener("click", generate);

els.btnRunSql?.addEventListener("click", async () => {
  const sql = (els.sqlOut.textContent || "").trim();
  if (!sql) {
    setMessage("Generate SQL first.", "warn");
    return;
  }

  setMessage("Running SQL on dummy SQLite DB...", "");
  setQueryResult("");

  try {
    const response = await fetch("/api/run-sql", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || `Request failed (${response.status})`);
    }

    if (data.mode === "query") {
      setQueryResult(JSON.stringify(data.rows, null, 2));
      setMessage(`Query completed. ${data.rowCount} row(s) returned.`, "ok");
      return;
    }

    const runPayload = {
      changes: data.changes,
      lastInsertRowid: data.lastInsertRowid,
    };
    setQueryResult(JSON.stringify(runPayload, null, 2));
    setMessage(`Statement executed. ${data.changes} row(s) changed.`, "ok");
  } catch (error) {
    setMessage(
      `SQL execution failed: ${error instanceof Error ? error.message : String(error)}`,
      "error",
    );
    setQueryResult("");
  }
});

els.btnCopy?.addEventListener("click", async () => {
  const text = els.sqlOut.textContent || "";
  if (!text) {
    setMessage("Nothing to copy.", "warn");
    return;
  }
  try {
    await navigator.clipboard.writeText(text);
    setMessage("Copied to clipboard.", "ok");
  } catch {
    setMessage("Clipboard failed. Select the output manually.", "error");
  }
});

els.btnDownload?.addEventListener("click", () => {
  const text = els.sqlOut.textContent || "";
  if (!text) {
    setMessage("Nothing to download.", "warn");
    return;
  }
  const blob = new Blob([text + "\n"], { type: "text/plain;charset=utf-8" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "query.sql";
  a.click();
  URL.revokeObjectURL(a.href);
  setMessage("Download started.", "ok");
});

setTab("select");

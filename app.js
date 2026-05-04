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
  btnCopy: $("btnCopy"),
  btnDownload: $("btnDownload"),
  message: $("message"),
  sqlOut: $("sqlOut"),
};

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
    return;
  }

  if (!result) return;

  if (result.error) {
    setMessage(result.error, "error");
    els.sqlOut.textContent = "";
    return;
  }

  const warnText =
    result.warnings.length > 0
      ? `Warnings (${result.warnings.length}): ${result.warnings.join(" ")}`
      : "OK.";
  setMessage(warnText, result.warnings.length ? "warn" : "ok");
  els.sqlOut.textContent = result.sql;
}

els.btnGenerate.addEventListener("click", generate);

els.btnCopy.addEventListener("click", async () => {
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

els.btnDownload.addEventListener("click", () => {
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

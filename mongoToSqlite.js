/**
 * Mongo-style JSON → SQLite SQL generator (browser-safe, no deps).
 */

/** @typedef {{ sql: string, warnings: string[], error?: string }} SqlResult */

const IDENT_SAFE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * @param {string} name
 * @returns {string}
 */
export function quoteIdentifier(name) {
  if (typeof name !== "string") name = String(name);
  if (IDENT_SAFE.test(name)) return name;
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * @param {string} s
 * @returns {string}
 */
export function escapeSqlString(s) {
  return `'${String(s).replace(/'/g, "''")}'`;
}

/**
 * @param {unknown} v
 * @param {string[]} warnings
 * @returns {string}
 */
export function emitLiteral(v, warnings) {
  if (v === null || v === undefined) return "NULL";
  if (typeof v === "boolean") return v ? "1" : "0";
  if (typeof v === "number") {
    if (!Number.isFinite(v)) {
      warnings.push("Non-finite number coerced to NULL.");
      return "NULL";
    }
    return String(v);
  }
  if (typeof v === "string") return escapeSqlString(v);
  if (v instanceof Date) return escapeSqlString(v.toISOString());
  if (typeof v === "bigint") return String(v);
  if (Array.isArray(v)) {
    warnings.push("Array value in literal context stringified.");
    return escapeSqlString(JSON.stringify(v));
  }
  if (typeof v === "object") {
    warnings.push("Object value in literal context stringified.");
    return escapeSqlString(JSON.stringify(v));
  }
  warnings.push(`Unsupported literal type stringified: ${typeof v}.`);
  return escapeSqlString(String(v));
}

/**
 * @param {unknown} value
 * @returns {boolean}
 */
function isPlainObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value) && !(value instanceof Date);
}

/**
 * @param {unknown} spec
 * @returns {boolean}
 */
function isOperatorObject(spec) {
  if (!isPlainObject(spec)) return false;
  const keys = Object.keys(spec);
  return keys.length > 0 && keys.every((k) => k.startsWith("$"));
}

/**
 * @param {string} field
 * @param {unknown} value
 * @param {string[]} warnings
 * @param {string | null} [tableAlias] safe SQL alias (e.g. base, j1); unquoted
 * @returns {string}
 */
function compileFieldCondition(field, value, warnings, tableAlias = null) {
  const col = tableAlias ? `${tableAlias}.${quoteIdentifier(field)}` : quoteIdentifier(field);

  if (value === null) return `${col} IS NULL`;

  if (!isPlainObject(value) || Array.isArray(value)) {
    if (typeof value === "object" && value !== null && !Array.isArray(value) && !(value instanceof Date)) {
      warnings.push(`Field "${field}": nested object match not supported; use operators or flat columns.`);
      return `${col} = ${emitLiteral(JSON.stringify(value), warnings)}`;
    }
    if (typeof value === "undefined") {
      warnings.push(`Field "${field}": undefined treated as NULL check.`);
      return `${col} IS NULL`;
    }
    return `${col} = ${emitLiteral(value, warnings)}`;
  }

  if (!isOperatorObject(value)) {
    warnings.push(`Field "${field}": subdocument equality not fully supported; comparing JSON text.`);
    return `${col} = ${emitLiteral(JSON.stringify(value), warnings)}`;
  }

  const parts = [];
  for (const [op, arg] of Object.entries(value)) {
    switch (op) {
      case "$eq":
        if (arg === null) parts.push(`${col} IS NULL`);
        else parts.push(`${col} = ${emitLiteral(arg, warnings)}`);
        break;
      case "$ne":
        if (arg === null) parts.push(`${col} IS NOT NULL`);
        else parts.push(`${col} != ${emitLiteral(arg, warnings)}`);
        break;
      case "$gt":
        parts.push(`${col} > ${emitLiteral(arg, warnings)}`);
        break;
      case "$gte":
        parts.push(`${col} >= ${emitLiteral(arg, warnings)}`);
        break;
      case "$lt":
        parts.push(`${col} < ${emitLiteral(arg, warnings)}`);
        break;
      case "$lte":
        parts.push(`${col} <= ${emitLiteral(arg, warnings)}`);
        break;
      case "$in": {
        if (!Array.isArray(arg) || arg.length === 0) {
          warnings.push(`$in on "${field}" empty or invalid → FALSE.`);
          parts.push("0");
        } else {
          const list = arg.map((x) => emitLiteral(x, warnings)).join(", ");
          parts.push(`${col} IN (${list})`);
        }
        break;
      }
      case "$nin": {
        if (!Array.isArray(arg) || arg.length === 0) {
          parts.push("1");
        } else {
          const list = arg.map((x) => emitLiteral(x, warnings)).join(", ");
          parts.push(`${col} NOT IN (${list})`);
        }
        break;
      }
      case "$exists":
        parts.push(arg ? `${col} IS NOT NULL` : `${col} IS NULL`);
        break;
      case "$regex": {
        const opts = typeof value.$options === "string" ? value.$options : "";
        const like = regexToLike(String(arg), opts, warnings);
        parts.push(`${col} LIKE ${escapeSqlString(like)} ESCAPE '\\'`);
        break;
      }
      case "$options":
        break;
      case "$not": {
        const inner = compileFieldCondition(field, arg, warnings, tableAlias);
        parts.push(`NOT (${inner})`);
        break;
      }
      default:
        warnings.push(`Unsupported operator "${op}" on field "${field}"; ignored.`);
    }
  }
  if (parts.length === 0) return "1";
  return parts.length === 1 ? parts[0] : `(${parts.join(" AND ")})`;
}

/**
 * @param {string} pattern
 * @param {string} options
 * @param {string[]} warnings
 * @returns {string}
 */
function regexToLike(pattern, options, warnings) {
  warnings.push("$regex mapped to LIKE (heuristic; not PCRE).");
  if (!options.includes("i")) {
    warnings.push("$regex: case-sensitive LIKE may differ from Mongo regex.");
  }
  if (/[\[\]()^$+|?{}\\]/.test(pattern.replace(/\.\*/g, ""))) {
    warnings.push("$regex: pattern contains regex metacharacters; LIKE result may be wrong.");
  }
  let p = pattern.replace(/\.\*/g, "\u0000DS\u0000").replace(/\./g, "_").replace(/\u0000DS\u0000/g, "%");
  p = p.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
  return p;
}

/**
 * @param {Record<string, unknown>} filter
 * @param {string[]} warnings
 * @param {string | null} [tableAlias] qualify top-level field refs (e.g. base)
 * @returns {string}
 */
export function filterToWhere(filter, warnings, tableAlias = null) {
  if (filter === null || typeof filter === "undefined") {
    warnings.push("Filter was null/undefined; using TRUE.");
    return "1";
  }
  if (typeof filter !== "object" || Array.isArray(filter)) {
    return /** @type {any} */ (filter) ? "1" : "0";
  }
  const keys = Object.keys(filter);
  if (keys.length === 0) return "1";

  const parts = [];
  for (const key of keys) {
    if (key === "$and") {
      const arr = filter.$and;
      if (!Array.isArray(arr) || arr.length === 0) {
        warnings.push("$and empty or invalid.");
        parts.push("1");
        continue;
      }
      const subs = arr.map((sub) => `(${filterToWhere(/** @type {Record<string, unknown>} */ (sub), warnings, tableAlias)})`);
      parts.push(`(${subs.join(" AND ")})`);
      continue;
    }
    if (key === "$or") {
      const arr = filter.$or;
      if (!Array.isArray(arr) || arr.length === 0) {
        warnings.push("$or empty or invalid.");
        parts.push("0");
        continue;
      }
      const subs = arr.map((sub) => `(${filterToWhere(/** @type {Record<string, unknown>} */ (sub), warnings, tableAlias)})`);
      parts.push(`(${subs.join(" OR ")})`);
      continue;
    }
    if (key === "$nor") {
      const arr = filter.$nor;
      if (!Array.isArray(arr) || arr.length === 0) {
        parts.push("1");
        continue;
      }
      const subs = arr.map((sub) => `(${filterToWhere(/** @type {Record<string, unknown>} */ (sub), warnings, tableAlias)})`);
      parts.push(`NOT (${subs.join(" OR ")})`);
      continue;
    }
    if (key === "$not") {
      parts.push(`NOT (${filterToWhere(/** @type {Record<string, unknown>} */ (filter.$not), warnings, tableAlias)})`);
      continue;
    }
    parts.push(compileFieldCondition(key, filter[key], warnings, tableAlias));
  }
  if (parts.length === 0) return "1";
  return parts.join(" AND ");
}

/**
 * @param {unknown} projection
 * @param {string[] | null} schemaColumns
 * @param {string[]} warnings
 * @returns {string}
 */
export function projectionToSelect(projection, schemaColumns, warnings) {
  if (projection === null || projection === undefined || projection === "") {
    return "*";
  }
  if (Array.isArray(projection)) {
    if (projection.length === 0) return "*";
    return projection.map((c) => quoteIdentifier(String(c))).join(", ");
  }
  if (!isPlainObject(projection)) {
    warnings.push("Projection not object/array; using SELECT *.");
    return "*";
  }
  const entries = Object.entries(projection);
  if (entries.length === 0) return "*";

  const inclusion = entries.filter(([, v]) => v === 1 || v === true);
  const exclusion = entries.filter(([, v]) => v === 0 || v === false);

  if (inclusion.length && exclusion.length) {
    warnings.push("Projection mixes inclusion and exclusion; using inclusion only.");
  }

  if (inclusion.length > 0) {
    return inclusion.map(([k]) => quoteIdentifier(k)).join(", ");
  }

  if (exclusion.length > 0) {
    if (!schemaColumns || schemaColumns.length === 0) {
      warnings.push("Exclusion projection needs schema hint; using SELECT *.");
      return "*";
    }
    const omit = new Set(exclusion.map(([k]) => k));
    const cols = schemaColumns.filter((c) => !omit.has(c));
    if (cols.length === 0) {
      warnings.push("All columns excluded; using SELECT *.");
      return "*";
    }
    return cols.map((c) => quoteIdentifier(c)).join(", ");
  }

  warnings.push("Projection had no 0/1 flags; using SELECT *.");
  return "*";
}

/**
 * @param {unknown} sort
 * @param {string[]} warnings
 * @returns {string}
 */
export function sortToOrderBy(sort, warnings) {
  if (sort === null || sort === undefined) return "";
  if (!isPlainObject(sort) || Object.keys(sort).length === 0) return "";
  const parts = [];
  for (const [field, dir] of Object.entries(sort)) {
    const ord = dir === -1 || dir === "-1" ? "DESC" : "ASC";
    parts.push(`${quoteIdentifier(field)} ${ord}`);
  }
  if (parts.length === 0) {
    warnings.push("Sort object empty; omitting ORDER BY.");
    return "";
  }
  return parts.join(", ");
}

/**
 * @param {string} raw
 * @returns {string[] | null}
 */
export function parseSchemaHint(raw) {
  if (!raw || typeof raw !== "string") return null;
  const cols = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  return cols.length ? cols : null;
}

/**
 * @param {{ table: string, filter: unknown, projection: unknown, sort: unknown, limit?: string|number, skip?: string|number, schemaHint?: string | null }} opts
 * @returns {SqlResult}
 */
export function buildSelect(opts) {
  const warnings = [];
  const table = normalizeTable(opts.table, warnings);
  if (!table) return { sql: "", warnings, error: "Invalid or empty table name." };

  let filterObj = {};
  try {
    filterObj = parseJsonInput(opts.filter, "filter");
  } catch (e) {
    return { sql: "", warnings, error: /** @type {Error} */ (e).message };
  }

  let projection = opts.projection;
  try {
    if (typeof projection === "string" && projection.trim()) projection = JSON.parse(projection);
  } catch (e) {
    return { sql: "", warnings, error: `Projection JSON: ${/** @type {Error} */ (e).message}` };
  }

  let sort = opts.sort;
  try {
    if (typeof sort === "string" && sort.trim()) sort = JSON.parse(sort);
    else if (typeof sort === "string") sort = null;
  } catch (e) {
    return { sql: "", warnings, error: `Sort JSON: ${/** @type {Error} */ (e).message}` };
  }

  const schemaCols = parseSchemaHint(opts.schemaHint ?? "");
  const selectList = projectionToSelect(projection, schemaCols, warnings);
  const where = filterToWhere(filterObj, warnings);
  const orderBy = sortToOrderBy(sort, warnings);

  let sql = `SELECT ${selectList} FROM ${table} WHERE ${where}`;
  if (orderBy) sql += ` ORDER BY ${orderBy}`;

  const lim = parseOptionalInt(opts.limit);
  const off = parseOptionalInt(opts.skip);
  if (lim !== null) {
    sql += ` LIMIT ${lim}`;
    if (off !== null && off > 0) sql += ` OFFSET ${off}`;
  } else if (off !== null && off > 0) {
    warnings.push("OFFSET without LIMIT is invalid in SQLite; adding LIMIT -1.");
    sql += ` LIMIT -1 OFFSET ${off}`;
  }

  return { sql: sql + ";", warnings };
}

/**
 * @param {unknown} n
 * @returns {number | null}
 */
function parseOptionalInt(n) {
  if (n === null || n === undefined || n === "") return null;
  const v = typeof n === "number" ? n : parseInt(String(n), 10);
  if (!Number.isFinite(v) || v < 0) return null;
  return v;
}

/**
 * @param {unknown} input
 * @param {string} label
 * @returns {Record<string, unknown>}
 */
function parseJsonInput(input, label) {
  if (input === null || input === undefined || input === "") return {};
  if (typeof input === "object" && !Array.isArray(input)) return /** @type {Record<string, unknown>} */ (input);
  if (typeof input !== "string") throw new Error(`${label}: expected object or JSON string.`);
  try {
    const v = JSON.parse(input);
    if (typeof v !== "object" || v === null || Array.isArray(v)) throw new Error("must be a JSON object.");
    return /** @type {Record<string, unknown>} */ (v);
  } catch (e) {
    throw new Error(`${label}: ${/** @type {Error} */ (e).message}`);
  }
}

/**
 * @param {string} name
 * @param {string[]} warnings
 * @returns {string}
 */
export function normalizeTable(name, warnings) {
  if (typeof name !== "string" || !name.trim()) return "";
  const t = name.trim();
  if (/[\s;'"\\]/.test(t)) {
    warnings.push("Table name contained unusual characters; quoting as identifier.");
  }
  return quoteIdentifier(t);
}

/**
 * @param {{ table: string, docs: unknown, inferColumns?: boolean }} opts
 * @returns {SqlResult}
 */
export function buildInsert(opts) {
  const warnings = [];
  const table = normalizeTable(opts.table, warnings);
  if (!table) return { sql: "", warnings, error: "Invalid or empty table name." };

  let docs;
  try {
    const raw = opts.docs;
    if (typeof raw === "string") docs = JSON.parse(raw);
    else docs = raw;
  } catch (e) {
    return { sql: "", warnings, error: `Documents JSON: ${/** @type {Error} */ (e).message}` };
  }

  const rows = Array.isArray(docs) ? docs : [docs];
  if (rows.length === 0) return { sql: "", warnings, error: "No documents to insert." };
  for (const r of rows) {
    if (!isPlainObject(r)) return { sql: "", warnings, error: "Each document must be a JSON object." };
  }

  /** @type {string[]} */
  let columns;
  if (opts.inferColumns !== false) {
    columns = Object.keys(/** @type {Record<string, unknown>} */ (rows[0]));
  } else {
    const all = new Set();
    for (const r of rows) {
      Object.keys(/** @type {Record<string, unknown>} */ (r)).forEach((k) => all.add(k));
    }
    columns = Array.from(all);
  }

  if (columns.length === 0) return { sql: "", warnings, error: "No columns inferred from documents." };

  const colSql = columns.map((c) => quoteIdentifier(c)).join(", ");
  const valueRows = rows.map((row) => {
    const obj = /** @type {Record<string, unknown>} */ (row);
    const vals = columns.map((c) => {
      if (!Object.prototype.hasOwnProperty.call(obj, c)) return "NULL";
      const v = obj[c];
      if (v !== null && typeof v === "object" && !Array.isArray(v) && !(v instanceof Date)) {
        warnings.push(`Column "${c}": nested object stringified.`);
        return emitLiteral(JSON.stringify(v), warnings);
      }
      if (Array.isArray(v)) {
        warnings.push(`Column "${c}": array stringified.`);
        return emitLiteral(JSON.stringify(v), warnings);
      }
      return emitLiteral(v, warnings);
    });
    return `(${vals.join(", ")})`;
  });

  const sql = `INSERT INTO ${table} (${colSql}) VALUES\n  ${valueRows.join(",\n  ")};`;
  return { sql, warnings };
}

/**
 * @param {{ table: string, filter: unknown, update: unknown, allowEmptyFilter?: boolean }} opts
 * @returns {SqlResult}
 */
export function buildUpdate(opts) {
  const warnings = [];
  const table = normalizeTable(opts.table, warnings);
  if (!table) return { sql: "", warnings, error: "Invalid or empty table name." };

  let filterObj;
  try {
    filterObj = parseJsonInput(opts.filter, "filter");
  } catch (e) {
    return { sql: "", warnings, error: /** @type {Error} */ (e).message };
  }

  let updateObj;
  try {
    const u = opts.update;
    if (typeof u === "string") updateObj = JSON.parse(u);
    else updateObj = u;
  } catch (e) {
    return { sql: "", warnings, error: `Update JSON: ${/** @type {Error} */ (e).message}` };
  }

  if (!isPlainObject(updateObj)) return { sql: "", warnings, error: "Update must be a JSON object." };

  const updKeys = Object.keys(updateObj);
  const hasDollar = updKeys.some((k) => k.startsWith("$"));
  const hasNonDollar = updKeys.some((k) => !k.startsWith("$"));
  if (hasDollar && hasNonDollar) {
    return { sql: "", warnings, error: "Update cannot mix $operators and replacement fields in one object." };
  }

  const where = filterToWhere(filterObj, warnings);
  const isEmptyFilter = Object.keys(filterObj).length === 0;
  if (isEmptyFilter && !opts.allowEmptyFilter) {
    return { sql: "", warnings, error: "Empty filter would update all rows. Check “Allow empty filter” or add criteria." };
  }

  /** @type {string[]} */
  const sets = [];

  if (isOperatorObject(updateObj) && (Object.prototype.hasOwnProperty.call(updateObj, "$set") || Object.prototype.hasOwnProperty.call(updateObj, "$unset"))) {
    const $set = updateObj.$set;
    if ($set !== undefined) {
      if (!isPlainObject($set)) return { sql: "", warnings, error: "$set must be an object." };
      for (const [k, v] of Object.entries($set)) {
        sets.push(`${quoteIdentifier(k)} = ${emitLiteral(v, warnings)}`);
      }
    }
    const $unset = updateObj.$unset;
    if ($unset !== undefined) {
      if (!isPlainObject($unset)) return { sql: "", warnings, error: "$unset must be an object." };
      for (const k of Object.keys($unset)) {
        sets.push(`${quoteIdentifier(k)} = NULL`);
      }
    }
    for (const k of Object.keys(updateObj)) {
      if (k !== "$set" && k !== "$unset") {
        warnings.push(`Update operator "${k}" not supported in v1; ignored.`);
      }
    }
  } else {
    for (const [k, v] of Object.entries(updateObj)) {
      sets.push(`${quoteIdentifier(k)} = ${emitLiteral(v, warnings)}`);
    }
  }

  if (sets.length === 0) return { sql: "", warnings, error: "No SET clauses produced (empty $set / replacement?)." };

  const sql = `UPDATE ${table} SET ${sets.join(", ")} WHERE ${where};`;
  return { sql, warnings };
}

/**
 * @param {{ table: string, filter: unknown, allowEmptyFilter?: boolean }} opts
 * @returns {SqlResult}
 */
export function buildDelete(opts) {
  const warnings = [];
  const table = normalizeTable(opts.table, warnings);
  if (!table) return { sql: "", warnings, error: "Invalid or empty table name." };

  let filterObj;
  try {
    filterObj = parseJsonInput(opts.filter, "filter");
  } catch (e) {
    return { sql: "", warnings, error: /** @type {Error} */ (e).message };
  }

  const where = filterToWhere(filterObj, warnings);
  const isEmptyFilter = Object.keys(filterObj).length === 0;
  if (isEmptyFilter && !opts.allowEmptyFilter) {
    return { sql: "", warnings, error: "Empty filter would delete all rows. Check “Allow empty filter” or add criteria." };
  }

  const sql = `DELETE FROM ${table} WHERE ${where};`;
  return { sql, warnings };
}

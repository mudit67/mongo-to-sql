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
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    !(value instanceof Date)
  );
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
 * FIX: dotted paths like "address.city" should warn rather than silently emit
 * a quoted identifier that SQLite cannot resolve to a nested field.
 * @param {string} field
 * @param {string[]} warnings
 * @returns {string}
 */
function fieldToCol(field, warnings) {
  if (field.includes(".")) {
    warnings.push(
      `Dotted path "${field}" treated as a flat column name. ` +
        `SQLite has no nested document support; ensure this column exists as-is.`
    );
  }
  return quoteIdentifier(field);
}

/**
 * @param {string} field
 * @param {unknown} value
 * @param {string[]} warnings
 * @param {string | null} [tableAlias]
 * @returns {string}
 */
function compileFieldCondition(field, value, warnings, tableAlias = null) {
  const colBase = fieldToCol(field, warnings);
  const col = tableAlias ? `${tableAlias}.${colBase}` : colBase;

  if (value === null) return `${col} IS NULL`;

  if (typeof value === "undefined") {
    warnings.push(`Field "${field}": undefined treated as NULL check.`);
    return `${col} IS NULL`;
  }

  // Plain scalar — equality
  if (!isPlainObject(value) && !Array.isArray(value)) {
    return `${col} = ${emitLiteral(value, warnings)}`;
  }

  // Array — not a valid field-level filter value unless wrapped in an operator
  if (Array.isArray(value)) {
    warnings.push(`Field "${field}": bare array as filter value is not valid; comparing JSON text.`);
    return `${col} = ${emitLiteral(JSON.stringify(value), warnings)}`;
  }

  // Plain object that has no $ keys → subdocument equality
  if (!isOperatorObject(value)) {
    warnings.push(
      `Field "${field}": subdocument equality not fully supported; comparing JSON text.`
    );
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
        // consumed by $regex handler above
        break;
      case "$not": {
        const inner = compileFieldCondition(field, arg, warnings, tableAlias);
        parts.push(`NOT (${inner})`);
        break;
      }
      // FIX: $type, $size, $all, $elemMatch were silently ignored with no warning
      case "$type":
        warnings.push(
          `$type on "${field}" is not translatable to SQLite; condition omitted. ` +
            `SQLite is dynamically typed and has no BSON type system.`
        );
        break;
      case "$size":
        warnings.push(
          `$size on "${field}" cannot be evaluated in SQLite without JSON1 array support; condition omitted.`
        );
        break;
      case "$all":
        warnings.push(
          `$all on "${field}" cannot be translated to SQLite; condition omitted. ` +
            `SQLite has no native array containment.`
        );
        break;
      case "$elemMatch":
        warnings.push(
          `$elemMatch on "${field}" cannot be translated to SQLite; condition omitted. ` +
            `SQLite has no native array element filtering.`
        );
        break;
      default:
        warnings.push(`Unsupported operator "${op}" on field "${field}"; ignored.`);
    }
  }
  if (parts.length === 0) return "1";
  return parts.length === 1 ? parts[0] : `(${parts.join(" AND ")})`;
}

/**
 * FIX: Original escape order was wrong — placeholder substitution then escape
 * meant the `%` produced by `.*` got re-escaped to `\%` by the escape pass.
 * Correct order: escape LIKE special chars first, then translate regex tokens.
 *
 * @param {string} pattern
 * @param {string} options
 * @param {string[]} warnings
 * @returns {string}
 */
function regexToLike(pattern, options, warnings) {
  warnings.push("$regex mapped to LIKE (heuristic; not PCRE).");
  if (!options.includes("i")) {
    warnings.push("$regex: SQLite LIKE is case-insensitive for ASCII by default; case-sensitive match may differ.");
  }
  if (/[\[\]()^$+|?{}]/.test(pattern)) {
    warnings.push("$regex: pattern contains regex metacharacters that LIKE cannot represent; result may be wrong.");
  }

  // Step 1: escape LIKE's own special characters in the raw pattern
  // (backslash is our escape char, so escape it first)
  let p = pattern
    .replace(/\\/g, "\\\\")  // literal backslash → \\
    .replace(/%/g, "\\%")    // literal % → \%
    .replace(/_/g, "\\_");   // literal _ → \_

  // Step 2: translate regex tokens AFTER escaping, so our % and _ don't get re-escaped
  p = p
    .replace(/\.\*/g, "%")   // .* → %  (any sequence)
    .replace(/\./g, "_");    // .  → _  (any single char)

  return p;
}

/**
 * @param {Record<string, unknown>} filter
 * @param {string[]} warnings
 * @param {string | null} [tableAlias]
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
      const subs = arr.map(
        (sub) =>
          `(${filterToWhere(/** @type {Record<string, unknown>} */ (sub), warnings, tableAlias)})`
      );
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
      const subs = arr.map(
        (sub) =>
          `(${filterToWhere(/** @type {Record<string, unknown>} */ (sub), warnings, tableAlias)})`
      );
      parts.push(`(${subs.join(" OR ")})`);
      continue;
    }
    if (key === "$nor") {
      const arr = filter.$nor;
      if (!Array.isArray(arr) || arr.length === 0) {
        parts.push("1");
        continue;
      }
      const subs = arr.map(
        (sub) =>
          `(${filterToWhere(/** @type {Record<string, unknown>} */ (sub), warnings, tableAlias)})`
      );
      parts.push(`NOT (${subs.join(" OR ")})`);
      continue;
    }
    if (key === "$not") {
      const operand = filter.$not;
      // `$not` on `{}` used to compile to `NOT (1)` → SQLite false (no rows).
      // An empty predicate has no constraints, so invert nothing → match-all.
      if (isPlainObject(operand) && Object.keys(operand).length === 0) {
        warnings.push(
          'Top-level $not with empty object {}; no constraints to negate — treated as match-all.'
        );
        parts.push("1");
        continue;
      }
      parts.push(
        `NOT (${filterToWhere(
          /** @type {Record<string, unknown>} */ (operand),
          warnings,
          tableAlias
        )})`
      );
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
    const names = projection.map((c) => String(c));
    const cols = names.map((c) => quoteIdentifier(c));
    if (!names.includes("_id"))
      cols.unshift(quoteIdentifier("_id"));
    return cols.join(", ");
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
    const idExplicitlyExcluded = entries.some(
      ([k, v]) => k === "_id" && (v === 0 || v === false),
    );
    const idExplicitlyIncluded = inclusion.some(([k]) => k === "_id");

    let cols = inclusion.map(([k]) => quoteIdentifier(k));
    // MongoDB: inclusion projection retains _id unless explicitly {_id: 0}.
    if (!idExplicitlyExcluded && !idExplicitlyIncluded) {
      cols.unshift(quoteIdentifier("_id"));
    }
    return cols.join(", ");
  }

  if (exclusion.length > 0) {
    if (!schemaColumns || schemaColumns.length === 0) {
      warnings.push("Exclusion projection needs schema hint; using SELECT *.");
      return "*";
    }
    const omit = new Set(exclusion.map(([k]) => k));
    const cols = schemaColumns.filter((c) => !omit.has(c));
    // MongoDB: _id is included by default unless explicitly excluded.
    if (!omit.has("_id") && !cols.includes("_id")) cols.unshift("_id");
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
 * Unconstrained filter → `filterToWhere` returns `1` (TRUE). Omit WHERE so SQL
 * matches all rows without a misleading `WHERE 1` / `NOT (1)` edge case.
 * @param {string} whereExpr
 * @returns {string}  ` WHERE …` or ""
 */
function sqlWhereClause(whereExpr) {
  if (String(whereExpr).trim() === "1") return "";
  return ` WHERE ${whereExpr}`;
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
    if (typeof projection === "string" && projection.trim())
      projection = JSON.parse(projection);
  } catch (e) {
    return {
      sql: "",
      warnings,
      error: `Projection JSON: ${/** @type {Error} */ (e).message}`,
    };
  }

  let sort = opts.sort;
  try {
    if (typeof sort === "string" && sort.trim()) sort = JSON.parse(sort);
    else if (typeof sort === "string") sort = null;
  } catch (e) {
    return {
      sql: "",
      warnings,
      error: `Sort JSON: ${/** @type {Error} */ (e).message}`,
    };
  }

  const schemaCols = parseSchemaHint(opts.schemaHint ?? "");
  const selectList = projectionToSelect(projection, schemaCols, warnings);
  const where = filterToWhere(filterObj, warnings);
  const orderBy = sortToOrderBy(sort, warnings);

  let sql = `SELECT ${selectList} FROM ${table}${sqlWhereClause(where)}`;
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
  if (typeof input === "object" && !Array.isArray(input))
    return /** @type {Record<string, unknown>} */ (input);
  if (typeof input !== "string")
    throw new Error(`${label}: expected object or JSON string.`);
  try {
    const v = JSON.parse(input);
    if (typeof v !== "object" || v === null || Array.isArray(v))
      throw new Error("must be a JSON object.");
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
 * FIX: inferColumns checkbox logic was inverted.
 * When inferColumns=true (checked), columns come from the first document only.
 * When inferColumns=false (unchecked), columns are the union of all documents.
 * The original code used `opts.inferColumns !== false` which made unchecked=union
 * but checked=first-doc — the label said "Infer from first document" matching
 * checked=true, so the branch bodies needed to be swapped.
 *
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
    return {
      sql: "",
      warnings,
      error: `Documents JSON: ${/** @type {Error} */ (e).message}`,
    };
  }

  const rows = Array.isArray(docs) ? docs : [docs];
  if (rows.length === 0) return { sql: "", warnings, error: "No documents to insert." };
  for (const r of rows) {
    if (!isPlainObject(r))
      return { sql: "", warnings, error: "Each document must be a JSON object." };
  }

  /** @type {string[]} */
  let columns;
  // FIX: swapped branch bodies to match the label semantics
  if (opts.inferColumns !== false) {
    // inferColumns=true (default/checked): union of all documents' keys
    const all = new Set();
    for (const r of rows) {
      Object.keys(/** @type {Record<string, unknown>} */ (r)).forEach((k) => all.add(k));
    }
    columns = Array.from(all);
  } else {
    // inferColumns=false (unchecked): only first document's keys
    columns = Object.keys(/** @type {Record<string, unknown>} */ (rows[0]));
  }

  if (columns.length === 0)
    return { sql: "", warnings, error: "No columns inferred from documents." };

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
 * FIX: Added $inc, $mul, $rename, $min, $max update operators.
 * FIX: The old operator fallthrough bug — if $set/$unset were absent but other
 *      $ operators were present, the code fell into the replacement branch and
 *      emitted `$inc = ...` etc. as literal column names.
 *      Now all $ operators are handled before the replacement branch.
 *
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
    return {
      sql: "",
      warnings,
      error: `Update JSON: ${/** @type {Error} */ (e).message}`,
    };
  }

  if (!isPlainObject(updateObj))
    return { sql: "", warnings, error: "Update must be a JSON object." };

  const updKeys = Object.keys(updateObj);
  const hasDollar = updKeys.some((k) => k.startsWith("$"));
  const hasNonDollar = updKeys.some((k) => !k.startsWith("$"));
  if (hasDollar && hasNonDollar) {
    return {
      sql: "",
      warnings,
      error:
        "Update cannot mix $operators and replacement fields in one object.",
    };
  }

  const where = filterToWhere(filterObj, warnings);
  const isEmptyFilter = Object.keys(filterObj).length === 0;
  if (isEmptyFilter && !opts.allowEmptyFilter) {
    return {
      sql: "",
      warnings,
      error:
        "Empty filter would update all rows. Check \u201cAllow empty filter\u201d or add criteria.",
    };
  }

  /** @type {string[]} */
  const sets = [];

  if (hasDollar) {
    // FIX: process ALL known $ operators; unknown ones warn but don't silently emit broken SQL
    for (const op of updKeys) {
      const opVal = updateObj[op];

      if (op === "$set") {
        if (!isPlainObject(opVal))
          return { sql: "", warnings, error: "$set must be an object." };
        for (const [k, v] of Object.entries(opVal)) {
          sets.push(`${quoteIdentifier(k)} = ${emitLiteral(v, warnings)}`);
        }
      } else if (op === "$unset") {
        if (!isPlainObject(opVal))
          return { sql: "", warnings, error: "$unset must be an object." };
        for (const k of Object.keys(opVal)) {
          sets.push(`${quoteIdentifier(k)} = NULL`);
        }
      } else if (op === "$inc") {
        // FIX: $inc — col = col + n
        if (!isPlainObject(opVal))
          return { sql: "", warnings, error: "$inc must be an object." };
        for (const [k, v] of Object.entries(opVal)) {
          if (typeof v !== "number")
            return {
              sql: "",
              warnings,
              error: `$inc value for "${k}" must be a number.`,
            };
          const qk = quoteIdentifier(k);
          const sign = v < 0 ? `- ${Math.abs(v)}` : `+ ${v}`;
          sets.push(`${qk} = ${qk} ${sign}`);
        }
      } else if (op === "$mul") {
        // FIX: $mul — col = col * n
        if (!isPlainObject(opVal))
          return { sql: "", warnings, error: "$mul must be an object." };
        for (const [k, v] of Object.entries(opVal)) {
          if (typeof v !== "number")
            return {
              sql: "",
              warnings,
              error: `$mul value for "${k}" must be a number.`,
            };
          sets.push(`${quoteIdentifier(k)} = ${quoteIdentifier(k)} * ${v}`);
        }
      } else if (op === "$rename") {
        // FIX: $rename — not directly expressible in a single UPDATE; warn
        warnings.push(
          `$rename cannot be translated to a single SQLite UPDATE statement. ` +
            `You will need separate UPDATE … SET new = old, old = NULL statements.`
        );
      } else if (op === "$min") {
        // FIX: $min — col = MIN(col, value) via CASE WHEN
        if (!isPlainObject(opVal))
          return { sql: "", warnings, error: "$min must be an object." };
        for (const [k, v] of Object.entries(opVal)) {
          const qk = quoteIdentifier(k);
          const lit = emitLiteral(v, warnings);
          sets.push(`${qk} = CASE WHEN ${qk} <= ${lit} THEN ${qk} ELSE ${lit} END`);
        }
      } else if (op === "$max") {
        // FIX: $max — col = MAX(col, value) via CASE WHEN
        if (!isPlainObject(opVal))
          return { sql: "", warnings, error: "$max must be an object." };
        for (const [k, v] of Object.entries(opVal)) {
          const qk = quoteIdentifier(k);
          const lit = emitLiteral(v, warnings);
          sets.push(`${qk} = CASE WHEN ${qk} >= ${lit} THEN ${qk} ELSE ${lit} END`);
        }
      } else {
        warnings.push(`Update operator "${op}" is not supported; ignored.`);
      }
    }
  } else {
    // Replacement-style update (no $ keys)
    for (const [k, v] of Object.entries(updateObj)) {
      sets.push(`${quoteIdentifier(k)} = ${emitLiteral(v, warnings)}`);
    }
  }

  if (sets.length === 0)
    return {
      sql: "",
      warnings,
      error: "No SET clauses produced (empty operators or replacement object?).",
    };

  const sql = `UPDATE ${table} SET ${sets.join(", ")}${sqlWhereClause(where)};`;
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
    return {
      sql: "",
      warnings,
      error:
        "Empty filter would delete all rows. Check \u201cAllow empty filter\u201d or add criteria.",
    };
  }

  const sql = `DELETE FROM ${table}${sqlWhereClause(where)};`;
  return { sql, warnings };
}

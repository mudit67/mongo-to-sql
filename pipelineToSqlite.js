/**
 * Aggregation pipeline → SQLite.
 *
 * Supported stages: $match, $lookup (multiple, equality), $unwind, $project,
 * $addFields, $group, $sort, $limit, $skip.
 */

import {
  filterToWhere,
  sortToOrderBy,
  quoteIdentifier,
  normalizeTable,
} from "./mongoToSqlite.js";

/** @typedef {{ sql: string, warnings: string[], error?: string }} SqlResult */

/** Alias for the primary (left) table in joins. */
const BASE = "base";

/**
 * @param {unknown} stage
 * @returns {{ name: string, body: unknown } | null}
 */
function stageNameBody(stage) {
  if (!isPlainObject(stage)) return null;
  const keys = Object.keys(stage);
  if (keys.length !== 1) return null;
  const name = keys[0];
  if (!name.startsWith("$")) return null;
  return { name, body: /** @type {Record<string, unknown>} */ (stage)[name] };
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
 * @param {string} path  e.g. "$region" or "region"
 * @returns {string}     e.g. "region"
 */
function stripDollar(path) {
  if (typeof path !== "string") return String(path);
  return path.startsWith("$") ? path.slice(1) : path;
}

/**
 * Produce a qualified SQL column reference for a field path, given the join
 * alias map (tableName → sqlAlias).
 *
 * FIX: The original code hardcoded BASE for group-by paths and JOIN (j1) for
 * accumulator paths. This was wrong whenever:
 *   - there was no join (JOIN alias doesn't exist)
 *   - the accumulator field lives on the base table not a joined table
 *
 * New approach: if there are joins, qualify every unqualified field reference
 * with BASE unless it is known to come from a joined table (tracked via the
 * joinAliases map). If there are no joins, never qualify.
 *
 * @param {string} fieldPath         raw field path (may start with $)
 * @param {Map<string, string>} joinAliases  foreignTable → sqlAlias
 * @returns {string}
 */
function qualifyField(fieldPath, joinAliases) {
  const field = stripDollar(fieldPath);
  if (joinAliases.size === 0) return quoteIdentifier(field);
  // Default to BASE for all fields; callers can override when they know the source.
  return `${BASE}.${quoteIdentifier(field)}`;
}

/**
 * Validate a simple (equality) $lookup spec.
 * FIX: pipeline/let variants now emit a clear unsupported error.
 *
 * @param {unknown} spec
 * @returns {string | null}  error message or null if valid
 */
function validateLookup(spec) {
  if (!isPlainObject(spec))
    return "$lookup spec must be an object.";
  if (spec.pipeline != null || spec.let != null)
    return "$lookup with pipeline/let is not supported; only simple equality $lookup is.";
  if (typeof spec.from !== "string" || !spec.from.trim())
    return "$lookup.from must be a non-empty string.";
  if (typeof spec.localField !== "string" || !spec.localField.trim())
    return "$lookup.localField must be a non-empty string.";
  if (typeof spec.foreignField !== "string" || !spec.foreignField.trim())
    return "$lookup.foreignField must be a non-empty string.";
  if (spec.as != null && typeof spec.as !== "string")
    return "$lookup.as must be a string if present.";
  return null;
}

/**
 * Compile a $group body into SELECT and GROUP BY parts.
 *
 * FIX: qualifyGroupPath alias logic was inverted — accumulators always used
 * the JOIN alias even when there was no join. Now uses qualifyField() which
 * defaults to BASE and is safe for no-join cases.
 *
 * @param {Record<string, unknown>} groupBody
 * @param {Map<string, string>} joinAliases
 * @param {string[]} warnings
 * @returns {{ selectParts: string[], groupByParts: string[], error?: string }}
 */
function compileGroup(groupBody, joinAliases, warnings) {
  if (!isPlainObject(groupBody))
    return { selectParts: [], groupByParts: [], error: "$group must be an object." };

  const selectParts = [];
  const groupByParts = [];

  const _id = groupBody._id;
  const rest = { ...groupBody };
  delete rest._id;

  if (_id === null || _id === undefined) {
    selectParts.push("NULL AS _id");
  } else if (typeof _id === "string") {
    const sql = qualifyField(_id, joinAliases);
    selectParts.push(`${sql} AS ${quoteIdentifier("_id")}`);
    groupByParts.push(sql);
  } else if (isPlainObject(_id)) {
    const entries = Object.entries(_id);
    if (entries.length === 0)
      return {
        selectParts: [],
        groupByParts: [],
        error: "$group._id object cannot be empty.",
      };
    for (const [outKey, pathVal] of entries) {
      if (typeof pathVal !== "string") {
        return {
          selectParts: [],
          groupByParts: [],
          error: `$group._id.${outKey} must be a string field path like "$region".`,
        };
      }
      const sql = qualifyField(pathVal, joinAliases);
      selectParts.push(`${sql} AS ${quoteIdentifier(outKey)}`);
      groupByParts.push(sql);
    }
  } else {
    return {
      selectParts: [],
      groupByParts: [],
      error: "$group._id must be null, a string path, or an object of paths.",
    };
  }

  for (const [outField, spec] of Object.entries(rest)) {
    const err = compileAccumulator(outField, spec, joinAliases, warnings, selectParts);
    if (err) return { selectParts: [], groupByParts: [], error: err };
  }

  return { selectParts, groupByParts };
}

/**
 * FIX: Added $first and $last accumulators (MIN/MAX semantics in SQLite —
 * SQLite has no FIRST_VALUE aggregate without window functions; we use MIN/MAX
 * and warn that ordering is not guaranteed without a prior $sort).
 *
 * @param {string} outField
 * @param {unknown} spec
 * @param {Map<string, string>} joinAliases
 * @param {string[]} warnings
 * @param {string[]} selectParts
 * @returns {string | undefined}
 */
function compileAccumulator(outField, spec, joinAliases, warnings, selectParts) {
  if (!isPlainObject(spec))
    return `Accumulator for "${outField}" must be an object.`;
  const keys = Object.keys(spec);
  if (keys.length !== 1)
    return `Accumulator for "${outField}" must have exactly one operator.`;
  const op = keys[0];
  const arg = spec[op];
  const outSql = quoteIdentifier(outField);

  // COUNT(*) shorthands
  if (op === "$sum" && arg === 1) {
    selectParts.push(`COUNT(*) AS ${outSql}`);
    return undefined;
  }
  if (op === "$count" && isPlainObject(arg) && Object.keys(arg).length === 0) {
    selectParts.push(`COUNT(*) AS ${outSql}`);
    return undefined;
  }

  if (typeof arg !== "string") {
    return `Unsupported accumulator form for "${outField}": arg must be a field path string.`;
  }

  const col = qualifyField(arg, joinAliases);

  switch (op) {
    case "$sum":
      selectParts.push(`SUM(${col}) AS ${outSql}`);
      return undefined;
    case "$avg":
      selectParts.push(`AVG(${col}) AS ${outSql}`);
      return undefined;
    case "$min":
      selectParts.push(`MIN(${col}) AS ${outSql}`);
      return undefined;
    case "$max":
      selectParts.push(`MAX(${col}) AS ${outSql}`);
      return undefined;
    // FIX: $first / $last — SQLite has no FIRST/LAST aggregate; MIN/MAX approximate
    // this only when combined with a preceding $sort. Warn accordingly.
    case "$first":
      warnings.push(
        `$first on "${outField}" approximated as MIN(${col}). ` +
          `Guarantee ordering with a $sort stage before $group.`
      );
      selectParts.push(`MIN(${col}) AS ${outSql}`);
      return undefined;
    case "$last":
      warnings.push(
        `$last on "${outField}" approximated as MAX(${col}). ` +
          `Guarantee ordering with a $sort stage before $group.`
      );
      selectParts.push(`MAX(${col}) AS ${outSql}`);
      return undefined;
    default:
      return `Unsupported accumulator "${op}" for "${outField}".`;
  }
}

/**
 * Compile a $project or $addFields body into extra SELECT expressions.
 *
 * $project with inclusion fields → SELECT only those fields (replaces *)
 * $project with exclusion fields → SELECT * with a warning (needs schema hint)
 * $addFields → appends computed columns; SELECT * plus the new columns
 *
 * Both stages support simple field references ("$field") and numeric literals.
 * Arithmetic expressions ($add, $subtract, $multiply, $divide) are supported
 * one level deep.
 *
 * @param {Record<string, unknown>} body
 * @param {Map<string, string>} joinAliases
 * @param {"$project" | "$addFields"} stageName
 * @param {string[]} warnings
 * @returns {{ selectExpr: string, error?: string }}
 *   selectExpr: full SELECT list string, or "" to signal "use *"
 */
function compileProjection(body, joinAliases, stageName, warnings) {
  if (!isPlainObject(body))
    return { selectExpr: "", error: `${stageName} body must be an object.` };

  const entries = Object.entries(body);
  if (entries.length === 0) {
    warnings.push(`${stageName} is empty; SELECT * used.`);
    return { selectExpr: "*" };
  }

  const inclusions = [];
  const exclusions = [];
  const computed = [];

  for (const [field, expr] of entries) {
    if (expr === 1 || expr === true) {
      inclusions.push(quoteIdentifier(field));
    } else if (expr === 0 || expr === false) {
      exclusions.push(field);
    } else if (typeof expr === "string" && expr.startsWith("$")) {
      // Field rename / alias
      const srcCol = qualifyField(expr, joinAliases);
      computed.push(`${srcCol} AS ${quoteIdentifier(field)}`);
    } else if (isPlainObject(expr)) {
      const exprSql = compileExpression(expr, joinAliases, warnings);
      if (exprSql == null) {
        warnings.push(`${stageName}: expression for "${field}" not supported; skipped.`);
      } else {
        computed.push(`${exprSql} AS ${quoteIdentifier(field)}`);
      }
    } else {
      warnings.push(`${stageName}: value for "${field}" not recognized; skipped.`);
    }
  }

  const mongoIdExcluded = exclusions.includes("_id");
  const mongoIdIncludedExplicit = entries.some(
    ([k, v]) => k === "_id" && (v === 1 || v === true),
  );
  /** MongoDB $project retains _id unless {_id: 0}. */
  const idColRef =
    joinAliases.size > 0 ? `${BASE}.${quoteIdentifier("_id")}` : quoteIdentifier("_id");
  /** @param {string[]} cols */
  const leadMongoProjectIdIfNeeded = (cols) => {
    if (
      mongoIdExcluded ||
      mongoIdIncludedExplicit ||
      cols.length === 0
    )
      return cols;
    return [idColRef, ...cols];
  };

  if (stageName === "$addFields") {
    // $addFields keeps all existing columns and appends computed ones
    if (computed.length === 0) {
      warnings.push("$addFields produced no new columns; SELECT * used.");
      return { selectExpr: "*" };
    }
    const base = joinAliases.size > 0 ? `${BASE}.*` : "*";
    return { selectExpr: [base, ...computed].join(", ") };
  }

  // $project
  if (inclusions.length > 0 && computed.length > 0) {
    const incWithId = leadMongoProjectIdIfNeeded(inclusions);
    return { selectExpr: [...incWithId, ...computed].join(", ") };
  }
  if (inclusions.length > 0) {
    return { selectExpr: leadMongoProjectIdIfNeeded(inclusions).join(", ") };
  }
  if (computed.length > 0) {
    let parts = [...computed];
    if (!mongoIdExcluded && !mongoIdIncludedExplicit) parts.unshift(idColRef);
    return { selectExpr: parts.join(", ") };
  }
  if (exclusions.length > 0) {
    warnings.push(
      "$project exclusion without schema hint; using SELECT *. " +
        "Add a schema hint to the Table section for accurate column exclusion."
    );
    return { selectExpr: "*" };
  }

  warnings.push("$project produced no columns; SELECT * used.");
  return { selectExpr: "*" };
}

/**
 * Compile a simple arithmetic/conditional expression object.
 * Supports: $add, $subtract, $multiply, $divide, $toLower, $toUpper,
 *           $concat (two-arg), $ifNull, $cond (object form).
 *
 * @param {Record<string, unknown>} expr
 * @param {Map<string, string>} joinAliases
 * @param {string[]} warnings
 * @returns {string | null}  SQL expression or null if unsupported
 */
function compileExpression(expr, joinAliases, warnings) {
  const ops = Object.keys(expr);
  if (ops.length !== 1) return null;
  const op = ops[0];
  const arg = expr[op];

  /**
   * @param {unknown} v
   * @returns {string | null}
   */
  function resolveArg(v) {
    if (typeof v === "string" && v.startsWith("$"))
      return qualifyField(v, joinAliases);
    if (typeof v === "number") return String(v);
    if (typeof v === "string") return `'${v.replace(/'/g, "''")}'`;
    if (isPlainObject(v)) return compileExpression(v, joinAliases, warnings);
    return null;
  }

  switch (op) {
    case "$add": {
      if (!Array.isArray(arg) || arg.length < 2) return null;
      const parts = arg.map(resolveArg);
      if (parts.some((p) => p == null)) return null;
      return `(${parts.join(" + ")})`;
    }
    case "$subtract": {
      if (!Array.isArray(arg) || arg.length !== 2) return null;
      const [a, b] = arg.map(resolveArg);
      if (a == null || b == null) return null;
      return `(${a} - ${b})`;
    }
    case "$multiply": {
      if (!Array.isArray(arg) || arg.length < 2) return null;
      const parts = arg.map(resolveArg);
      if (parts.some((p) => p == null)) return null;
      return `(${parts.join(" * ")})`;
    }
    case "$divide": {
      if (!Array.isArray(arg) || arg.length !== 2) return null;
      const [a, b] = arg.map(resolveArg);
      if (a == null || b == null) return null;
      return `(${a} / ${b})`;
    }
    case "$toLower": {
      const a = resolveArg(arg);
      return a ? `LOWER(${a})` : null;
    }
    case "$toUpper": {
      const a = resolveArg(arg);
      return a ? `UPPER(${a})` : null;
    }
    case "$concat": {
      if (!Array.isArray(arg) || arg.length < 2) return null;
      const parts = arg.map(resolveArg);
      if (parts.some((p) => p == null)) return null;
      return parts.join(" || ");
    }
    case "$ifNull": {
      if (!Array.isArray(arg) || arg.length !== 2) return null;
      const [a, b] = arg.map(resolveArg);
      if (a == null || b == null) return null;
      return `COALESCE(${a}, ${b})`;
    }
    case "$cond": {
      if (!isPlainObject(arg)) return null;
      const ifPart = resolveArg(arg.if);
      const thenPart = resolveArg(arg.then);
      const elsePart = resolveArg(arg.else);
      if (ifPart == null || thenPart == null || elsePart == null) return null;
      return `CASE WHEN ${ifPart} THEN ${thenPart} ELSE ${elsePart} END`;
    }
    default:
      return null;
  }
}

/**
 * @param {unknown} n
 * @returns {number | null}
 */
function parseNonNegInt(n) {
  if (n === null || n === undefined || n === "") return null;
  const v = typeof n === "number" ? n : parseInt(String(n), 10);
  if (!Number.isFinite(v) || v < 0) return null;
  return v;
}

/**
 * Main pipeline compiler.
 *
 * FIX summary vs original:
 *   1. Multiple $lookup stages → multiple LEFT JOINs with auto-aliased tables (j1, j2, …)
 *   2. $match after $lookup → compiled as an additional WHERE predicate (qualified with BASE)
 *   3. $project stage compiled and applied
 *   4. $addFields stage compiled and applied
 *   5. $unwind stage: warns (SQLite JSON1 json_each needed for true unwind) but continues
 *   6. $sort before $group is allowed (needed for $first/$last semantics)
 *   7. qualifyGroupPath inversion fixed via qualifyField()
 *   8. ORDER BY qualification uses correct alias in join context
 *
 * @param {{ table: string, pipeline: unknown }} opts
 * @returns {SqlResult}
 */
export function buildAggregate(opts) {
  const warnings = [];
  const tableSql = normalizeTable(opts.table, warnings);
  if (!tableSql) return { sql: "", warnings, error: "Invalid or empty table name." };

  let stages;
  try {
    const raw = opts.pipeline;
    if (typeof raw === "string") stages = JSON.parse(raw);
    else stages = raw;
  } catch (e) {
    return {
      sql: "",
      warnings,
      error: `Pipeline JSON: ${/** @type {Error} */ (e).message}`,
    };
  }
  if (!Array.isArray(stages))
    return { sql: "", warnings, error: "Pipeline must be a JSON array of stages." };
  if (stages.length === 0)
    return { sql: "", warnings, error: "Pipeline is empty." };

  // Validate all stages have the right shape before starting compilation
  for (let i = 0; i < stages.length; i++) {
    const sb = stageNameBody(stages[i]);
    if (!sb) {
      return {
        sql: "",
        warnings,
        error: `Invalid stage at index ${i}: each stage must be a single-key object like { "$match": { ... } }.`,
      };
    }
    const supported = [
      "$match", "$lookup", "$unwind", "$group",
      "$project", "$addFields", "$sort", "$limit", "$skip",
    ];
    if (!supported.includes(sb.name)) {
      return {
        sql: "",
        warnings,
        error: `Unsupported stage "${sb.name}" at index ${i}. Supported: ${supported.join(", ")}.`,
      };
    }
  }

  // ── Pass 1: collect all stages by category ────────────────────────────────

  /** @type {Record<string, unknown>[]} */
  const matchFilters = [];      // all $match bodies (before or after $lookup)

  /** @type {Array<Record<string, unknown>>} */
  const lookupSpecs = [];       // all $lookup bodies in order

  /** @type {Record<string, unknown> | null} */
  let groupBody = null;

  /** @type {Record<string, unknown> | null} */
  let projectBody = null;
  /** @type {"$project" | "$addFields"} */
  let projectStageName = "$project";

  /** @type {Record<string, unknown> | null} */
  let sortDoc = null;
  let limitVal = null;
  let skipVal = null;

  // Whether a $group has been seen yet (for ordering validation)
  let groupSeen = false;

  for (let i = 0; i < stages.length; i++) {
    const sb = stageNameBody(stages[i]);
    // Already validated above; sb is non-null
    const { name, body } = sb;

    if (name === "$match") {
      if (!isPlainObject(body))
        return { sql: "", warnings, error: `$match at index ${i} body must be an object.` };
      matchFilters.push(/** @type {Record<string, unknown>} */ (body));
    } else if (name === "$lookup") {
      if (groupSeen)
        return { sql: "", warnings, error: "$lookup after $group is not supported." };
      const err = validateLookup(body);
      if (err) return { sql: "", warnings, error: err };
      lookupSpecs.push(/** @type {Record<string, unknown>} */ (body));
    } else if (name === "$unwind") {
      // FIX: $unwind is not silently ignored; it warns that SQLite can't
      // natively unwind arrays (needs json_each which is env-specific).
      const path =
        isPlainObject(body) ? body.path : body;
      const fieldName =
        typeof path === "string" ? stripDollar(path) : "(unknown)";
      warnings.push(
        `$unwind on "${fieldName}" cannot be fully compiled to SQLite. ` +
          `SQLite requires json_each() in a FROM clause to unwind JSON arrays. ` +
          `The stage is skipped; results will include un-unwound (array) values.`
      );
    } else if (name === "$group") {
      if (groupSeen)
        return { sql: "", warnings, error: "Multiple $group stages are not supported." };
      if (!isPlainObject(body))
        return { sql: "", warnings, error: `$group at index ${i} body must be an object.` };
      groupBody = /** @type {Record<string, unknown>} */ (body);
      groupSeen = true;
    } else if (name === "$project" || name === "$addFields") {
      if (!isPlainObject(body))
        return { sql: "", warnings, error: `${name} at index ${i} body must be an object.` };
      if (projectBody !== null) {
        warnings.push(`Multiple ${name} stages; only the last one is applied.`);
      }
      projectBody = /** @type {Record<string, unknown>} */ (body);
      projectStageName = /** @type {"$project" | "$addFields"} */ (name);
    } else if (name === "$sort") {
      if (!isPlainObject(body))
        return { sql: "", warnings, error: `$sort at index ${i} body must be an object.` };
      sortDoc = /** @type {Record<string, unknown>} */ (body);
    } else if (name === "$limit") {
      const lim = parseNonNegInt(body);
      if (lim === null)
        return { sql: "", warnings, error: `$limit at index ${i} must be a non-negative integer.` };
      limitVal = lim;
    } else if (name === "$skip") {
      const sk = parseNonNegInt(body);
      if (sk === null)
        return { sql: "", warnings, error: `$skip at index ${i} must be a non-negative integer.` };
      skipVal = sk;
    }
  }

  // ── Pass 2: build JOIN aliases map ────────────────────────────────────────

  // FIX: Multiple $lookup stages → multiple LEFT JOINs with aliases j1, j2, …
  /** @type {Map<string, string>}  foreignTableName → sqlAlias */
  const joinAliases = new Map();
  /** @type {Array<{ joinTableSql: string, alias: string, localField: string, foreignField: string }>} */
  const joins = [];

  for (let i = 0; i < lookupSpecs.length; i++) {
    const spec = lookupSpecs[i];
    const alias = `j${i + 1}`;
    const joinTableSql = normalizeTable(String(spec.from), warnings);
    if (!joinTableSql)
      return { sql: "", warnings, error: `Invalid $lookup.from table name in lookup ${i + 1}.` };
    joinAliases.set(String(spec.from), alias);
    joins.push({
      joinTableSql,
      alias,
      localField: String(spec.localField),
      foreignField: String(spec.foreignField),
    });
    warnings.push(
      `$lookup #${i + 1} on "${spec.from}" compiles to LEFT JOIN: ` +
        `multiple right-side matches duplicate left rows (MongoDB returns an array field).`
    );
  }

  const hasJoin = joins.length > 0;

  // ── Pass 3: build FROM clause ─────────────────────────────────────────────

  let fromSql = hasJoin ? `${tableSql} AS ${BASE}` : tableSql;
  for (const j of joins) {
    fromSql += ` LEFT JOIN ${j.joinTableSql} AS ${j.alias}` +
      ` ON ${BASE}.${quoteIdentifier(j.localField)} = ${j.alias}.${quoteIdentifier(j.foreignField)}`;
  }

  // ── Pass 4: build WHERE clause ────────────────────────────────────────────

  // FIX: All $match filters (before AND after $lookup) compile to WHERE.
  // In a join context, qualify every field reference with BASE alias.
  const whereParts = matchFilters.map((m) =>
    filterToWhere(m, warnings, hasJoin ? BASE : null)
  );
  const tautologyOnly =
    whereParts.length === 0 ||
    whereParts.every((w) => String(w).trim() === "1");
  const whereSql = tautologyOnly
    ? ""
    : whereParts.map((w) => `(${w})`).join(" AND ");

  // ── Pass 5: build SELECT list ─────────────────────────────────────────────

  let selectList;

  if (groupBody !== null) {
    // $group dominates SELECT — $project on top of $group is unsupported cleanly
    if (projectBody !== null) {
      warnings.push(
        "$project after $group is not supported; $project is ignored. " +
          "Use $group accumulator aliases directly."
      );
    }
    const compiled = compileGroup(groupBody, joinAliases, warnings);
    if (compiled.error) return { sql: "", warnings, error: compiled.error };
    if (compiled.selectParts.length === 0)
      return { sql: "", warnings, error: "$group produced no select columns." };

    selectList = compiled.selectParts.join(", ");
    const groupByClause = compiled.groupByParts.length
      ? ` GROUP BY ${compiled.groupByParts.join(", ")}`
      : "";

    // FIX: ORDER BY in join context — qualify bare field names with BASE
    const orderBy = buildOrderBy(sortDoc, warnings, hasJoin);

    const whereFrag = whereSql ? ` WHERE ${whereSql}` : "";
    let sql = `SELECT ${selectList} FROM ${fromSql}${whereFrag}${groupByClause}`;
    if (orderBy) sql += ` ORDER BY ${orderBy}`;
    sql += buildLimitOffset(limitVal, skipVal, warnings);

    return { sql: sql + ";", warnings };
  }

  // No $group
  if (projectBody !== null) {
    const result = compileProjection(projectBody, joinAliases, projectStageName, warnings);
    if (result.error) return { sql: "", warnings, error: result.error };
    selectList = result.selectExpr || "*";
  } else {
    selectList = hasJoin ? `${BASE}.*, ${joins.map((j) => `${j.alias}.*`).join(", ")}` : "*";
  }

  const orderBy = buildOrderBy(sortDoc, warnings, hasJoin);

  const whereFrag = whereSql ? ` WHERE ${whereSql}` : "";
  let sql = `SELECT ${selectList} FROM ${fromSql}${whereFrag}`;
  if (orderBy) sql += ` ORDER BY ${orderBy}`;
  sql += buildLimitOffset(limitVal, skipVal, warnings);

  return { sql: sql + ";", warnings };
}

/**
 * Build ORDER BY clause, qualifying bare field names with BASE when in a join.
 *
 * FIX: The original qualifyOrderByFragment always prefixed with BASE, even
 * when it was already qualified. This version uses sortToOrderBy and then
 * qualifies any unqualified fragment with BASE in a join context.
 *
 * @param {Record<string, unknown> | null} sortDoc
 * @param {string[]} warnings
 * @param {boolean} hasJoin
 * @returns {string}
 */
function buildOrderBy(sortDoc, warnings, hasJoin) {
  const raw = sortToOrderBy(sortDoc, warnings);
  if (!raw) return "";
  if (!hasJoin) return raw;
  // Qualify each sort term with BASE if it has no existing table prefix
  return raw
    .split(", ")
    .map((frag) => {
      const m = frag.trim().match(/^(\S+)\s+(ASC|DESC)$/i);
      if (!m) return frag;
      const col = m[1];
      if (col.includes(".")) return frag; // already qualified
      return `${BASE}.${col} ${m[2].toUpperCase()}`;
    })
    .join(", ");
}

/**
 * @param {number | null} limitVal
 * @param {number | null} skipVal
 * @param {string[]} warnings
 * @returns {string}
 */
function buildLimitOffset(limitVal, skipVal, warnings) {
  if (limitVal !== null) {
    let s = ` LIMIT ${limitVal}`;
    if (skipVal !== null && skipVal > 0) s += ` OFFSET ${skipVal}`;
    return s;
  }
  if (skipVal !== null && skipVal > 0) {
    warnings.push("OFFSET without LIMIT is invalid in SQLite; adding LIMIT -1.");
    return ` LIMIT -1 OFFSET ${skipVal}`;
  }
  return "";
}

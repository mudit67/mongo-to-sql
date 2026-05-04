/**
 * Limited aggregation pipeline → SQLite (subset: $match, $lookup, $group, $sort, $limit, $skip).
 */

import {
  filterToWhere,
  sortToOrderBy,
  quoteIdentifier,
  normalizeTable,
} from "./mongoToSqlite.js";

/** @typedef {{ sql: string, warnings: string[], error?: string }} SqlResult */

const BASE = "base";
const JOIN = "j1";

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
  return typeof value === "object" && value !== null && !Array.isArray(value) && !(value instanceof Date);
}

/**
 * @param {unknown} spec
 * @returns {SqlResult | null} error result or null if ok
 */
function validateLookup(spec) {
  if (!isPlainObject(spec)) return { sql: "", warnings: [], error: "$lookup spec must be an object." };
  if (spec.pipeline != null || spec.let != null) {
    return { sql: "", warnings: [], error: "$lookup with pipeline or let is not supported." };
  }
  const from = spec.from;
  const localField = spec.localField;
  const foreignField = spec.foreignField;
  if (typeof from !== "string" || !from.trim()) return { sql: "", warnings: [], error: "$lookup.from must be a non-empty string." };
  if (typeof localField !== "string" || !localField.trim()) return { sql: "", warnings: [], error: "$lookup.localField must be a non-empty string." };
  if (typeof foreignField !== "string" || !foreignField.trim()) return { sql: "", warnings: [], error: "$lookup.foreignField must be a non-empty string." };
  if (spec.as != null && typeof spec.as !== "string") return { sql: "", warnings: [], error: "$lookup.as must be a string if present." };
  return null;
}

/**
 * @param {string} path
 * @returns {string}
 */
function stripFieldPath(path) {
  if (typeof path !== "string") return String(path);
  return path.startsWith("$") ? path.slice(1) : path;
}

/**
 * @param {unknown} v
 * @param {boolean} hasJoin
 * @param {boolean} forGroupBy
 * @returns {string}
 */
function qualifyGroupPath(v, hasJoin, forGroupBy) {
  const p = stripFieldPath(/** @type {string} */ (v));
  if (!hasJoin) return quoteIdentifier(p);
  const alias = forGroupBy ? BASE : JOIN;
  return `${alias}.${quoteIdentifier(p)}`;
}

/**
 * @param {Record<string, unknown>} groupBody
 * @param {boolean} hasJoin
 * @param {string[]} warnings
 * @returns {{ selectParts: string[], groupByParts: string[], error?: string }}
 */
function compileGroup(groupBody, hasJoin, warnings) {
  if (!isPlainObject(groupBody)) return { selectParts: [], groupByParts: [], error: "$group must be an object." };

  const selectParts = [];
  const groupByParts = [];

  const _id = groupBody._id;
  const rest = { ...groupBody };
  delete rest._id;

  if (_id === null || _id === undefined) {
    selectParts.push("NULL AS _id");
  } else if (typeof _id === "string") {
    const sql = qualifyGroupPath(_id, hasJoin, true);
    selectParts.push(`${sql} AS ${quoteIdentifier("_id")}`);
    groupByParts.push(sql);
  } else if (isPlainObject(_id)) {
    const entries = Object.entries(_id);
    if (entries.length === 0) return { selectParts: [], groupByParts: [], error: "$group._id object cannot be empty." };
    for (const [outKey, pathVal] of entries) {
      if (typeof pathVal !== "string") {
        return { selectParts: [], groupByParts: [], error: `$group._id.${outKey} must be a string field path like "$region".` };
      }
      const sql = qualifyGroupPath(pathVal, hasJoin, true);
      selectParts.push(`${sql} AS ${quoteIdentifier(outKey)}`);
      groupByParts.push(sql);
    }
  } else {
    return { selectParts: [], groupByParts: [], error: "$group._id must be null, a string path, or an object of paths." };
  }

  for (const [outField, spec] of Object.entries(rest)) {
    const err = compileAccumulator(outField, spec, hasJoin, warnings, selectParts);
    if (err) return { selectParts: [], groupByParts: [], error: err };
  }

  return { selectParts, groupByParts };
}

/**
 * @param {string} outField
 * @param {unknown} spec
 * @param {boolean} hasJoin
 * @param {string[]} warnings
 * @param {string[]} selectParts
 * @returns {string | undefined} error message
 */
function compileAccumulator(outField, spec, hasJoin, warnings, selectParts) {
  if (!isPlainObject(spec)) return `Accumulator for "${outField}" must be an object.`;
  const keys = Object.keys(spec);
  if (keys.length !== 1) return `Accumulator for "${outField}" must have exactly one operator.`;
  const op = keys[0];
  const arg = spec[op];
  const outSql = quoteIdentifier(outField);

  if (op === "$sum" && arg === 1) {
    selectParts.push(`COUNT(*) AS ${outSql}`);
    return undefined;
  }
  if (op === "$count" && isPlainObject(arg) && Object.keys(arg).length === 0) {
    selectParts.push(`COUNT(*) AS ${outSql}`);
    return undefined;
  }

  if (typeof arg !== "string") {
    return `Unsupported accumulator form for "${outField}".`;
  }
  const col = qualifyGroupPath(arg, hasJoin, false);

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
    default:
      return `Unsupported accumulator "${op}" for "${outField}".`;
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
    return { sql: "", warnings, error: `Pipeline JSON: ${/** @type {Error} */ (e).message}` };
  }
  if (!Array.isArray(stages)) return { sql: "", warnings, error: "Pipeline must be a JSON array of stages." };
  if (stages.length === 0) return { sql: "", warnings, error: "Pipeline is empty." };

  const groupIdx = stages.findIndex((s) => stageNameBody(s)?.name === "$group");
  const hasGroup = groupIdx !== -1;

  const preStages = hasGroup ? stages.slice(0, groupIdx) : stages;
  const groupStage = hasGroup ? stages[groupIdx] : null;
  const tailStages = hasGroup ? stages.slice(groupIdx + 1) : [];

  /** @type {Record<string, unknown>[]} */
  const preMatches = [];
  let lookupSpec = null;
  let seenLookup = false;

  for (let i = 0; i < preStages.length; i++) {
    const sb = stageNameBody(preStages[i]);
    if (!sb) return { sql: "", warnings, error: `Invalid stage at index ${i}: each stage must be a single-key object like { "$match": { ... } }.` };

    if (sb.name === "$sort" || sb.name === "$limit" || sb.name === "$skip") {
      if (hasGroup) return { sql: "", warnings, error: "$sort / $limit / $skip must come after $group." };
      const tailRest = preStages.slice(i);
      for (const t of tailRest) {
        const tb = stageNameBody(t);
        if (!tb || !["$sort", "$limit", "$skip"].includes(tb.name)) {
          return { sql: "", warnings, error: "Without $group: only $match and $lookup may appear before final $sort/$limit/$skip block." };
        }
      }
      break;
    }

    if (sb.name === "$match") {
      if (seenLookup) {
        return { sql: "", warnings, error: "$match after $lookup is not supported in this limited compiler." };
      }
      if (!isPlainObject(sb.body)) return { sql: "", warnings, error: "$match body must be an object." };
      preMatches.push(/** @type {Record<string, unknown>} */ (sb.body));
      continue;
    }

    if (sb.name === "$lookup") {
      const err = validateLookup(sb.body);
      if (err?.error) return err;
      if (lookupSpec) return { sql: "", warnings, error: "Only one $lookup stage is supported." };
      seenLookup = true;
      lookupSpec = /** @type {Record<string, unknown>} */ (sb.body);
      continue;
    }

    return { sql: "", warnings, error: `Unsupported stage "${sb.name}" in this segment.` };
  }

  let tailFromPre = /** @type {unknown[]} */ ([]);
  if (!hasGroup) {
    const firstTailIdx = preStages.findIndex((s) => {
      const n = stageNameBody(s)?.name;
      return n === "$sort" || n === "$limit" || n === "$skip";
    });
    if (firstTailIdx !== -1) {
      tailFromPre = preStages.slice(firstTailIdx);
    }
  }

  for (const t of tailStages) {
    const tb = stageNameBody(t);
    if (!tb || !["$sort", "$limit", "$skip"].includes(tb.name)) {
      return { sql: "", warnings, error: `After $group only $sort, $limit, and $skip are allowed (got "${tb?.name ?? "invalid"}").` };
    }
  }

  const allTail = hasGroup ? tailStages : tailFromPre;
  /** @type {Record<string, unknown> | null} */
  let sortDoc = null;
  let limitVal = null;
  let skipVal = null;
  for (const t of allTail) {
    const tb = stageNameBody(t);
    if (!tb) continue;
    if (tb.name === "$sort") {
      if (!isPlainObject(tb.body)) return { sql: "", warnings, error: "$sort body must be an object." };
      sortDoc = /** @type {Record<string, unknown>} */ (tb.body);
    } else if (tb.name === "$limit") {
      const lim = parseNonNegInt(tb.body);
      if (lim === null) return { sql: "", warnings, error: "$limit must be a non-negative integer." };
      limitVal = lim;
    } else if (tb.name === "$skip") {
      const sk = parseNonNegInt(tb.body);
      if (sk === null) return { sql: "", warnings, error: "$skip must be a non-negative integer." };
      skipVal = sk;
    }
  }

  const hasJoin = lookupSpec != null;
  const joinTableSql = hasJoin ? normalizeTable(String(lookupSpec.from), warnings) : "";
  if (hasJoin && !joinTableSql) return { sql: "", warnings, error: "Invalid $lookup.from table name." };

  if (hasJoin) {
    warnings.push(
      "$lookup compiles to LEFT JOIN: multiple right-side matches duplicate left rows (not Mongo’s array field)."
    );
  }

  const whereParts = [];
  const matchAlias = hasJoin ? BASE : null;
  for (const m of preMatches) {
    whereParts.push(filterToWhere(m, warnings, matchAlias));
  }
  const whereSql = whereParts.length ? whereParts.map((w) => `(${w})`).join(" AND ") : "1";

  let fromSql;
  if (hasJoin) {
    const localField = String(lookupSpec.localField);
    const foreignField = String(lookupSpec.foreignField);
    fromSql = `${tableSql} AS ${BASE} LEFT JOIN ${joinTableSql} AS ${JOIN} ON ${BASE}.${quoteIdentifier(localField)} = ${JOIN}.${quoteIdentifier(foreignField)}`;
  } else {
    fromSql = tableSql;
  }

  let sql;

  if (groupStage) {
    const gb = stageNameBody(groupStage);
    if (!gb || gb.name !== "$group") return { sql: "", warnings, error: "Internal error: $group stage invalid." };
    const compiled = compileGroup(/** @type {Record<string, unknown>} */ (gb.body), hasJoin, warnings);
    if (compiled.error) return { sql: "", warnings, error: compiled.error };
    if (compiled.selectParts.length === 0) return { sql: "", warnings, error: "$group produced no select columns." };

    const selectList = compiled.selectParts.join(", ");
    const groupByClause = compiled.groupByParts.length ? ` GROUP BY ${compiled.groupByParts.join(", ")}` : "";

    sql = `SELECT ${selectList} FROM ${fromSql} WHERE ${whereSql}${groupByClause}`;

    const orderBy = sortToOrderBy(sortDoc, warnings);
    if (orderBy) sql += ` ORDER BY ${orderBy}`;

    if (limitVal !== null) {
      sql += ` LIMIT ${limitVal}`;
      if (skipVal !== null && skipVal > 0) sql += ` OFFSET ${skipVal}`;
    } else if (skipVal !== null && skipVal > 0) {
      warnings.push("OFFSET without LIMIT is invalid in SQLite; adding LIMIT -1.");
      sql += ` LIMIT -1 OFFSET ${skipVal}`;
    }
  } else {
    const selectList = hasJoin ? `${BASE}.*, ${JOIN}.*` : "*";
    sql = `SELECT ${selectList} FROM ${fromSql} WHERE ${whereSql}`;

    const orderBy = sortToOrderBy(sortDoc, warnings);
    if (orderBy) {
      const ob = hasJoin ? orderBy.split(", ").map((frag) => qualifyOrderByFragment(frag)).join(", ") : orderBy;
      sql += ` ORDER BY ${ob}`;
    }

    if (limitVal !== null) {
      sql += ` LIMIT ${limitVal}`;
      if (skipVal !== null && skipVal > 0) sql += ` OFFSET ${skipVal}`;
    } else if (skipVal !== null && skipVal > 0) {
      warnings.push("OFFSET without LIMIT is invalid in SQLite; adding LIMIT -1.");
      sql += ` LIMIT -1 OFFSET ${skipVal}`;
    }
  }

  return { sql: sql + ";", warnings };
}

/**
 * Prefix sort column with base. if join and fragment is `col ASC`
 * @param {string} frag
 * @returns {string}
 */
function qualifyOrderByFragment(frag) {
  const m = frag.trim().match(/^(\S+)\s+(ASC|DESC)$/i);
  if (!m) return frag;
  const col = m[1];
  if (col.includes(".")) return frag;
  return `${BASE}.${col} ${m[2].toUpperCase()}`;
}

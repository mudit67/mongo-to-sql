import express from "express";
import { openDb, ensureSeeded } from "./db.js";

const app = express();
const port = Number(process.env.PORT || 3000);

const db = openDb();
const seedInfo = ensureSeeded(db);

app.use(express.json({ limit: "1mb" }));
app.use(express.static("."));

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, seed: seedInfo });
});

app.post("/api/run-sql", (req, res) => {
  const sqlRaw = req.body?.sql;
  if (typeof sqlRaw !== "string" || !sqlRaw.trim()) {
    res.status(400).json({ ok: false, error: "SQL query is required." });
    return;
  }

  const sql = sqlRaw.trim();
  try {
    const stmt = db.prepare(sql);
    if (stmt.reader) {
      const rows = stmt.all();
      res.json({ ok: true, mode: "query", rowCount: rows.length, rows });
      return;
    }

    const result = stmt.run();
    res.json({
      ok: true,
      mode: "run",
      changes: result.changes,
      lastInsertRowid: result.lastInsertRowid,
    });
  } catch (error) {
    res.status(400).json({
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
  console.log(`Seeded DB at ${seedInfo.dbPath}`);
  console.log(
    `Rows -> users: ${seedInfo.users}, books: ${seedInfo.books}, borrows: ${seedInfo.borrows}`,
  );
});

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { faker } from "@faker-js/faker";

const dataDir = path.join(process.cwd(), "data");
const dbPath = path.join(dataDir, "dummy.sqlite");

function randInt(min, max) {
  return faker.number.int({ min, max });
}

function randomBorrowDate() {
  return faker.date.between({
    from: "2023-01-01T00:00:00.000Z",
    to: "2026-12-31T00:00:00.000Z",
  });
}

function newId() {
  return faker.string.uuid();
}

export function openDb() {
  fs.mkdirSync(dataDir, { recursive: true });
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  return db;
}

function createSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      _id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL,
      region TEXT NOT NULL,
      age INTEGER NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS books (
      _id TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      author TEXT NOT NULL,
      genre TEXT NOT NULL,
      published_year INTEGER NOT NULL,
      available_copies INTEGER NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS borrows (
      _id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      book_id TEXT NOT NULL,
      borrowed_at TEXT NOT NULL,
      returned_at TEXT,
      status TEXT NOT NULL,
      FOREIGN KEY (user_id) REFERENCES users(_id),
      FOREIGN KEY (book_id) REFERENCES books(_id)
    );
  `);
}

function hasExpectedSchema(db) {
  const hasTextIdPk = (table) => {
    const cols = db.prepare(`PRAGMA table_info(${table})`).all();
    const idCol = cols.find((c) => c.name === "_id");
    return !!idCol && String(idCol.type || "").toUpperCase() === "TEXT" && idCol.pk === 1;
  };

  const usersOk = hasTextIdPk("users");
  const booksOk = hasTextIdPk("books");
  const borrowsOk = hasTextIdPk("borrows");
  if (!usersOk || !booksOk || !borrowsOk) return false;

  const borrowCols = db.prepare("PRAGMA table_info(borrows)").all();
  const userId = borrowCols.find((c) => c.name === "user_id");
  const bookId = borrowCols.find((c) => c.name === "book_id");
  return (
    !!userId &&
    !!bookId &&
    String(userId.type || "").toUpperCase() === "TEXT" &&
    String(bookId.type || "").toUpperCase() === "TEXT"
  );
}

function seedUsers(db, count) {
  const statuses = ["active", "inactive", "blocked"];
  const regions = ["north", "south", "east", "west"];
  const insert = db.prepare(
    "INSERT INTO users (_id, name, email, status, region, age, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
  );
  const insertMany = db.transaction((n) => {
    for (let i = 0; i < n; i += 1) {
      insert.run(
        newId(),
        faker.person.fullName(),
        faker.internet.email().toLowerCase(),
        faker.helpers.arrayElement(statuses),
        faker.helpers.arrayElement(regions),
        randInt(18, 75),
        faker.date.past({ years: 4 }).toISOString(),
      );
    }
  });
  insertMany(count);
}

function seedBooks(db, count) {
  const genres = ["fiction", "history", "science", "technology", "fantasy"];
  const insert = db.prepare(
    "INSERT INTO books (_id, title, author, genre, published_year, available_copies, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
  );
  const insertMany = db.transaction((n) => {
    for (let i = 0; i < n; i += 1) {
      insert.run(
        newId(),
        faker.lorem.words({ min: 2, max: 5 }),
        faker.person.fullName(),
        faker.helpers.arrayElement(genres),
        randInt(1980, 2025),
        randInt(0, 20),
        faker.date.past({ years: 6 }).toISOString(),
      );
    }
  });
  insertMany(count);
}

function seedBorrows(db, count) {
  const userIds = db.prepare("SELECT _id FROM users").all().map((row) => row._id);
  const bookIds = db.prepare("SELECT _id FROM books").all().map((row) => row._id);
  if (!userIds.length || !bookIds.length) return;

  const insert = db.prepare(
    "INSERT INTO borrows (_id, user_id, book_id, borrowed_at, returned_at, status) VALUES (?, ?, ?, ?, ?, ?)",
  );
  const insertMany = db.transaction((n) => {
    for (let i = 0; i < n; i += 1) {
      const borrowedAt = randomBorrowDate();
      const isReturned = faker.datatype.boolean({ probability: 0.7 });
      const returnedAt = isReturned
        ? faker.date
            .between({
              from: borrowedAt,
              to: "2026-12-31T00:00:00.000Z",
            })
            .toISOString()
        : null;
      insert.run(
        newId(),
        faker.helpers.arrayElement(userIds),
        faker.helpers.arrayElement(bookIds),
        borrowedAt.toISOString(),
        returnedAt,
        isReturned ? "returned" : "borrowed",
      );
    }
  });
  insertMany(count);
}

export function resetAndSeed(db, config = {}) {
  const usersCount = config.usersCount ?? randInt(10, 50);
  const booksCount = config.booksCount ?? randInt(10, 50);
  const borrowsCount = config.borrowsCount ?? randInt(10, 50);

  // Recreate tables to guarantee `_id TEXT PRIMARY KEY` schema.
  db.exec("DROP TABLE IF EXISTS borrows; DROP TABLE IF EXISTS books; DROP TABLE IF EXISTS users;");
  createSchema(db);

  seedUsers(db, usersCount);
  seedBooks(db, booksCount);
  seedBorrows(db, borrowsCount);

  return {
    dbPath,
    users: db.prepare("SELECT COUNT(*) AS count FROM users").get().count,
    books: db.prepare("SELECT COUNT(*) AS count FROM books").get().count,
    borrows: db.prepare("SELECT COUNT(*) AS count FROM borrows").get().count,
  };
}

export function ensureSeeded(db) {
  createSchema(db);
  if (!hasExpectedSchema(db)) {
    return resetAndSeed(db);
  }
  const row = db
    .prepare(
      "SELECT (SELECT COUNT(*) FROM users) AS users, (SELECT COUNT(*) FROM books) AS books, (SELECT COUNT(*) FROM borrows) AS borrows",
    )
    .get();
  if (row.users > 0 && row.books > 0 && row.borrows > 0) {
    return { dbPath, users: row.users, books: row.books, borrows: row.borrows };
  }
  return resetAndSeed(db);
}

export { dbPath };

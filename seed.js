import { openDb, resetAndSeed } from "./db.js";

const db = openDb();
const info = resetAndSeed(db);

console.log("Database reseeded.");
console.log(`Path: ${info.dbPath}`);
console.log(`users=${info.users} books=${info.books} borrows=${info.borrows}`);

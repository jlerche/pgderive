import { getMigrations } from "better-auth/db/migration";
import { auth, database } from "./auth.mjs";

try {
  const { runMigrations } = await getMigrations(auth.options);
  await runMigrations();
} finally {
  await database.end();
}

import { betterAuth } from "better-auth";
import { organization } from "better-auth/plugins";
import pg from "pg";

const { DATABASE_URL, BETTER_AUTH_URL, BETTER_AUTH_SECRET } = process.env;
if (!DATABASE_URL || !BETTER_AUTH_URL || !BETTER_AUTH_SECRET) {
  throw new Error("DATABASE_URL, BETTER_AUTH_URL and BETTER_AUTH_SECRET are required");
}

export const database = new pg.Pool({ connectionString: DATABASE_URL });
export const auth = betterAuth({
  baseURL: BETTER_AUTH_URL,
  secret: BETTER_AUTH_SECRET,
  database,
  session: { disableSessionRefresh: true },
  emailAndPassword: { enabled: true },
  plugins: [organization()],
});

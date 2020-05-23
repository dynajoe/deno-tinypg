import { TinyPg, TinyPgOptions } from "../../mod.ts";
import { Client } from "https://deno.land/x/postgres/client.ts";
import { QueryResult } from "https://deno.land/x/postgres/query.ts";
import { __ } from "https://deno.land/x/dirname/mod.ts";
const { __dirname } = __(import.meta);
import * as H from "./helper.ts";
const { test } = Deno;

export const connection_string =
  "postgres://postgres:postgres@localhost:5432/tinypg_test?sslmode=disable";

export async function tinyTest(
  name: string,
  fn: (db: TinyPg) => Promise<void>,
) {
  return tinyTestWithOptions(name, {}, fn);
}

export async function tinyTestWithOptions(
  name: string,
  options: Partial<TinyPgOptions>,
  fn: (db: TinyPg) => Promise<void>,
) {
  return test(name, async () => {
    const tiny = H.newTiny(options);
    try {
      await H.setUpDb();
      await fn(tiny);
    } finally {
      await tiny.close();
    }
  });
}
export async function dbQuery(
  query: string,
  args: any[] = [],
): Promise<QueryResult> {
  const client = new Client(connection_string);
  await client.connect();

  try {
    if (args.length > 0) {
      return await client.query(query, args);
    }
    return await client.query(query);
  } finally {
    void client.end();
  }
}

export function getA(): Promise<QueryResult> {
  return dbQuery("SELECT * FROM __tiny_test_db.a;");
}

export function insertA(text: string): Promise<QueryResult> {
  return dbQuery("INSERT INTO __tiny_test_db.a (text) VALUES ($1);", [text]);
}

export async function setUpDb(): Promise<any> {
  const commands = [
    "ROLLBACK;",
    "DROP SCHEMA IF EXISTS __tiny_test_db CASCADE;",
    "CREATE SCHEMA __tiny_test_db;",
    "SET search_path TO __tiny_test_db, public;",
    "CREATE TABLE __tiny_test_db.a (id serial PRIMARY KEY, text text UNIQUE);",
  ];

  for (const cmd of commands) {
    await dbQuery(cmd);
  }
}

export function newTiny(options?: Partial<TinyPgOptions>): TinyPg {
  return new TinyPg({
    connection_string: connection_string,
    root_dir: __dirname + "/sql/",
    ...options,
  });
}

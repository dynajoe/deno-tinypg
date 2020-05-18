import * as H from "./helper.ts";
import * as E from "../errors.ts";
import { expect } from "chai";

const { test } = Deno;

test("Multiple root directories - should allow specifying multiple directories that do not conflict", () => {
  const tiny = H.newTiny({
    root_dir: [__dirname + "/multi/a_sql", __dirname + "/multi/b_sql"],
  });

  expect(tiny.sql_db_calls["a.insert"]).to.exist;
  expect(tiny.sql_db_calls["b.insert"]).to.exist;
});

test("Multiple root directories - should error on naming conflict", () => {
  expect(() => {
    H.newTiny({
      root_dir: [__dirname + "/multi/a_sql", __dirname + "/sql"],
    });
  }).to.throw(E.TinyPgError);
});

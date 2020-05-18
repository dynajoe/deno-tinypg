import { assertEquals } from "https://deno.land/std/testing/asserts.ts";
import * as P from "../parser.ts";
import * as Path from "path";

const { test } = Deno;

test("should parse files", () => {
  const result = P.parseFiles([Path.join(__dirname, "./sql")]);
  const parse_file_marker = result.find((x) =>
    x.key.indexOf("parse_file_test_marker") != -1
  )!;

  assertEquals(parse_file_marker.name, "a_parse_file_test_marker");
  assertEquals(parse_file_marker.relative_path, "a/parse_file_test_marker.sql");
});

test("should correctly format root level file names", () => {
  const result = P.parseFiles([Path.join(__dirname, "./sql")]);
  const root_level_file = result.find((x) =>
    x.key.indexOf("root_level_sql_file") != -1
  )!;

  assertEquals(root_level_file.name, "root_level_sql_file");
  assertEquals(root_level_file.relative_path, "root_level_sql_file.sql");
});

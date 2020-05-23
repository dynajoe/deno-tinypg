import {
  assertEquals,
  assertStrContains,
  assertThrowsAsync,
} from "https://deno.land/std/testing/asserts.ts";
import * as H from "./helper.ts";
import { tinyTest, tinyTestWithOptions } from "./helper.ts";
import { TinyPgError } from "../errors.ts";

tinyTest(
  "Transactions - Sql file queries - should commit successful transactions",
  async (db) => {
    await db.transaction((ctx) => {
      const queries = [1, 2, 3].map((v) => {
        return ctx.sql("a.insert", { text: v.toString() });
      });

      return Promise.all(queries);
    });

    const res = await H.getA();
    assertEquals(res.rows.length, 3);
  },
);

tinyTest(
  "Transactions - Sql file queries - should rollback failed transactions",
  async (db) => {
    try {
      await db.transaction(async (ctx) => {
        await ctx.sql("a.insert", {
          text: "TEST",
        });

        throw new Error("THIS SHOULD ABORT");
      });
    } catch {
      // Expected error
    } finally {
      const res = await H.getA();
      assertEquals(res.rows.length, 0);
    }
  },
);

tinyTestWithOptions(
  "Transactions - Sql file queries - When an error is thrown - should have the correct stack trace",
  { capture_stack_trace: true },
  async (db) => {
    const thisShouldBeInStack = () => {
      return db.transaction((tx_db) => {
        return tx_db.sql("a.test_missing_params");
      });
    };

    try {
      await thisShouldBeInStack();
    } catch (err) {
      assertStrContains(err.stack, "thisShouldBeInStack");
    }
  },
);

tinyTest(
  "Transactions - Raw Queries - should commit successful transactions",
  async (db) => {
    await db.transaction((ctx) => {
      return ctx.query("INSERT INTO __tiny_test_db.a (text) VALUES (:text)", {
        text: "TEST",
      });
    });

    const res = await H.getA();
    assertEquals(res.rows.length, 1);
  },
);

tinyTest(
  "Transactions - Raw queries - should rollback failed transactions",
  async (db) => {
    try {
      await db.transaction(async (ctx) => {
        await ctx.query("INSERT INTO __tiny_test_db.a (text) VALUES (:text)", {
          text: "TEST",
        });
        throw new Error("THIS SHOULD ABORT");
      });
    } catch {
      const res = await H.getA();
      assertEquals(res.rows.length, 0);
    }
  },
);

tinyTestWithOptions(
  "Transactions - Raw queries - When an error is thrown - should have the correct stack trace",
  { capture_stack_trace: true },
  async (db) => {
    const thisShouldBeInStack = () => {
      return db.transaction((tx_db) => {
        return tx_db.query("SELECT 1/0;");
      });
    };

    assertThrowsAsync(
      async () => {
        await thisShouldBeInStack();
      },
      TinyPgError,
      "thisShouldBeInStack",
    );
  },
);

tinyTest(
  "Nested Transactions - should commit successful transactions",
  async (db) => {
    await db
      .transaction(async (ctx) => {
        await ctx.query("INSERT INTO __tiny_test_db.a (text) VALUES (:text)", {
          text: "1",
        });

        await ctx.transaction(async (ctx2) => {
          await ctx2.query(
            "INSERT INTO __tiny_test_db.a (text) VALUES (:text)",
            {
              text: "2",
            },
          );
        });
      });

    const res = await H.getA();
    assertEquals(res.rows.length, 2);
  },
);

tinyTest(
  "Nested Transactions - should rollback on a failed inner transaction",
  async (db) => {
    await db
      .transaction(async (ctx) => {
        await ctx.query("INSERT INTO __tiny_test_db.a (text) VALUES (:text)", {
          text: "1",
        });

        await ctx.transaction(async (ctx2) => {
          await ctx2.query(
            "INSERT INTO __tiny_test_db.a (text) VALUES (:text)",
            {
              text: "1",
            },
          );
        });
      });

    const res = await H.getA();
    assertEquals(res.rows.length, 0);
  },
);

tinyTest(
  "Nested Transactions - should require thennable from transaction function",
  async (db) => {
    assertThrowsAsync(
      async () => {
        await db
          .transaction(
            <any> (() => {
              return null;
            }),
          );
      },
      TinyPgError,
      "thennable",
    );
  },
);

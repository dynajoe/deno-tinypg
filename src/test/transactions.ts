import { assertEquals } from "https://deno.land/std/testing/asserts.ts";
import * as H from "./helper.ts";
const { test } = Deno;

test("Transactions - Sql file queries - should commit successful transactions", async () => {
  const tiny = H.newTiny();
  await H.setUpDb();

  await tiny.transaction((ctx) => {
    const queries = [1, 2, 3].map((v) => {
      return ctx.sql("a.insert", { text: v.toString() });
    });

    return Promise.all(queries);
  });

  const res = await H.getA();
  assertEquals(res.rows.length, 3);
});

test("Transactions - Sql file queries - should rollback failed transactions", async () => {
  const tiny = H.newTiny();
  await H.setUpDb();

  try {
    await tiny.transaction(async (ctx) => {
      await ctx.sql("a.insert", {
        text: "TEST",
      });

      throw new Error("THIS SHOULD ABORT");
    });
  } finally {
    const res = await H.getA();
    assertEquals(res.rows.length, 0);
  }
});

// test('Transactions - Sql file queries - When an error is thrown - should have the correct stack trace', async () => {
//    const thisShouldBeInStack = () => {
//       return H.newTiny({ capture_stack_trace: true }).transaction(tx_db => {
//          return tx_db.sql('a.test_missing_params')
//       })
//    }

//    return thisShouldBeInStack()
//       .then(() => expect.fail('this should not succeed'))
//       .catch(err => {
//          expect(err.stack).to.include('thisShouldBeInStack')
//       })
// })

// test('Transactions - Raw Queries - should commit successful transactions', async () => {
//    const tiny = H.newTiny()
//    await H.setUpDb()

//    await tiny.transaction(ctx => {
//       return ctx.query('INSERT INTO __tiny_test_db.a (text) VALUES (:text)', {
//          text: 'TEST',
//       })
//    })

//    const res = await H.getA()
//    expect(res.rows).to.have.length(1)
// })

// test('Transactions - Raw queries - should rollback failed transactions', async () => {
//    const tiny = H.newTiny()
//    await H.setUpDb()

//    return tiny
//       .transaction(ctx => {
//          return ctx
//             .query('INSERT INTO __tiny_test_db.a (text) VALUES (:text)', {
//                text: 'TEST',
//             })
//             .then(() => {
//                throw new Error('THIS SHOULD ABORT')
//             })
//       })
//       .catch(() => {
//          return H.getA().then(res => {
//             expect(res.rows).to.have.length(0)
//          })
//       })
// })

// test('Transactions - Raw queries - When an error is thrown - should have the correct stack trace', async () => {
//    const thisShouldBeInStack = () => {
//       return H.newTiny({ capture_stack_trace: true }).transaction(tx_db => {
//          return tx_db.query('SELECT 1/0;')
//       })
//    }

//    return thisShouldBeInStack()
//       .then(() => expect.fail('this should not succeed'))
//       .catch(err => {
//          expect(err.stack).to.include('thisShouldBeInStack')
//       })
// })

// test('Nested Transactions - should commit successful transactions', async () => {
//    const tiny = H.newTiny()
//    await H.setUpDb()

//    return tiny
//       .transaction(ctx => {
//          return ctx
//             .query('INSERT INTO __tiny_test_db.a (text) VALUES (:text)', {
//                text: '1',
//             })
//             .then(() => {
//                return ctx.transaction(ctx2 => {
//                   return ctx2.query('INSERT INTO __tiny_test_db.a (text) VALUES (:text)', {
//                      text: '2',
//                   })
//                })
//             })
//       })
//       .then(() => {
//          return H.getA().then(res => {
//             expect(res.rows).to.have.length(2)
//          })
//       })
// })

// test('Nested Transactions - should rollback on a failed inner transaction', async () => {
//    const tiny = H.newTiny()
//    await H.setUpDb()

//    return tiny
//       .transaction(ctx => {
//          return ctx
//             .query('INSERT INTO __tiny_test_db.a (text) VALUES (:text)', {
//                text: '1',
//             })
//             .then(() => {
//                return ctx.transaction(ctx2 => {
//                   return ctx2
//                      .query('INSERT INTO __tiny_test_db.a (text) VALUES (:text)', {
//                         text: '1',
//                      })
//                      .then(() => {
//                         throw new Error('THIS SHOULD ABORT')
//                      })
//                })
//             })
//       })
//       .catch(() => {
//          return H.getA().then(res => {
//             expect(res.rows).to.have.length(0)
//          })
//       })
// })

// test('Nested Transactions - should require thennable from transaction function', async () => {
//    const tiny = H.newTiny()
//    await H.setUpDb()

//    return tiny
//       .transaction(() => {
//          return null
//       })
//       .then(() => expect.fail('this should not succeed'))
//       .catch(error => {
//          expect(error.message).to.contain('thennable')
//       })
// })

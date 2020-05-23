import * as Pg from "../../deno-postgres/mod.ts";
import { PoolClient } from "../../deno-postgres/client.ts";
import * as Uuid from "https://deno.land/std/uuid/mod.ts";
import * as Hash from "https://deno.land/std/hash/sha1.ts";
import Debug from "https://deno.land/x/debuglog/debug.ts";
import * as T from "./types.ts";
import * as E from "./errors.ts";
import * as Util from "./util.ts";
import * as Parser from "./parser.ts";

const log = Debug("tinypg");

const parseConnectionConfigFromUrlOrDefault = (
  connection_string?: string,
): any => {
  // TODO: add tls options parameter when Deno supports it
  const default_user = Deno.env.get("PGUSER") || "postgres";
  const default_password = Deno.env.get("PGPASSWORD") || undefined;
  const default_host = Deno.env.get("PGHOST") || "localhost";
  const default_database = Deno.env.get("PGDATABASE") || "postgres";
  const default_port = Deno.env.get("PGPORT") || "5432";
  const default_ssl = Deno.env.get("PGSSLMODE") || "disable";

  const url = connection_string
    ? new URL(connection_string.replace("postgres:", "http:"))
    : null;
  const user = url?.username || default_user;
  const password = url?.password || default_password;
  const port = parseInt(url?.port || default_port, 10);
  const database = url?.pathname.split("/")[1] || default_database;
  const enable_ssl = ["disable", "allow"].includes(
    url?.searchParams.get("sslmode") || default_ssl,
  );
  const host = url?.hostname || default_host;

  return {
    user: user,
    password: password,
    host: host,
    port: port,
    database: database,
    ssl: enable_ssl,
  };
};

export class TinyPg {
  public pool: Pg.Pool;
  public sql_db_calls: { [key: string]: DbCall };

  private hooks: T.TinyHooks[];
  private error_transformer: E.TinyPgErrorTransformer;
  private sql_files: T.SqlFile[];
  private options: T.TinyPgOptions;
  private transaction_id?: string;

  constructor(options: T.TinyPgOptions) {
    options = !options ? {} : options;

    this.error_transformer = options.error_transformer
      ? options.error_transformer
      : (x) => x;
    this.options = options;
    this.hooks = !this.options.hooks ? [] : [this.options.hooks];

    const pool_options = !options.pool_options ? {} : options.pool_options;

    const config_from_url = parseConnectionConfigFromUrlOrDefault(
      options.connection_string,
    );

    this.pool = new Pg.Pool(
      {
        ...config_from_url,
        keepAlive: pool_options.keep_alive,
        connectionTimeoutMillis: pool_options.connection_timeout_ms,
        idleTimeoutMillis: pool_options.idle_timeout_ms,
        application_name: pool_options.application_name,
        statement_timeout: pool_options.statement_timeout_ms,
        max: pool_options.max,
        min: pool_options.min,
        log: Debug("tinypg:pool"),
      },
      1,
      false,
    );

    const paths =
      (Array.isArray(options.root_dir) ? options.root_dir : [options.root_dir!])
        .filter(Boolean);
    this.sql_files = Parser.parseFiles(paths);

    const db_calls = this.sql_files.map((sql_file) => {
      return new DbCall({
        name: sql_file.name,
        key: sql_file.key,
        text: sql_file.text,
        parameterized_query: sql_file.parsed.parameterized_sql,
        parameter_map: sql_file.parsed.mapping,
        prepared: options.use_prepared_statements == null
          ? false
          : options.use_prepared_statements,
      });
    });

    this.sql_db_calls = db_calls.reduce((acc: { [k: string]: DbCall }, x) => {
      acc[x.config.key] = x;
      return acc;
    }, {});
  }

  query<T extends object = any, P extends T.TinyPgParams = T.TinyPgParams>(
    raw_sql: string,
    params?: P,
  ): Promise<T.Result<T>> {
    const query_id = Uuid.v4.generate();

    const hook_lifecycle = this.makeHooksLifeCycle();

    const [new_query, new_params] = hook_lifecycle.preRawQuery({
      query_id: query_id,
      transaction_id: this.transaction_id,
    }, [raw_sql, params!]).args;

    return Util.stackTraceAccessor(
      this.options.capture_stack_trace!,
      async () => {
        const parsed = Parser.parseSql(raw_sql);
        const key = new Hash.Sha1();
        key.update;
        const db_call = new DbCall({
          name: "raw_query",
          key: key.update(parsed.parameterized_sql).hex(),
          text: new_query,
          parameterized_query: parsed.parameterized_sql,
          parameter_map: parsed.mapping,
          prepared: false,
        });

        return await this.performDbCall(
          db_call,
          hook_lifecycle,
          new_params,
          query_id,
        );
      },
    );
  }

  sql<T extends object = any, P extends T.TinyPgParams = T.TinyPgParams>(
    name: string,
    params?: P,
  ): Promise<T.Result<T>> {
    const query_id = Uuid.v4.generate();

    const hook_lifecycle = this.makeHooksLifeCycle();

    const [, new_params] = hook_lifecycle.preSql({
      query_id: query_id,
      transaction_id: this.transaction_id,
    }, [name, params!]).args;

    return Util.stackTraceAccessor(
      this.options.capture_stack_trace!,
      async () => {
        log("sql", name);

        const db_call: DbCall = this.sql_db_calls[name];

        if (!db_call) {
          throw new Error(`Sql query with name [${name}] not found!`);
        }

        return this.performDbCall(
          db_call,
          hook_lifecycle,
          new_params,
          query_id,
        );
      },
    );
  }

  transaction<T = any>(tx_fn: (db: TinyPg) => Promise<T>): Promise<T> {
    const transaction_id = Uuid.v4.generate();

    const hook_lifecycle = this.makeHooksLifeCycle();

    hook_lifecycle.preTransaction(transaction_id);

    return Util.stackTraceAccessor(
      this.options.capture_stack_trace!,
      async () => {
        log("transaction");

        const tx_client = await this.getClient();

        const release_ref = tx_client.release;
        tx_client.release = () => Promise.resolve();

        const release = () => {
          log("RELEASE transaction client");
          tx_client.release = release_ref;
          tx_client.release();
        };

        try {
          log("BEGIN transaction");

          await tx_client.query("BEGIN");

          hook_lifecycle.onBegin(transaction_id);

          const tiny_tx: TinyPg = Object.create(this);

          tiny_tx.transaction_id = transaction_id;

          const assertThennable = (tx_fn_result: any) => {
            if (tx_fn_result == null || tx_fn_result.then == null) {
              throw new Error(
                "Expected thennable to be returned from transaction function.",
              );
            }

            return tx_fn_result;
          };

          tiny_tx.transaction = <T = any>(
            tx_fn: (db: TinyPg) => Promise<T>,
          ): Promise<T> => {
            log("inner transaction");
            return assertThennable(tx_fn(tiny_tx));
          };

          tiny_tx.getClient = async () => {
            log("getClient (transaction)");
            return tx_client;
          };

          const result = await assertThennable(tx_fn(tiny_tx));

          log("COMMIT transaction");

          await tx_client.query("COMMIT");

          hook_lifecycle.onCommit(transaction_id);

          return result;
        } catch (error) {
          log("ROLLBACK transaction");

          await tx_client.query("ROLLBACK");

          hook_lifecycle.onRollback(transaction_id, error);

          throw error;
        } finally {
          release();
        }
      },
    );
  }

  withHooks(hooks: T.TinyHooks): TinyPg {
    const new_tiny = Object.create(this) as TinyPg;

    new_tiny.hooks = [...new_tiny.hooks, hooks];

    return new_tiny;
  }

  makeHooksLifeCycle(): Required<T.TinyHookLifecycle> {
    const hooks_to_run: T.HookSetWithContext[] = this.hooks.map((hook_set) => {
      return { ctx: null, transaction_ctx: null, hook_set: hook_set };
    });

    const preHook = (
      fn_name: "preSql" | "preRawQuery",
      ctx: T.TinyCallContext,
      args: [string, T.TinyPgParams],
    ): T.HookResult<[string, T.TinyPgParams]> => {
      return hooks_to_run.reduce(
        (last_result, hook_set_with_ctx) => {
          const hook_fn: any = hook_set_with_ctx.hook_set[fn_name];

          if (!hook_fn) {
            return last_result;
          }

          const [name_or_query, params] = last_result.args;

          const result = hook_fn(ctx, name_or_query, params);

          hook_set_with_ctx.ctx = result.ctx;

          return result;
        },
        { args: args, ctx: ctx },
      );
    };

    const dbCallHook = (
      fn_name: "onSubmit" | "onQuery" | "onResult",
      query_context:
        | T.QuerySubmitContext
        | T.QueryBeginContext
        | T.QueryCompleteContext,
    ): void => {
      hooks_to_run.forEach((hook_set_with_ctx) => {
        const hook_fn: any = hook_set_with_ctx.hook_set[fn_name];

        if (!hook_fn) {
          return;
        }

        try {
          hook_set_with_ctx.ctx = hook_fn(
            hook_set_with_ctx.ctx,
            <any> query_context,
          );
        } catch (error) {
          log(`${fn_name} hook error`, error);
        }
      });
    };

    const transactionHook = (
      fn_name: "preTransaction" | "onBegin" | "onCommit" | "onRollback",
      transaction_id: string,
      transaction_error?: Error,
    ) => {
      hooks_to_run.forEach((hook_set_with_ctx) => {
        const hook_fn: any = hook_set_with_ctx.hook_set[fn_name];

        if (!hook_fn) {
          return;
        }

        try {
          hook_set_with_ctx.transaction_ctx = fn_name === "preTransaction"
            ? hook_fn(transaction_id)
            : hook_fn(
              hook_set_with_ctx.transaction_ctx,
              transaction_id,
              transaction_error,
            );
        } catch (error) {
          log(`${fn_name} hook error`, error);
        }
      });
    };

    return {
      preSql: (ctx: T.TinyCallContext, args) => {
        return preHook("preSql", ctx, args);
      },
      preRawQuery: (ctx: T.TinyCallContext, args) => {
        return preHook("preRawQuery", ctx, args);
      },
      onSubmit: (query_submit_context: T.QuerySubmitContext) => {
        dbCallHook("onSubmit", query_submit_context);
      },
      onQuery: (query_begin_context: T.QueryBeginContext) => {
        dbCallHook("onQuery", query_begin_context);
      },
      onResult: (query_complete_context: T.QueryCompleteContext) => {
        dbCallHook("onResult", query_complete_context);
      },
      preTransaction: (transaction_id: string) => {
        transactionHook("preTransaction", transaction_id);
      },
      onBegin: (transaction_id: string) => {
        transactionHook("onBegin", transaction_id);
      },
      onCommit: (transaction_id: string) => {
        transactionHook("onCommit", transaction_id);
      },
      onRollback: (transaction_id: string, transaction_error: Error) => {
        transactionHook("onRollback", transaction_id, transaction_error);
      },
    };
  }

  close(): Promise<void> {
    return this.pool.end();
  }

  getClient(): Promise<PoolClient> {
    log(`getClient [total=${this.pool.maxSize},idle=${this.pool.available}]`);
    return this.pool.connect();
  }

  async performDbCall<
    T extends object = any,
    P extends T.TinyPgParams = T.TinyPgParams,
  >(
    db_call: DbCall,
    hooks: Required<T.TinyHookLifecycle>,
    params?: P,
    query_id?: string,
  ): Promise<T.Result<T>> {
    log("performDbCall", db_call.config.name);
    const start_at = Date.now();

    const begin_context: T.QueryBeginContext = {
      id: !query_id ? Uuid.v4.generate() : query_id,
      sql: db_call.config.parameterized_query,
      start: start_at,
      name: db_call.config.name,
      params: params,
    };

    let submit_context: T.QuerySubmitContext | null = null;

    const query_promise = async (): Promise<T.Result<T>> => {
      const client = await this.getClient();

      try {
        hooks.onQuery(begin_context);

        log("executing", db_call.config.name);

        const values: any[] = db_call.config.parameter_map.map((m) => {
          const value = Util.get(params, m.name);
          if (value === undefined) {
            throw new Error(
              `Missing expected key [${m.name}] on input parameters.`,
            );
          }
          return value;
        });

        const submitted_at = Date.now();
        submit_context = {
          ...begin_context,
          submit: submitted_at,
          wait_duration: submitted_at - begin_context.start,
        };
        hooks.onSubmit(submit_context);

        const result = db_call.config.prepared
          ? await client.query({
            name: db_call.prepared_name,
            text: db_call.config.parameterized_query,
            args: values,
          })
          : await client.query(db_call.config.parameterized_query, values);

        log("execute result", db_call.config.name);

        return {
          row_count: result.rows.length,
          rows: result.rows,
          command: result.query.text,
        };
      } finally {
        await client.release();
      }
    };

    const createCompleteContext = (
      error: any,
      data: T.Result<T> | null,
    ): T.QueryCompleteContext => {
      const end_at = Date.now();
      const query_duration = end_at - start_at;

      const submit_timings = !submit_context
        ? {
          submit: undefined,
          wait_duration: query_duration,
          active_duration: 0,
        }
        : {
          submit: submit_context.submit,
          wait_duration: submit_context.wait_duration,
          active_duration: end_at - submit_context.submit,
        };

      return {
        ...begin_context,
        ...submit_timings,
        end: end_at,
        duration: query_duration,
        error: error,
        data: data,
      };
    };

    const emitQueryComplete = (complete_context: T.QueryCompleteContext) => {
      hooks.onResult(complete_context);
    };

    try {
      const data = await query_promise();

      emitQueryComplete(createCompleteContext(null, data));

      return data;
    } catch (e) {
      const tiny_stack =
        `[${db_call.config.name}]\n\n${db_call.config.text}\n\n${e.stack}`;
      const complete_context = createCompleteContext(e, null);

      emitQueryComplete(complete_context);

      const tiny_error = new E.TinyPgError(
        `${e.message}`,
        tiny_stack,
        complete_context,
      );

      throw this.error_transformer(tiny_error);
    }
  }
}

export class DbCall {
  config: T.DbCallConfig;
  prepared_name?: string;

  constructor(config: T.DbCallConfig) {
    this.config = config;

    if (this.config.prepared) {
      const hash_code = Util.hashCode(config.parameterized_query).toString()
        .replace("-", "n");
      this.prepared_name = `${config.name}_${hash_code}`.substring(0, 63);
    }
  }
}

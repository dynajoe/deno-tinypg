export type TinyPgErrorTransformer = (error: TinyPgError) => any;

export class TinyPgError extends Error {
  name: string;
  message: string;
  queryContext: any;

  constructor(message: string, stack?: string, query_context?: any) {
    super(message);

    Object.setPrototypeOf(this, new.target.prototype);

    this.stack = stack;
    this.name = this.constructor.name;
    this.message = message;
    this.queryContext = query_context;
  }
}

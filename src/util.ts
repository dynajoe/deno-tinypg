export function hashCode(str: string): number {
   let hash = 0

   if (str.length == 0) {
      return hash
   }

   for (let i = 0, l = str.length; i < l; i++) {
      const ch = str.charCodeAt(i)
      hash = (hash << 5) - hash + ch
      hash |= 0
   }

   return hash
}

export async function stackTraceAccessor<T>(is_enabled: boolean, fn: () => Promise<T>): Promise<T> {
   if (!is_enabled) {
      return fn()
   }

   const stack_trace_error = new Error(`TinyPg Captured Stack Trace`)

   try {
      return await fn()
   } catch (error) {
      error.stack = `${error.stack ? `${error.stack}\nFrom: ` : ''}${stack_trace_error.stack}`
      throw error
   }
}

// https://github.com/you-dont-need/You-Dont-Need-Lodash-Underscore#_get
export function get(obj: any, path: string, defaultValue: any = undefined): any {
   const travel = (regexp: RegExp) =>
     String.prototype.split
       .call(path, regexp)
       .filter(Boolean)
       .reduce((res, key) => (res !== null && res !== undefined ? res[key] : res), obj);
   const result = travel(/[,[\]]+?/) || travel(/[,[\].]+?/);
   return result === undefined || result === obj ? defaultValue : result;
 };

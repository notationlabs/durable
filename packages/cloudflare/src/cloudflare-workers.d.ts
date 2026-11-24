// The part of `cloudflare:workers` that this package uses. A Worker's own
// types declare the whole module, and the published types import from it.
declare module "cloudflare:workers" {
  export abstract class DurableObject<Env = unknown> {
    protected ctx: import("./storage").DurableObjectStateLike;
    protected env: Env;
    constructor(ctx: import("./storage").DurableObjectStateLike, env: Env);
  }
}

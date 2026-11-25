declare module "cloudflare:workers" {
  // The part of the class that this package uses. A Worker's own types
  // declare the whole module, and the published types import from it.
  export abstract class DurableObject<Env = unknown> {
    protected ctx: import("./storage").DurableObjectStateLike;
    protected env: Env;
    constructor(ctx: import("./storage").DurableObjectStateLike, env: Env);
  }
}

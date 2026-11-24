import {
  CasStoreClient,
  InfrastructureError,
  Runtime,
  decodeThrownValue,
  durableValueCodec,
  encodeThrownValue,
  prepareEffectBegin,
  prepareEffectSettle,
  prepareEffectWatch,
  prepareExecutionStart,
  type EffectClient,
  type ExecutionClient,
  type HeapClient,
  type JoinClient,
  type RuntimeOptions,
  type StoreDefinition,
  type StoreDeletion,
  type StoreMutation,
  type StoreMutationCommitResult,
  type StoreSnapshot,
  type StoreStepId,
  type StoreWaiter,
} from "@durable/core";

// The methods a delivery calls. The store client's are the compare-and-swap
// methods of `CasStoreClient`, which the SQLite store client declares protected.
const METHODS = {
  executions: ["start"],
  effects: ["begin", "settle", "watch", "getEffect"],
  heap: ["readStep", "writeStep", "recordCancellation"],
  joins: ["join"],
  stores: [
    "readStore",
    "insertStore",
    "getAppliedStoreStep",
    "commitStoreMutation",
    "commitStoreDeletion",
    "registerWaiter",
    "listStores",
    "deleteStore",
  ],
} as const;

type ClientName = keyof typeof METHODS;

export type RuntimeReply =
  | { ok: true; value: string }
  | { ok: false; error: string; infrastructure: boolean };

/**
 * Calls one method of one runtime client. `args` and the reply's `value` are
 * durable value encodings, and the reply's `error` is an `encodeThrownValue`
 * encoding, so values and errors keep their class across isolates.
 */
export type RuntimeCall = (
  client: ClientName,
  method: string,
  args: string
) => Promise<RuntimeReply>;

function encode(value: unknown, what: string): string {
  const encoding = durableValueCodec.encode(value);
  if (encoding.status === "invalid") {
    throw new Error(`${what} is not a durable value: ${encoding.message}`);
  }
  return encoding.canonical;
}

function decode(canonical: string, what: string): unknown {
  const decoding = durableValueCodec.decode(canonical);
  if (decoding.status === "invalid") {
    throw new Error(`${what} has an invalid encoding: ${decoding.message}`);
  }
  return decoding.value;
}

/** Returns the function that a remote runtime calls to reach `runtime`. */
export function serveRuntime(runtime: Runtime): RuntimeCall {
  return async (client, method, args) => {
    try {
      const name = `${client}.${method}`;
      const methods: readonly string[] | undefined = Object.hasOwn(METHODS, client)
        ? METHODS[client]
        : undefined;
      if (!methods?.includes(method)) {
        throw new Error(`A delivery cannot call "${name}" on the runtime`);
      }
      const target = runtime[client] as unknown as Record<
        string,
        (...args: unknown[]) => unknown
      >;
      const value = await target[method](...(decode(args, `The call to ${name}`) as unknown[]));
      return { ok: true, value: encode(value, `The result of ${name}`) };
    } catch (err) {
      return {
        ok: false,
        error: encodeThrownValue(err),
        infrastructure: InfrastructureError.is(err),
      };
    }
  };
}

async function invoke(call: RuntimeCall, client: ClientName, method: string, args: unknown[]) {
  const name = `${client}.${method}`;
  const reply = await call(client, method, encode(args, `The call to ${name}`));
  if (reply.ok) return decode(reply.value, `The result of ${name}`);
  const error = decodeThrownValue(reply.error);
  throw reply.infrastructure ? new InfrastructureError(error) : error;
}

// A method that no delivery calls rejects, so a new use fails where it is made.
function remoteClient<Client extends object>(
  call: RuntimeCall,
  client: ClientName,
  methods: Partial<Client> = {}
): Client {
  const allowed: readonly string[] = METHODS[client];
  return new Proxy(methods, {
    get: (target, method, receiver) => {
      if (typeof method !== "string" || Reflect.has(target, method)) {
        return Reflect.get(target, method, receiver);
      }
      // A promise resolved with the client must not treat it as a thenable.
      if (method === "then") return undefined;
      if (allowed.includes(method)) {
        return (...args: unknown[]) => invoke(call, client, method, args);
      }
      return async () => {
        throw new Error(`A delivery cannot call "${client}.${method}" on the runtime`);
      };
    },
  }) as Client;
}

// A definition holds a schema and an `initial` function. The runtime's store
// client reads only its name.
const named = (definition: StoreDefinition) => ({ name: definition.name });

/**
 * Runs updaters, selectors, claims and `initial` in this isolate, and sends
 * each read and compare-and-swap to the runtime that `call` reaches.
 */
class RemoteStoreClient extends CasStoreClient {
  constructor(private call: RuntimeCall) {
    super();
  }

  private send<Result>(method: string, ...args: unknown[]) {
    return invoke(this.call, "stores", method, args) as Promise<Result>;
  }

  protected readStore(params: { definition: StoreDefinition; id: string }) {
    return this.send<StoreSnapshot<unknown> | undefined>("readStore", {
      ...params,
      definition: named(params.definition),
    });
  }

  protected insertStore(params: {
    definition: StoreDefinition;
    id: string;
    snapshot: StoreSnapshot<unknown>;
  }) {
    return this.send<StoreSnapshot<unknown>>("insertStore", {
      ...params,
      definition: named(params.definition),
    });
  }

  protected getAppliedStoreStep(params: {
    definition: StoreDefinition;
    id: string;
    stepId: StoreStepId;
  }) {
    return this.send<{ result: unknown } | undefined>("getAppliedStoreStep", {
      ...params,
      definition: named(params.definition),
    });
  }

  protected commitStoreMutation(mutation: StoreMutation) {
    return this.send<StoreMutationCommitResult>("commitStoreMutation", {
      ...mutation,
      definition: named(mutation.definition),
    });
  }

  protected commitStoreDeletion(deletion: StoreDeletion) {
    return this.send<StoreMutationCommitResult>("commitStoreDeletion", {
      ...deletion,
      definition: named(deletion.definition),
    });
  }

  registerWaiter(waiter: StoreWaiter) {
    return this.send<void>("registerWaiter", waiter);
  }

  listStores(definition: StoreDefinition) {
    return this.send<string[]>("listStores", named(definition));
  }

  deleteStore(params: { definition: StoreDefinition; id: string }) {
    return this.send<void>("deleteStore", {
      ...params,
      definition: named(params.definition),
    });
  }
}

/**
 * A runtime whose clients forward each call through `call`, for a delivery
 * that runs in a different isolate from the runtime's database. It has the
 * methods a delivery calls: claiming, acknowledging and the other methods of
 * a dispatcher or an SDK reject.
 */
export class RemoteRuntime extends Runtime {
  readonly clock: () => number;
  readonly executions: ExecutionClient;
  readonly effects: EffectClient;
  readonly heap: HeapClient;
  readonly joins: JoinClient;
  readonly stores: CasStoreClient;

  constructor(call: RuntimeCall, options: RuntimeOptions = {}) {
    super();
    this.clock = options.clock ?? (() => Date.now());
    const send = <Result>(client: ClientName, method: string, request: unknown) =>
      invoke(call, client, method, [request]) as Promise<Result>;
    // The runtime's clients reject an invalid request before they read the
    // database. A request that is invalid may not survive encoding, so the
    // same checks run here first, and give the same result.
    this.executions = remoteClient<ExecutionClient>(call, "executions", {
      async start(request) {
        const prepared = prepareExecutionStart(request);
        return prepared.ok ? send("executions", "start", request) : prepared.result;
      },
    });
    this.effects = remoteClient<EffectClient>(call, "effects", {
      async begin(request) {
        prepareEffectBegin(request);
        return send("effects", "begin", request);
      },
      async settle(request) {
        prepareEffectSettle(request);
        return send("effects", "settle", request);
      },
      async watch(request) {
        prepareEffectWatch(request);
        return send("effects", "watch", request);
      },
    });
    this.heap = remoteClient<HeapClient>(call, "heap");
    this.joins = remoteClient<JoinClient>(call, "joins");
    this.stores = new RemoteStoreClient(call);
  }
}

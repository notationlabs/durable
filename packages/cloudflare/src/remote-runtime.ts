import {
  CasStoreClient,
  InfrastructureError,
  Runtime,
  decodeThrownValue,
  durableValueCodec,
  encodeThrownValue,
  isLeaseLost,
  prepareEffectBegin,
  prepareEffectSettle,
  prepareEffectWatch,
  prepareExecutionStart,
  type EffectClient,
  type ExecutionClient,
  type HeapClient,
  type HeapRecord,
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

export type RuntimeReply = { ok: true; value: string } | ({ ok: false } & EncodedError);

/**
 * An `encodeThrownValue` encoding. For an `InfrastructureError`, which the
 * codec would decode as a plain `Error`, `error` encodes its cause and
 * `infrastructure` is true.
 */
export type EncodedError = { error: string; infrastructure: boolean };

export function encodeError(error: unknown): EncodedError {
  return InfrastructureError.is(error)
    ? { error: encodeThrownValue(error.cause), infrastructure: true }
    : { error: encodeThrownValue(error), infrastructure: false };
}

export function decodeError(encoded: EncodedError): Error {
  const error = decodeThrownValue(encoded.error);
  return encoded.infrastructure ? new InfrastructureError(error) : error;
}

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
      return { ok: false, ...encodeError(err) };
    }
  };
}

async function invoke(call: RuntimeCall, client: ClientName, method: string, args: unknown[]) {
  const name = `${client}.${method}`;
  const reply = await call(client, method, encode(args, `The call to ${name}`));
  if (reply.ok) return decode(reply.value, `The result of ${name}`);
  throw decodeError(reply);
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

/**
 * The latest record of each step of one execution when its delivery started,
 * by step key. `null` stands for a record that was too large to send, which
 * the delivery reads from the runtime. A step without an entry has no record.
 */
export type StepRecords = Record<string, HeapRecord | null>;

/**
 * Reads the steps of one execution from `records` and from what the delivery
 * has written since, without a call. This is correct only for the delivery
 * that holds the execution's lease: every write to the execution's records
 * carries that lease, so no other writer changes them during the delivery.
 */
function cachedHeap(remote: HeapClient, executionId: string, records: StepRecords): HeapClient {
  const known = new Map<string, HeapRecord | null>(Object.entries(records));
  return {
    async readStep(params) {
      if (params.executionId !== executionId) return remote.readStep(params);
      const record = known.get(params.stepKey);
      if (record === undefined) return null;
      if (record !== null) return record;
      const read = await remote.readStep(params);
      if (read) known.set(params.stepKey, read);
      else known.delete(params.stepKey);
      return read;
    },
    async writeStep(params) {
      const written = await remote.writeStep(params);
      if (params.lease.executionId === executionId && !isLeaseLost(written)) {
        const latest = known.get(params.stepKey);
        // `readStep` returns the latest attempt. Without the record that was
        // too large to send, the latest attempt is not known here.
        if (latest === undefined || (latest && latest.meta.attempt <= written.meta.attempt)) {
          known.set(params.stepKey, written);
        }
      }
      return written;
    },
    async recordCancellation(params) {
      const result = await remote.recordCancellation(params);
      // The call wrote the records or found conflicting ones. Either way the
      // next read of these steps comes from the runtime.
      if (params.lease.executionId === executionId) {
        for (const { stepKey } of params.steps) known.set(stepKey, null);
      }
      return result;
    },
  } as HeapClient;
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

  constructor(
    call: RuntimeCall,
    options: RuntimeOptions & {
      /** The records of the execution that the delivery runs. */
      steps?: { executionId: string; records: StepRecords };
    } = {}
  ) {
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
    const heap = remoteClient<HeapClient>(call, "heap");
    const { steps } = options;
    this.heap = steps ? cachedHeap(heap, steps.executionId, steps.records) : heap;
    this.joins = remoteClient<JoinClient>(call, "joins");
    this.stores = new RemoteStoreClient(call);
  }
}

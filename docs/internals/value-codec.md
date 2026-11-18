# Value Codec & Errors

Notation Durable serializes workflow inputs, step results, store snapshots, effect payloads, and thrown errors through `durableValueCodec` in `packages/core/src/base/durable-value.ts`. Canonical encoding ensures structurally equal values produce byte-exact JSON across processes, while decoding revives built-in types and registered `Error` classes without invoking constructors.

## Canonical JSON format

`durableValueCodec.encode(value)` validates `value` recursively and returns `{ status: "encoded", canonical }` or `{ status: "invalid", message }`. Three normalization rules make effect inputs and idempotent triggers compare identically by string equality:

- **UTF-16 object key order** – Own keys sort lexicographically by UTF-16 code unit rather than insertion or integer-index order.
- **Negative zero** – `-0` encodes as `"0"` via `JSON.stringify`.
- **`$type` escaping** – Plain objects with an own `"$type"` key wrap as `{"$type":"object","value":...}` so user objects never collide with codec tags.

Arrays, `Map` entries, and `Set` values preserve insertion order, so maps or sets with identical items in different insertion orders produce different canonical strings.

## Tagged encodings

Values without a native JSON representation encode as sorted `$type` tag objects:

| Value | Canonical JSON |
| :--- | :--- |
| Undefined | `{"$type":"undefined"}` |
| BigInt | `{"$type":"bigint","value":"-12"}` |
| Date | `{"$type":"date","value":"2026-10-29T12:00:00.123Z"}` |
| Map | `{"$type":"map","entries":[["a",1]]}` |
| Set | `{"$type":"set","values":[3,1,"x"]}` |
| Error | `{"$type":"error","message":"boom","name":"Error"}` |
| Object with `$type` | `{"$type":"object","value":{"$type":"custom"}}` |

`durableValueCodec.decode` and `decodeParsedValue` enforce strict tag schemas. They reject missing or extra tag keys, malformed dates, invalid bigint strings, and duplicate map keys or set elements with `DurableValueError`. Decoded objects assign keys through `Object.defineProperty`, preserving any own `"__proto__"` key as regular data.

## Compile-time vs runtime enforcement

`Durable<T>` and `IfDurable<T, Otherwise>` check workflow and store shapes at compile time – keeping interfaces, tuples, and array mutability intact while mapping functions to `never` – and `durableValueCodec.encode` enforces remaining rules at runtime:

| Rejected input | Runtime failure condition |
| :--- | :--- |
| Non-finite number | Value is `NaN`, `Infinity`, or `-Infinity` |
| Non-plain instance | Class instance other than `Array`, `Date`, `Map`, `Set`, or `Error` |
| Reference cycle | Object appears in the active ancestor path |
| Accessor or hidden key | Own property is a getter, setter, non-enumerable, or symbol |
| Sparse or extra-key array | Missing element index or non-index own property |
| Built-in subclass or own keys | `Array`, `Date`, `Map`, or `Set` is subclassed or has own properties |
| Invalid date | `Date` timestamp is `NaN` |

Shared sibling references to the same acyclic object succeed. Property descriptors are read with `Object.getOwnPropertyDescriptor` so accessor properties reject without invoking getters.

## Step copy isolation & error revival

Step persistence routes every `StepResponse` through `encodeStepRecord` in `packages/sdk/src/internal/step-record.ts`, which encodes the response and immediately decodes it back before returning to workflow code:

```ts [packages/sdk/src/internal/step-record.ts]
export function encodeStepRecord(
  response: StepResponse,
  stepKey: string
): { json: string; response: StepResponse } {
  const json = encodeResponse(response, stepKey);
  return { json, response: decodeStepRecord(json) };
}
```

Returning the re-decoded response guarantees that a step's first run receives the exact same copy, property descriptors, and error prototypes that replay later reads from storage. If encoding a step or workflow result fails, `encodeStepRecord` records a step error wrapping `WorkflowValueError` with code `"INVALID_STEP_VALUE"` under `stepKey` or `WORKFLOW_RESULT_STEP_KEY`.

For thrown values, `encodeThrownValue` encodes the value directly when durable, or falls back to encoding an `Error` stringified from that value. Error encoding keeps standard metadata, causes, aggregate error lists, and durable own enumerable fields – omitting any cyclic or non-durable field rather than failing the step.

```ts [packages/core/src/base/durable-value.ts]
const errorClass = registeredErrorClass(name as string) ?? Error;
const error: Error = Reflect.construct(Error, [], errorClass);
```

Decoding revives errors via `Reflect.construct`, attaching the registered prototype without running constructors that require arguments or side effects. Own properties are restored directly onto the instance; if no stack trace was encoded, the empty trace is deleted rather than fabricating a new one.

Built-in JavaScript errors, core workflow errors, `StoreValidationError`, and `RetryableError` register automatically at startup. Unregistered names revive as plain `Error` instances with their recorded properties intact; call `registerErrorClass` in every worker, server, and client process that needs `instanceof` checks (see [Errors](../reference/errors.md)).

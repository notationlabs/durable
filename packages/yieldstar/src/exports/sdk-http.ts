import type {
  CancelOptions,
  CancelResult,
  ClearDivergenceResult,
  EventParamsOf,
  ExecutionAdmissionErrorCode,
  ExecutionControl,
  ExecutionListRequest,
  ExecutionObserver,
  ExecutionView,
  ExecutionViewPage,
  ExecutionWaitOptions,
  TriggerAck,
  TriggerEvent,
  WaitResult,
  WorkflowGeneratorReturnType,
  WorkflowRouter,
} from "@yieldstar/core";
import {
  ExecutionAdmissionError,
  ExecutionCancelRequestError,
  ExecutionListRequestError,
  WorkflowCancelledError,
  durableValueCodec,
  prepareExecutionCancel,
} from "@yieldstar/core";
import { HttpRouteError } from "./errors";

type TriggerResponse<W extends WorkflowRouter, K extends string & keyof W> = TriggerAck & {
  ack: () => Promise<TriggerAck>;
  waitForResult: () => Promise<WorkflowGeneratorReturnType<W[K]>>;
};

const admissionErrorCodes: ExecutionAdmissionErrorCode[] = [
  "EXECUTION_ID_CONFLICT",
  "INVALID_EXECUTION_EVENT",
  "EXECUTION_REQUEST_MISMATCH",
  "IDEMPOTENT_TRIGGER_REQUIRES_ID",
];

export function createHttpSdkFactory<W extends WorkflowRouter>() {
  return (params: {
    url: string;
    fetchOptions?: Omit<RequestInit, "body" | "method">;
  }) => {
    const post = (route: string, body: string) => {
      const headers = new Headers(params.fetchOptions?.headers);
      headers.set("content-type", "application/json");
      return fetch(`${params.url}/${route}`, {
        ...params.fetchOptions,
        method: "POST",
        headers,
        body,
      });
    };

    const decode = async (res: Response): Promise<unknown> => {
      const decoding = durableValueCodec.decode(await res.text());
      if (decoding.status === "invalid") {
        throw new HttpRouteError(
          res.status,
          undefined,
          `Invalid response body: ${decoding.message}`
        );
      }
      return decoding.value;
    };

    /**
     * Decodes a `/poll`, `/wait`, `/cancel` or `/clear-divergence` response. Only a `404` with code
     * `EXECUTION_NOT_FOUND` means the execution does not exist; any other
     * `404`, such as one from a wrong base path, throws.
     */
    const decodeObservation = async <T>(res: Response): Promise<T | undefined> => {
      if (res.ok) return (await decode(res)) as T;
      const error = await routeError(res);
      if (res.status === 404 && error.code === "EXECUTION_NOT_FOUND") return undefined;
      throw error;
    };

    /** One `/wait` request. The server caps how long it is held open. */
    const waitRequest = async (
      executionId: string,
      timeout: number | undefined
    ): Promise<WaitResult | undefined> => {
      const res = await post(
        "wait",
        JSON.stringify(timeout === undefined ? { executionId } : { executionId, timeout })
      );
      return decodeObservation<WaitResult>(res);
    };

    const sdk = {
      /**
       * Admits an execution through the trigger route, and resolves once
       * admission commits. The server chooses the ID when `executionId` is
       * omitted, and the response carries it. With `idempotent: true`, a
       * repeat of the same request resolves with `status: "existing"`. The
       * body is encoded with the durable value codec, so params and context
       * reach the runtime unchanged.
       * @throws {ExecutionAdmissionError} when the route rejects the
       * admission, or before any request when the codec rejects params or
       * context
       * @throws {HttpRouteError} for any other error response
       */
      async trigger<K extends string & keyof W>(
        event: TriggerEvent<K, EventParamsOf<W[K]>>
      ): Promise<TriggerResponse<W, K>> {
        const request: Record<string, unknown> = { workflowId: event?.workflowId };
        if (event?.executionId !== undefined) request.executionId = event.executionId;
        if (event && "params" in event) request.params = event.params;
        if (event?.context !== undefined) request.context = event.context;
        if (event?.idempotent !== undefined) request.idempotent = event.idempotent;
        const encoding = durableValueCodec.encode(request);
        if (encoding.status === "invalid") {
          throw new ExecutionAdmissionError(
            "INVALID_EXECUTION_EVENT",
            `Event cannot be stored without loss: ${encoding.message}`
          );
        }
        const res = await post("trigger", encoding.canonical);

        if (!res.ok) {
          const error = await routeError(res);
          if (admissionErrorCodes.includes(error.code as ExecutionAdmissionErrorCode)) {
            throw new ExecutionAdmissionError(
              error.code as ExecutionAdmissionErrorCode,
              error.message
            );
          }
          throw error;
        }

        const body = (await res.json()) as TriggerAck;
        const ack: TriggerAck = { executionId: body.executionId, status: body.status };
        return {
          ...ack,
          ack: async () => ack,
          waitForResult: () => sdk.waitForResult<K>(ack),
        };
      },

      /**
       * The current view of an execution, or `undefined` if none exists.
       * @throws {HttpRouteError} for an error response other than `404`
       * `EXECUTION_NOT_FOUND`
       */
      async poll(executionId: string): Promise<ExecutionView | undefined> {
        const res = await post("poll", JSON.stringify({ executionId }));
        return decodeObservation<ExecutionView>(res);
      },

      /**
       * Waits for a terminal outcome. The server caps each `/wait` request,
       * so this repeats requests until the execution is terminal or
       * `timeout` passes. Resolves with `timed-out` and the current view when
       * `timeout` passes first, and with `undefined` for an unknown ID.
       * @throws {RangeError} for a negative timeout
       * @throws {HttpRouteError} for an error response other than `404`
       * `EXECUTION_NOT_FOUND`
       */
      async wait(
        executionId: string,
        options?: ExecutionWaitOptions
      ): Promise<WaitResult | undefined> {
        const timeout = options?.timeout ?? Infinity;
        if (typeof timeout !== "number" || Number.isNaN(timeout) || timeout < 0) {
          throw new RangeError("timeout must be a non-negative number of milliseconds");
        }
        const deadline = Date.now() + timeout;
        while (true) {
          const remaining = Math.max(0, deadline - Date.now());
          const result = await waitRequest(
            executionId,
            remaining === Infinity ? undefined : remaining
          );
          if (result?.status !== "timed-out" || Date.now() >= deadline) {
            return result;
          }
        }
      },

      /**
       * Pages through executions, newest first by admission.
       * @throws {ExecutionListRequestError} for an invalid limit, status or cursor
       * @throws {HttpRouteError} for any other error response
       */
      async list(request: ExecutionListRequest = {}): Promise<ExecutionViewPage> {
        const res = await post("list", JSON.stringify(request));
        if (!res.ok) {
          const error = await routeError(res);
          if (error.code === "INVALID_LIST_REQUEST") {
            throw new ExecutionListRequestError(error.message);
          }
          throw error;
        }
        return (await decode(res)) as ExecutionViewPage;
      },

      /**
       * Records a cancellation request through the cancel route, and returns
       * without waiting for the workflow to stop. Resolves with `requested`
       * and the current view, with the terminal view when the execution is
       * terminal (including after a forced cancel), and with `undefined` for
       * an unknown ID.
       * @throws {ExecutionCancelRequestError} before any request for an empty
       * ID, or a `reason` or `force` of the wrong type
       * @throws {HttpRouteError} for an error response other than `404`
       * `EXECUTION_NOT_FOUND`
       */
      async cancel(
        executionId: string,
        options?: CancelOptions
      ): Promise<CancelResult | undefined> {
        const prepared = prepareExecutionCancel({ ...options, executionId });
        if (!prepared.ok) throw new ExecutionCancelRequestError(prepared.message);
        const res = await post(
          "cancel",
          JSON.stringify({ executionId, ...prepared.request })
        );
        return decodeObservation<CancelResult>(res);
      },

      /**
       * Removes a diverged execution's `divergence` through the
       * clear-divergence route, and makes it due now. Resolves with
       * `not-diverged` when there is nothing to clear, and with `undefined`
       * for an unknown ID.
       * @throws {TypeError} before any request for an empty ID
       * @throws {HttpRouteError} for an error response other than `404`
       * `EXECUTION_NOT_FOUND`
       */
      async clearDivergence(executionId: string): Promise<ClearDivergenceResult | undefined> {
        if (typeof executionId !== "string" || executionId === "") {
          throw new TypeError("executionId must be a non-empty string");
        }
        const res = await post("clear-divergence", JSON.stringify({ executionId }));
        return decodeObservation<ClearDivergenceResult>(res);
      },

      /**
       * `wait` without a timeout. Resolves with the result, or throws the
       * recorded error, revived as the class registered under its name.
       * @throws {WorkflowCancelledError} with the outcome's `reason` and
       * `source` when the execution ends `cancelled`
       * @throws {HttpRouteError} with code `EXECUTION_NOT_FOUND` for an
       * unknown ID
       */
      async waitForResult<K extends string & keyof W>(
        ack: Pick<TriggerAck, "executionId">
      ): Promise<WorkflowGeneratorReturnType<W[K]>> {
        const result = await sdk.wait(ack.executionId);
        if (!result) {
          throw new HttpRouteError(
            404,
            "EXECUTION_NOT_FOUND",
            `Execution "${ack.executionId}" does not exist`
          );
        }
        if (result.status === "failed") throw result.error;
        if (result.status === "cancelled") {
          throw new WorkflowCancelledError({ reason: result.reason, source: result.source });
        }
        if (result.status === "timed-out") {
          throw new Error(
            `Execution "${ack.executionId}" is still ${result.execution.status}`
          );
        }
        return result.result as WorkflowGeneratorReturnType<W[K]>;
      },
    };
    return sdk satisfies ExecutionObserver & ExecutionControl;
  };
}

async function routeError(res: Response): Promise<HttpRouteError> {
  const body = (await res.json().catch(() => undefined)) as
    | { code?: unknown; message?: unknown }
    | undefined;
  return new HttpRouteError(
    res.status,
    typeof body?.code === "string" ? body.code : undefined,
    typeof body?.message === "string" ? body.message : res.statusText
  );
}

import { registerErrorClass } from "@durable/core";

export class RetryableError extends Error {
  maxAttempts: number;
  retryInterval: number;
  constructor(
    message: string,
    opts: { maxAttempts: number; retryInterval: number }
  ) {
    if (!Number.isInteger(opts.maxAttempts) || opts.maxAttempts < 1) {
      throw new RangeError("RetryableError maxAttempts must be a positive integer");
    }
    if (!Number.isFinite(opts.retryInterval) || opts.retryInterval < 0) {
      throw new RangeError("RetryableError retryInterval must be a finite number that is not negative");
    }
    super(message);
    this.name = "RetryableError";
    this.maxAttempts = opts.maxAttempts;
    this.retryInterval = opts.retryInterval;
  }
}

registerErrorClass("RetryableError", RetryableError);

/**
 * Thrown by the HTTP SDK for an error response that has no more specific
 * error, such as a middleware denial. `code` is the response's `code`, when
 * it has one.
 */
export class HttpRouteError extends Error {
  readonly status: number;
  readonly code?: string;

  constructor(status: number, code: string | undefined, message: string) {
    super(message);
    this.name = "HttpRouteError";
    this.status = status;
    this.code = code;
  }
}

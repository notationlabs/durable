import { registerErrorClass } from "@yieldstar/core";

export class RetryableError extends Error {
  maxAttempts: number;
  retryInterval: number;
  constructor(
    message: string,
    opts: { maxAttempts: number; retryInterval: number }
  ) {
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

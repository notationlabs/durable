export function waiterKey(step: { executionId: string; stepKey: string }) {
  return JSON.stringify([step.executionId, step.stepKey]);
}

/** Deletes `executionId`'s waiters from a map keyed by target, then by `waiterKey`. */
export function deleteExecutionWaiters(
  waitersByTarget: Map<string, Map<string, { executionId: string }>>,
  executionId: string
) {
  for (const [target, waiters] of waitersByTarget) {
    for (const [key, waiter] of waiters) {
      if (waiter.executionId === executionId) waiters.delete(key);
    }
    if (waiters.size === 0) waitersByTarget.delete(target);
  }
}

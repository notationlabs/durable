# Cloudflare Workflows and Notation Durable on a deployed Worker

Measured on 2026-11-25. In every table a lower figure is better.

## Time: median

| Workflow | Measured | Cloudflare Workflows | Durable | Durable, deliveries in the object |
| :--- | :--- | ---: | ---: | ---: |
| One step | Trigger call returns | 268 ms | 31 ms | 26 ms |
|  | Trigger to the step function starting | 682 ms | 30 ms | 23 ms |
|  | Trigger to the caller holding the result | 906 ms | 70 ms | 39 ms |
| 100 steps in sequence | Time per step | 25 ms | 13 ms | 0.26 ms |
| 20 steps that each return 100 KB | Time per step | 30 ms | 20 ms | 1.1 ms |
| 5 sleeps of 1 s | Time beyond 1 s until the next step starts | 27 ms | 42 ms | 10 ms |
| 20 concurrent steps that each wait 100 ms | Time beyond 100 ms until the next step starts | 100 ms | 52 ms | 13 ms |
| A wait for a signal from outside | Signal sent to the next step starting | 41 ms | 34 ms | 16 ms |
| A parent that runs 5 one-step children | Children started to all results joined | 1.45 s | 129 ms | 35 ms |
| 100 three-step executions triggered at once | Trigger calls return | 824 ms | 164 ms | 169 ms |
|  | Trigger to the median execution's last step | 1.01 s | 615 ms | 441 ms |
|  | Trigger to the last execution's last step | 1.73 s | 1.04 s | 745 ms |

## Time: 90th percentile

| Workflow | Measured | Cloudflare Workflows | Durable | Durable, deliveries in the object |
| :--- | :--- | ---: | ---: | ---: |
| One step | Trigger call returns | 321 ms | 42 ms | 38 ms |
|  | Trigger to the step function starting | 1.14 s | 44 ms | 36 ms |
|  | Trigger to the caller holding the result | 1.54 s | 82 ms | 50 ms |
| 100 steps in sequence | Time per step | 33 ms | 13 ms | 0.41 ms |
| 20 steps that each return 100 KB | Time per step | 33 ms | 21 ms | 1.8 ms |
| 5 sleeps of 1 s | Time beyond 1 s until the next step starts | 36 ms | 47 ms | 13 ms |
| 20 concurrent steps that each wait 100 ms | Time beyond 100 ms until the next step starts | 131 ms | 61 ms | 18 ms |
| A wait for a signal from outside | Signal sent to the next step starting | 49 ms | 60 ms | 22 ms |
| A parent that runs 5 one-step children | Children started to all results joined | 3.42 s | 203 ms | 48 ms |
| 100 three-step executions triggered at once | Trigger calls return | 1.39 s | 234 ms | 221 ms |
|  | Trigger to the median execution's last step | 3.96 s | 657 ms | 475 ms |
|  | Trigger to the last execution's last step | 6.20 s | 1.17 s | 872 ms |

## How to read the times

- **The systems** – All three run in one Worker. "Durable" is `WorkflowObject` with its defaults, which runs each delivery in another instance of the class. "Durable, deliveries in the object" sets `isolateDeliveries: false`.
- **The objects** – One Durable object holds every execution it is given. The measurements are spread evenly over 3 such objects for each Durable column, because the time of a call between two objects depends on where Cloudflare runs them.
- **The workflows** – Each workflow is written once for Cloudflare Workflows (`native.ts`) and once for Durable (`durable.ts`), with the same steps. Cloudflare Workflows has no step that waits for another instance, so its parent waits for an event that each child sends from a second step.
- **The clock** – Every time is read in one Durable Object. The driver runs in it, and a step function reports that it started by calling it three times in a row on one stub. The first call on a new stub takes longer than the next, so the first call only warms the stub, and the step function times it with its own clock. The time between the arrivals of the other two is one round trip of that call. The warm-up and half a round trip are taken off each figure that starts at a step, and half a round trip off each figure that ends at a step. The median round trip was: Cloudflare Workflows 3.0 ms, Durable 4.0 ms, Durable, deliveries in the object 13 ms.
- **The caller holding the result** – The Durable caller uses `sdk.wait`. The Cloudflare Workflows caller reads `instance.status()` every 10 ms.
- **The burst** – Cloudflare Workflows runs each instance on its own. One Durable object runs 10 deliveries at once by default (`concurrency`).
- **Measurements for each figure** – One step: 21; 100 steps in sequence: 12; 20 steps that each return 100 KB: 12; 5 sleeps of 1 s: 30; 20 concurrent steps that each wait 100 ms: 15; A wait for a signal from outside: 15; A parent that runs 5 one-step children: 15; 100 three-step executions triggered at once: 9. One execution of each workflow ran unmeasured first in each object.

## Cost

US dollars for one million executions, measured on 2026-11-25. The prices are those beyond the monthly allowances of the Workers Paid plan. Storage is not in these figures.

| Workflow | Cloudflare Workflows | Durable | Durable, deliveries in the object |
| :--- | ---: | ---: | ---: |
| One step | $8.30 | $11.4 | $11.2 |
| 100 steps in sequence | $817 | $117 | $113 |
| 20 steps that each return 100 KB | $177 | $33.6 | $32.2 |
| 5 sleeps of 1 s | $88.3 | $61.2 | $59.4 |
| 20 concurrent steps that each wait 100 ms | $176 | $34.2 | $33.4 |
| A wait for a signal from outside | $16.3 | $27.2 | $26.6 |
| A parent that runs 5 one-step children | $178 | $127 | $111 |
| 100 three-step executions triggered at once | $24.3 | $13.6 | $13.3 |
| One step that waits 1 s | $8.31 | $14.7 | $12.8 |
| One step that waits 30 s | $8.30 | $116 | $67.8 |
| 10 steps that each wait 1 s | $80.4 | $56.0 | $39.3 |
| 100 executions at once of one step that waits 1 s | $8.31 | $13.5 | $11.7 |

### What Cloudflare bills for one execution on Cloudflare Workflows

| Workflow | Instances | Steps | CPU time |
| :--- | ---: | ---: | ---: |
| One step | 1 | 1 | 0.2 ms |
| 100 steps in sequence | 1 | 102 | 27 ms |
| 20 steps that each return 100 KB | 1 | 22 | 10 ms |
| 5 sleeps of 1 s | 1 | 11 | 1.8 ms |
| 20 concurrent steps that each wait 100 ms | 1 | 22 | 3.6 ms |
| A wait for a signal from outside | 1 | 2 | 0.2 ms |
| A parent that runs 5 one-step children | 6 | 22 | 9.4 ms |
| 100 three-step executions triggered at once | 1 | 3 | 0.86 ms |
| One step that waits 1 s | 1 | 1 | 0.6 ms |
| One step that waits 30 s | 1 | 1 | 0 ms |
| 10 steps that each wait 1 s | 1 | 10 | 6.3 ms |
| 100 executions at once of one step that waits 1 s | 1 | 1 | 0.39 ms |

### What Cloudflare bills for one execution on Durable

| Workflow | Requests | Rows written | Rows read | Duration |
| :--- | ---: | ---: | ---: | ---: |
| One step | 2 | 11 | 32 | 71 ms |
| 100 steps in sequence | 2 | 112 | 281 | 2.43 s |
| 20 steps that each return 100 KB | 2 | 32 | 89 | 775 ms |
| 5 sleeps of 1 s | 12 | 58 | 483 | 558 ms |
| 20 concurrent steps that each wait 100 ms | 2 | 33 | 82 | 357 ms |
| A wait for a signal from outside | 4.2 | 26 | 77 | 208 ms |
| A parent that runs 5 one-step children | 10 | 123 | 404 | 878 ms |
| 100 three-step executions triggered at once | 2 | 13 | 114 | 81 ms |
| One step that waits 1 s | 2 | 11 | 72 | 2.07 s |
| One step that waits 30 s | 2.3 | 18 | 1261 | 60.10 s |
| 10 steps that each wait 1 s | 2 | 23 | 464 | 20.34 s |
| 100 executions at once of one step that waits 1 s | 2 | 11 | 319 | 1.17 s |

### What Cloudflare bills for one execution on Durable, with deliveries in the object

| Workflow | Requests | Rows written | Rows read | Duration |
| :--- | ---: | ---: | ---: | ---: |
| One step | 1 | 11 | 31 | 9 ms |
| 100 steps in sequence | 1 | 112 | 233 | 11 ms |
| 20 steps that each return 100 KB | 1 | 32 | 73 | 10 ms |
| 5 sleeps of 1 s | 6 | 58 | 418 | 72 ms |
| 20 concurrent steps that each wait 100 ms | 1 | 33 | 79 | 109 ms |
| A wait for a signal from outside | 2 | 26 | 72 | 22 ms |
| A parent that runs 5 one-step children | 1 | 110 | 250 | 23 ms |
| 100 three-step executions triggered at once | 1 | 13 | 104 | 2 ms |
| One step that waits 1 s | 1 | 11 | 71 | 1.01 s |
| One step that waits 30 s | 1.3 | 18 | 1259 | 30.02 s |
| 10 steps that each wait 1 s | 1 | 23 | 456 | 10.02 s |
| 100 executions at once of one step that waits 1 s | 1 | 11 | 316 | 101 ms |

### How to read it

- **The executions** – Each figure is the mean of 2 to 5 runs of the workflow. A step function does nothing unless its row says that it waits, and a step that waits does so on a timer. A burst's figures are for one of its executions, and a parent's figures include its children.
- **Cloudflare Workflows** – An instance costs $0.30 per million, a step $8.00 per million, and a millisecond of CPU time $0.02 per million. A sleep and a wait for an event each count as a step. The step count of each instance comes from Cloudflare's API. The CPU time comes from Cloudflare's analytics, which sample their events, so it is an estimate. It is under 1% of every figure.
- **Durable** – A request to an object costs $0.15 per million, a row written $1.00 per million, a row read $0.001 per million, and a second of an object's duration $1.60 per million. The object counts them itself: its trigger requests, alarms, RPC calls, the rows that each SQL cursor reports, and the time it has a request, an alarm or a delivery in progress. A call to `setAlarm` or `deleteAlarm` counts as a row written.
- **Where Durable's cost comes from** – Where no step waits, rows written are 95% to 100% of a Durable figure and duration is 0% to 4%. Where a step waits, duration is 1% to 83%. Cloudflare counts a row for each index that a write changes, as well as the row itself. Duration is an estimate: it leaves out the time a response waits for storage to confirm a write.
- **Deliveries in other instances** – By default each delivery is one more request, to the instance that runs it, and that instance is billed for the delivery's duration too. The instance sends each step result to the object over a stub it was given. These calls are not counted as requests, which is how Cloudflare's pricing page describes calls on a stub that a call returns.
- **Not counted** – A request that reads an execution's status or waits for its result is one more request to the object, and the object is billed for the time a wait stays open. Reading the status of a Cloudflare Workflows instance is not billed.
- **Steps that take time** – Durable Objects bill duration while a step function runs, and Cloudflare Workflows bills only CPU time. So each second that a step waits adds $1.60 per million executions with deliveries in the object, and $3.20 by default, where both the object and the instance that runs the delivery are billed. On Cloudflare Workflows it adds nothing. An object is billed once for any number of executions that wait at the same time, so the executions that wait at once share the object's second, and each instance that runs a delivery is still billed for its own.
- **Storage** – Both cost $0.20 per GB-month, and it is not in the figures above. Cloudflare Workflows deletes an instance 7 days after it ends by default. Durable keeps an execution until it is deleted, and one execution added 12.3 KB for "100 steps in sequence" ($2.46 a month for a million of them), 2.1 MB for "20 steps that each return 100 KB" ($413 a month for a million of them), 0.8 KB for "100 three-step executions triggered at once" ($0.16 a month for a million of them). Cloudflare's analytics do not give a usable size for a Workflows instance.

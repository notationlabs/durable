# Cloudflare Workflows and Notation Durable on a deployed Worker

Measured on 2026-11-25. In every table a lower figure is better.

## Time: median

| Workflow | Measured | Cloudflare Workflows | Durable | Durable, deliveries in the object |
| :--- | :--- | ---: | ---: | ---: |
| One step | Trigger call returns | 321 ms | 48 ms | 71 ms |
|  | Trigger to the step function starting | 740 ms | 54 ms | 74 ms |
|  | Trigger to the caller holding the result | 924 ms | 95 ms | 89 ms |
| 100 steps in sequence | Time per step | 28 ms | 12 ms | 0.41 ms |
| 20 steps that each return 100 KB | Time per step | 29 ms | 23 ms | 2.5 ms |
| 5 sleeps of 1 s | Time beyond 1 s until the next step starts | 30 ms | 58 ms | 19 ms |
| 20 concurrent steps that each wait 100 ms | Time beyond 100 ms until the next step starts | 138 ms | 63 ms | 23 ms |
| A wait for a signal from outside | Signal sent to the next step starting | 49 ms | 62 ms | 54 ms |
| A parent that runs 5 one-step children | Children started to all results joined | 1.60 s | 150 ms | 42 ms |
| 100 three-step executions triggered at once | Trigger calls return | 1.12 s | 205 ms | 260 ms |
|  | Trigger to the median execution's last step | 1.23 s | 652 ms | 529 ms |
|  | Trigger to the last execution's last step | 3.93 s | 1.10 s | 879 ms |

## Time: 90th percentile

| Workflow | Measured | Cloudflare Workflows | Durable | Durable, deliveries in the object |
| :--- | :--- | ---: | ---: | ---: |
| One step | Trigger call returns | 378 ms | 60 ms | 185 ms |
|  | Trigger to the step function starting | 1.15 s | 67 ms | 194 ms |
|  | Trigger to the caller holding the result | 1.51 s | 120 ms | 212 ms |
| 100 steps in sequence | Time per step | 33 ms | 16 ms | 0.70 ms |
| 20 steps that each return 100 KB | Time per step | 37 ms | 24 ms | 3.8 ms |
| 5 sleeps of 1 s | Time beyond 1 s until the next step starts | 42 ms | 69 ms | 22 ms |
| 20 concurrent steps that each wait 100 ms | Time beyond 100 ms until the next step starts | 201 ms | 79 ms | 45 ms |
| A wait for a signal from outside | Signal sent to the next step starting | 74 ms | 79 ms | 177 ms |
| A parent that runs 5 one-step children | Children started to all results joined | 2.48 s | 206 ms | 51 ms |
| 100 three-step executions triggered at once | Trigger calls return | 1.24 s | 264 ms | 494 ms |
|  | Trigger to the median execution's last step | 3.01 s | 834 ms | 814 ms |
|  | Trigger to the last execution's last step | 5.07 s | 1.52 s | 1.16 s |

## How to read the times

- **The systems** – All three run in one Worker. "Durable" is `WorkflowObject` with its defaults, which runs each delivery in another instance of the class. "Durable, deliveries in the object" sets `isolateDeliveries: false`.
- **The objects** – One Durable object holds every execution it is given. The measurements are spread evenly over 3 such objects for each Durable column, because the time of a call between two objects depends on where Cloudflare runs them.
- **The workflows** – Each workflow is written once for Cloudflare Workflows (`native.ts`) and once for Durable (`durable.ts`), with the same steps. Cloudflare Workflows has no step that waits for another instance, so its parent waits for an event that each child sends from a second step.
- **The clock** – Every time is read in one Durable Object. The driver runs in it, and a step function reports that it started by calling it twice in a row. The time between the two arrivals is one round trip of that call, and half of it is taken off each figure that starts or ends at a step. The median round trip was: Cloudflare Workflows 4.0 ms, Durable 4.0 ms, Durable, deliveries in the object 19 ms.
- **The caller holding the result** – The Durable caller uses `sdk.wait`. The Cloudflare Workflows caller reads `instance.status()` every 10 ms.
- **The burst** – Cloudflare Workflows runs each instance on its own. One Durable object runs 10 deliveries at once by default (`concurrency`).
- **Measurements for each figure** – One step: 21; 100 steps in sequence: 12; 20 steps that each return 100 KB: 12; 5 sleeps of 1 s: 30; 20 concurrent steps that each wait 100 ms: 15; A wait for a signal from outside: 15; A parent that runs 5 one-step children: 15; 100 three-step executions triggered at once: 9. One execution of each workflow ran unmeasured first in each object.

## Cost

US dollars for one million executions, measured on 2026-11-25. The prices are those beyond the monthly allowances of the Workers Paid plan. Storage is not in these figures.

| Workflow | Cloudflare Workflows | Durable | Durable, deliveries in the object |
| :--- | ---: | ---: | ---: |
| One step | $8.30 | $21.6 | $21.3 |
| 100 steps in sequence | $817 | $328 | $324 |
| 20 steps that each return 100 KB | $177 | $85.7 | $84.3 |
| 5 sleeps of 1 s | $88.4 | $119 | $117 |
| 20 concurrent steps that each wait 100 ms | $176 | $88.1 | $87.5 |
| A wait for a signal from outside | $16.3 | $50.2 | $49.5 |
| A parent that runs 5 one-step children | $178 | $241 | $213 |
| 100 three-step executions triggered at once | $24.3 | $24.8 | $24.5 |
| One step that waits 1 s | $8.31 | $24.9 | $22.9 |
| One step that waits 30 s | $8.31 | $132 | $83.5 |
| 10 steps that each wait 1 s | $80.5 | $85.3 | $68.7 |
| 100 executions at once of one step that waits 1 s | $8.31 | $20.7 | $18.9 |

### What Cloudflare bills for one execution on Cloudflare Workflows

| Workflow | Instances | Steps | CPU time |
| :--- | ---: | ---: | ---: |
| One step | 1 | 1 | 0.2 ms |
| 100 steps in sequence | 1 | 102 | 27 ms |
| 20 steps that each return 100 KB | 1 | 22 | 11 ms |
| 5 sleeps of 1 s | 1 | 11 | 5.2 ms |
| 20 concurrent steps that each wait 100 ms | 1 | 22 | 3.8 ms |
| A wait for a signal from outside | 1 | 2 | 1 ms |
| A parent that runs 5 one-step children | 6 | 22 | 9.4 ms |
| 100 three-step executions triggered at once | 1 | 3 | 1.1 ms |
| One step that waits 1 s | 1 | 1 | 0.6 ms |
| One step that waits 30 s | 1 | 1 | 0.67 ms |
| 10 steps that each wait 1 s | 1 | 10 | 11 ms |
| 100 executions at once of one step that waits 1 s | 1 | 1 | 0.52 ms |

### What Cloudflare bills for one execution on Durable

| Workflow | Requests | Rows written | Rows read | Duration |
| :--- | ---: | ---: | ---: | ---: |
| One step | 2 | 21 | 32 | 161 ms |
| 100 steps in sequence | 2 | 324 | 274 | 2.13 s |
| 20 steps that each return 100 KB | 2 | 84 | 86 | 814 ms |
| 5 sleeps of 1 s | 12 | 116 | 481 | 631 ms |
| 20 concurrent steps that each wait 100 ms | 2 | 87 | 82 | 473 ms |
| A wait for a signal from outside | 4 | 49 | 76 | 308 ms |
| A parent that runs 5 one-step children | 9.2 | 238 | 356 | 868 ms |
| 100 three-step executions triggered at once | 2 | 24 | 114 | 78 ms |
| One step that waits 1 s | 2 | 21 | 72 | 2.18 s |
| One step that waits 30 s | 2 | 34 | 1259 | 60.18 s |
| 10 steps that each wait 1 s | 2 | 52 | 463 | 20.35 s |
| 100 executions at once of one step that waits 1 s | 2 | 18 | 323 | 1.16 s |

### What Cloudflare bills for one execution on Durable, with deliveries in the object

| Workflow | Requests | Rows written | Rows read | Duration |
| :--- | ---: | ---: | ---: | ---: |
| One step | 1 | 21 | 31 | 63 ms |
| 100 steps in sequence | 1 | 324 | 233 | 64 ms |
| 20 steps that each return 100 KB | 1 | 84 | 73 | 69 ms |
| 5 sleeps of 1 s | 6 | 116 | 418 | 56 ms |
| 20 concurrent steps that each wait 100 ms | 1 | 87 | 79 | 167 ms |
| A wait for a signal from outside | 2 | 49 | 72 | 97 ms |
| A parent that runs 5 one-step children | 1 | 213 | 254 | 27 ms |
| 100 three-step executions triggered at once | 1 | 24 | 104 | 3 ms |
| One step that waits 1 s | 1 | 21 | 71 | 1.06 s |
| One step that waits 30 s | 1 | 34 | 1258 | 30.06 s |
| 10 steps that each wait 1 s | 1 | 52 | 458 | 10.05 s |
| 100 executions at once of one step that waits 1 s | 1 | 18 | 318 | 102 ms |

### How to read it

- **The executions** – Each figure is the mean of 2 to 5 runs of the workflow. A step function does nothing unless its row says that it waits, and a step that waits does so on a timer. A burst's figures are for one of its executions, and a parent's figures include its children.
- **Cloudflare Workflows** – An instance costs $0.30 per million, a step $8.00 per million, and a millisecond of CPU time $0.02 per million. A sleep and a wait for an event each count as a step. The step count of each instance comes from Cloudflare's API. The CPU time comes from Cloudflare's analytics, which sample their events, so it is an estimate. It is under 1% of every figure.
- **Durable** – A request to an object costs $0.15 per million, a row written $1.00 per million, a row read $0.001 per million, and a second of an object's duration $1.60 per million. The object counts them itself: its trigger requests, alarms, RPC calls, the rows that each SQL cursor reports, and the time it has a request, an alarm or a delivery in progress. A call to `setAlarm` or `deleteAlarm` counts as a row written.
- **Where Durable's cost comes from** – Where no step waits, rows written are 97% to 100% of a Durable figure and duration is 0% to 2%. Where a step waits, duration is 1% to 73%. Cloudflare counts a row for each index that a write changes, as well as the row itself. Duration is an estimate: it leaves out the time a response waits for storage to confirm a write.
- **Deliveries in other instances** – By default each delivery is one more request, to the instance that runs it, and that instance is billed for the delivery's duration too. The instance sends each step result to the object over a stub it was given. These calls are not counted as requests, which is how Cloudflare's pricing page describes calls on a stub that a call returns.
- **Not counted** – A request that reads an execution's status or waits for its result is one more request to the object, and the object is billed for the time a wait stays open. Reading the status of a Cloudflare Workflows instance is not billed.
- **Steps that take time** – Durable Objects bill duration while a step function runs, and Cloudflare Workflows bills only CPU time. So each second that a step waits adds $1.60 per million executions with deliveries in the object, and $3.20 by default, where both the object and the instance that runs the delivery are billed. On Cloudflare Workflows it adds nothing. An object is billed once for any number of executions that wait at the same time, so the executions that wait at once share the object's second, and each instance that runs a delivery is still billed for its own.
- **Storage** – Both cost $0.20 per GB-month, and it is not in the figures above. Cloudflare Workflows deletes an instance 7 days after it ends by default. Durable keeps an execution until it is deleted, and one execution added 22.9 KB for "100 steps in sequence" ($4.59 a month for a million of them), 2.0 MB for "20 steps that each return 100 KB" ($402 a month for a million of them), 1.3 KB for "100 three-step executions triggered at once" ($0.27 a month for a million of them). Cloudflare's analytics do not give a usable size for a Workflows instance.

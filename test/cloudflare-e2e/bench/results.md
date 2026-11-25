# Cloudflare Workflows and Notation Durable on a deployed Worker

Measured on 2026-11-25. Every figure is a time, and a lower one is better.

## Median

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

## 90th percentile

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

## How to read it

- **The systems** – All three run in one Worker. "Durable" is `WorkflowObject` with its defaults, which runs each delivery in another instance of the class. "Durable, deliveries in the object" sets `isolateDeliveries: false`.
- **The objects** – One Durable object holds every execution it is given. The measurements are spread evenly over 3 such objects for each Durable column, because the time of a call between two objects depends on where Cloudflare runs them.
- **The workflows** – Each workflow is written once for Cloudflare Workflows (`native.ts`) and once for Durable (`durable.ts`), with the same steps. Cloudflare Workflows has no step that waits for another instance, so its parent waits for an event that each child sends from a second step.
- **The clock** – Every time is read in one Durable Object. The driver runs in it, and a step function reports that it started by calling it twice in a row. The time between the two arrivals is one round trip of that call, and half of it is taken off each figure that starts or ends at a step. The median round trip was: Cloudflare Workflows 4.0 ms, Durable 4.0 ms, Durable, deliveries in the object 19 ms.
- **The caller holding the result** – The Durable caller uses `sdk.wait`. The Cloudflare Workflows caller reads `instance.status()` every 10 ms.
- **The burst** – Cloudflare Workflows runs each instance on its own. One Durable object runs 10 deliveries at once by default (`concurrency`).
- **Measurements for each figure** – One step: 21; 100 steps in sequence: 12; 20 steps that each return 100 KB: 12; 5 sleeps of 1 s: 30; 20 concurrent steps that each wait 100 ms: 15; A wait for a signal from outside: 15; A parent that runs 5 one-step children: 15; 100 three-step executions triggered at once: 9. One execution of each workflow ran unmeasured first in each object.

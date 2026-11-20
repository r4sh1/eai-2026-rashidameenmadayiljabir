# AI usage

I used ChatGPT as an assistant while working on this capstone. It helped me understand some requirements, investigate errors, review my implementation and organise the documentation.

I did not use AI output without checking it. I reviewed the suggestions, compared them with the project requirements and tested the changes where possible. I made the final decisions about what to keep in my project.

## Tools used

- **ChatGPT (GPT-6):** I used it to understand requirements, discuss implementation problems, review errors and help prepare documentation.
- **Visual Studio Code:** I used it to edit the project files and work with the source code.
- **PowerShell:** I used it to run commands, inspect files and execute the tests.
- **Node.js and npm:** I used them to run the project test suite.
- **Docker Compose:** I used it to build and run the services and their supporting containers.

ChatGPT was used as an assistant, not as a replacement for understanding or testing my own work.

## What for

I used AI assistance at different stages of the capstone.

- **Understanding requirements:** I asked for explanations of the README requirements and the expected behaviour of the services.
- **Debugging:** I used ChatGPT to help investigate test failures, terminal output and problems in the implementation.
- **Intake and order processing:** I discussed order validation, message publishing, idempotency and the order lifecycle.
- **Saga and compensation:** I used it to understand retry handling, payment and inventory operations, compensation and stuck sagas.
- **RabbitMQ:** I reviewed the exchange, queue, binding and dead-letter configuration.
- **Testing:** I used explanations of test results to understand which stages passed and which requirements still needed work.
- **Documentation:** I used assistance to organise the architecture, agent-readiness and AI-usage documentation.



## What I changed or rejected

I did not accept every suggestion automatically. Some suggestions needed to be checked against the actual project.

For example:

- When a test failed because `npm test` was run from the project root, I checked the `tests/package.json` file. I found that the test script was inside the `tests` folder, so I used the correct command from that directory.
- When preparing the RabbitMQ documentation, I did not rely only on a suggested diagram. I inspected `rabbitmq/definitions.json` and used the actual exchanges, queues and bindings from my configuration.
- I rejected the idea of giving a future AI agent unrestricted access to RabbitMQ or provider administration endpoints. Such access could affect messages and business operations.
- I also decided that replaying messages and compensating sagas should require human approval in a future agent integration. These are proposed controls, not features I claim to have implemented.
- I avoided describing proposed AI-agent features as if they were already part of my application.

My final implementation and documentation should reflect what the project actually does, rather than simply copying an AI suggestion.

## What I verified, and how

I used the public test suite and manual checks to verify my work.

- The latest recorded local public-test run passed 30 tests across the seven stage files (stages 0–6).
- I inspected the current stage 6 test source and checked the document headings, diagrams, patterns table, runbook sections and BPMN structure.
- On 19 November 2026, I downloaded a fresh copy of the repository and installed its dependencies with `npm ci`.
- I ran `npx tsc --noEmit` successfully and checked the Docker Compose configuration.
- I built and started the Compose stack from that fresh copy. All 12 services started; the 11 services with health checks reported healthy.
- I ran the public test suite with `npx vitest run --config vitest.config.ts --root tests`. All 7 test files and all 30 tests passed.
- On 20 November 2026, I ran `t-check.ps1` from the capstone folder in Temporal mode. The script reported **12 passed, 0 failed** across the T1–T7 checks.
- **T1:** The Temporal stack started within the time limit, services reported healthy, the worker polled the `checkout` task queue, `GET /sagas/stuck` returned an empty list, and the compensation endpoint returned HTTP 409 with `saga_not_stuck` for a completed saga.
- **T2:** Web and B2B orders completed. The worker trace recorded the expected activities. The web order had one charge, one inventory reservation, one shipment and one notification. The B2B order had no payment charge and had one inventory reservation, one shipment and one notification.
- **T3a–T3c:** Inventory failure led to `compensated`; a shipping timeout produced three attempts and ended as `compensated` with code `timeout`; and an inventory failure followed by a refund failure ended as `compensation_failed`.
- **T4:** I killed the worker during slow shipping and restarted it. The order completed with one payment charge and one inventory reservation, but the shipping provider recorded **two shipment effects**. This duplicate shipment side effect remains a known limitation.
- **T5:** The payment provider returned HTTP 503 twice before succeeding. The order completed with one charge, and no matching message appeared in `orders.dlq`.
- **T6:** The check republished the same `order.accepted` event; the order remained completed with one charge. Submitting the same order with a new `Idempotency-Key` returned `duplicate`. I then inspected the Temporal UI at `http://localhost:8240`. The Workflows list showed one completed workflow row for `web:WEB-X90821874`, with workflow type `checkout` and a single run ID. This confirmed the one-workflow UI check for the tested business key.
- **T7:** The ADR-011 structure check passed: the five required headings contained text, and there were at least three list items under `What the engine took over`.
- I inspected `rabbitmq/definitions.json` to confirm the actual exchanges, queues and routing bindings.
- The T5 Temporal check exercised payment retry behaviour: the payment provider returned HTTP 503 twice, then succeeded; the order completed with one charge.
- I previously tested a permanent payment failure and observed the order parked in the dead-letter queue. This does not by itself verify the complete hand-rolled retry-and-replay recovery sequence listed below.
- I inspected the order status and trace information to understand the processing steps and retry behaviour.

These checks were run on my project machine using the fresh repository copy. The T1–T7 script reported 12 passed and 0 failed. The separate T6 Temporal UI inspection was also completed manually and showed one workflow for the tested business key. These results are not a result from the instructor's 60-point hidden test suite.

The following additional operational hand checks are still outstanding and are not claimed as passed: the complete hand-rolled retry path followed by DLQ replay; concurrent order processing; hand-rolled stuck-saga detection followed by operator compensation; and RabbitMQ broker stop/start recovery. The public P4.5 test did pass the notification DLQ replay case, but that is not a substitute for the separate end-to-end order retry/replay hand check.

## Dependencies beyond the allow-list

- **Axios (`axios`, worker dependency):** I use Axios in the Temporal worker to send HTTP requests to the payment, inventory, shipping and notification provider services. It also lets the worker inspect HTTP error responses and distinguish non-retryable 4xx responses from retryable provider or network failures. Axios is declared in `worker/package.json` and locked in `worker/package-lock.json`.

ChatGPT was used for development assistance; it is not an application runtime dependency.

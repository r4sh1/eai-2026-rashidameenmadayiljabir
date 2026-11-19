
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

I still needed to inspect the actual project files and compare the suggestions with the requirements before applying changes.

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

- The latest recorded local public-test run before this final review passed 30 tests across the seven stage files (stages 0–6).
- I inspected the current stage 6 test source and checked the document headings, diagrams, patterns table, runbook sections and BPMN structure.
- The final README, ADR-011 and workflow-start trace changes in this review still need a fresh test run in the project environment.
- I inspected `rabbitmq/definitions.json` to confirm the actual exchanges, queues and routing bindings.
- I tested the payment retry behaviour by configuring the payment mock to fail twice and then succeed. The order completed after three attempts, and the payment effect occurred only once.
- I tested a permanent payment failure. The order was parked after the retry attempts and appeared in the dead-letter queue.
- I inspected the order status and trace information to understand the processing steps and retry behaviour.

The current review environment could not rerun the project suite: the archive does not include the platform-compatible test dependencies, and Docker is unavailable here. The final changes must be checked with the test command above on the project machine.

## Dependencies beyond the allow-list

None intentionally added beyond the dependencies permitted by the capstone requirements.

I used the existing project dependencies and tools to develop, run and test the application. ChatGPT was used for assistance and is not an application dependency.

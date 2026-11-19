# Patterns

The table below lists integration patterns that are present in the capstone implementation. Each link points to the relevant implementation area in the repository.

| Pattern | Problem | Where | Why |
|---|---|---|---|
| Normalizer | Web, mobile and B2B orders arrive in different shapes and formats, so downstream services cannot safely process them as one order structure. | [transform.ts:454](../intake/src/transform.ts#L454) | Intake translates each source into the canonical order shape at the system boundary, rather than making every downstream service understand every source format. |
| Message Translator | Source-specific field names, dates, status values and B2B XML do not match the canonical order model. | [transform.ts:564](../intake/src/transform.ts#L564) | A dedicated B2B translation path maps the partner message into the shared model while keeping source-format handling out of the saga. |
| Content Enricher | Incoming order lines do not contain a trusted price, currency and tax rate. | [transform.ts:309](../intake/src/transform.ts#L309) | Intake looks up product details from the pricing service and enriches valid lines instead of trusting client-supplied prices or hardcoding them. |
| Content Filter | Incoming web orders contain payment details that should not be copied into the canonical order. | [transform.ts:477](../intake/src/transform.ts#L477) | The canonical customer object is built from the required customer and address fields only, so payment details are not propagated downstream. |
| Canonical Data Model | Each channel's payload has different names and structures, which would otherwise force consumers to handle multiple formats. | [transform.ts:62](../intake/src/transform.ts#L62) | `CanonicalOrder` gives intake and downstream services one agreed order structure. |
| Idempotent Receiver | A client may retry the same HTTP request, or the broker may redeliver a message, causing duplicate work. | [index.ts:739](../intake/src/index.ts#L739) | Intake stores the idempotency key and request hash before accepting the order, allowing a repeated key and body to reuse the saved response and correlation ID. |
| Guaranteed Delivery | An accepted order must not be acknowledged to the client if it has not been safely handed to the broker. | [index.ts:913](../intake/src/index.ts#L913) | Intake persists the accepted order before publishing it, and its publish path uses a confirm channel so a failed publish can be reported and the stored request state cleaned up. |
| Process Manager / Saga | An order spans several independent services, and a failure partway through must be coordinated rather than handled as unrelated calls. | [index.ts:707](../orchestrator/src/index.ts#L707) | The orchestrator controls the ordered payment, inventory, shipping and notification steps, including skipping payment for B2B orders. |
| Compensating Transaction | A later saga step can fail after an earlier step has already changed another service's state. | [index.ts:967](../orchestrator/src/index.ts#L967) | The orchestrator runs compensating actions for completed reversible steps, such as releasing inventory and refunding payment, instead of pretending the earlier work never happened. |



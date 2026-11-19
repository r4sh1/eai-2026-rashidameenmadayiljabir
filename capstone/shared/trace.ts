
import { randomUUID } from "node:crypto";
import type { Channel } from "amqplib";

export type TraceStatus =
  | "success"
  | "failed"
  | "timeout"
  | "retried"
  | "dead_lettered"
  | "quarantined"
  | "skipped"
  | "duplicate";

export interface TraceHopOptions {
  correlationId: string;
  causationId: string | null;
  service: "intake" | "orchestrator" | "notifier" | "observer" | "worker";
  operation: string;
  attempt?: number;
  messageId?: string;
  detail?: Record<string, unknown>;
}

export interface TraceHopHandle {
  hopId: string;
  finish: (
    status: TraceStatus,
    detail?: Record<string, unknown>
  ) => void;
}

/**
 * Starts a trace hop immediately, generating its UUID before
 * the corresponding business operation or provider call.
 */
export function beginTraceHop(
  channel: Channel,
  options: TraceHopOptions
): TraceHopHandle {
  const hopId = randomUUID();
  const startedAt = new Date();
  let finished = false;

  function finish(
    status: TraceStatus,
    extraDetail?: Record<string, unknown>
  ): void {
    if (finished) return;
    finished = true;

    const finishedAt = new Date();

    const hop = {
      hopId,
      correlationId: options.correlationId,
      causationId: options.causationId,
      service: options.service,
      operation: options.operation,
      status,
      attempt: options.attempt ?? 1,
      ...(options.messageId
        ? { messageId: options.messageId }
        : {}),
      startedAt: startedAt.toISOString(),
      finishedAt: finishedAt.toISOString(),
      durationMs: Math.max(
        0,
        finishedAt.getTime() - startedAt.getTime()
      ),
      ...(
        options.detail || extraDetail
          ? {
              detail: {
                ...(options.detail ?? {}),
                ...(extraDetail ?? {})
              }
            }
          : {}
      )
    };

    // Tracing must never block or fail the business operation.
    try {
      channel.publish(
        "trace.events",
        "",
        Buffer.from(JSON.stringify(hop)),
        {
          persistent: true,
          contentType: "application/json",
          messageId: hopId,
          headers: {
            correlationId: options.correlationId,
            causationId: hopId
          }
        }
      );
    } catch (error) {
      console.error(
        `[Trace] Could not publish ${options.operation}:`,
        error
      );
    }
  }

  return { hopId, finish };
}

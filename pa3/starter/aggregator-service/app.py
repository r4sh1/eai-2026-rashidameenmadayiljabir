"""
Aggregator Service
==================
Your job: implement the AGGREGATOR pattern on top of the connection
handling and consume loop already wired up below.

- Collect item results from orders.results, grouped by orderId.
- Completion condition: every item of the order has reported a result.
- Timeout: if the order has been sitting incomplete for too long (one
  worker crashed, or was never running), emit a PARTIAL result instead of
  waiting forever. A hung order is a worse outcome than an honest partial
  answer.
- Duplicate results (the same item redelivered, e.g. after a requeue) must
  not be double-counted.
- Two orders in flight at once must never have their results mixed up.

Consumes from: orders.results
Publishes to:  orders.complete

Required shape of the message you publish to orders.complete (tests depend
on every one of these fields):

    {
        "orderId": "<the order this result set belongs to>",
        "correlationId": "<same value as orderId>",
        "status": "complete" | "partial",
        "totalItems": <the totalItems every item message carried>,
        "receivedItems": <how many distinct items you actually collected>,
        "itemResults": [ <the result messages you received, in any order> ],
        "missingItemIndexes": [ <itemIndex values you never received>
                                 -- empty list when status == "complete" ]
    }

Publish exactly ONE message to orders.complete per order, whichever status
it ends up with.
"""

import json
import pika
import os
import threading
import time


# In-flight order state, keyed by orderId:
#   in_flight[order_id] = {
#       "correlation_id": str,
#       "total_items": int,
#       "results": {item_index: result_dict, ...},   # keyed by itemIndex
#       "last_seen": float (time.monotonic()),
#   }
#
# Keying "results" by itemIndex (not appending to a list) is what makes a
# redelivered duplicate a no-op: writing the same key twice just overwrites
# the same entry instead of growing the collection.
in_flight = {}
lock = threading.Lock()

# How long an order may sit with no new results before the sweep gives up
# on it and emits a partial. Chosen to be well above normal per-item worker
# latency but short enough that "partial" still means something -- see
# docs/adr-002.md for the production value and reasoning.
IDLE_TIMEOUT_SECONDS = float(os.environ.get('AGGREGATOR_IDLE_TIMEOUT_SECONDS', '5'))

# How often the background sweep checks for timed-out orders. Independent
# of IDLE_TIMEOUT_SECONDS; this just controls how promptly a timeout is
# noticed once it has actually elapsed.
SWEEP_INTERVAL_SECONDS = 1.0


def get_rabbitmq_connection():
    """Create a connection to RabbitMQ using environment variable for host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost'))
    )


def publish_completion(message):
    """Publish a single message to orders.complete. Called with the lock
    already released -- do not hold `lock` while doing network I/O."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='orders.complete', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='orders.complete',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # Persistent
    )
    connection.close()


def _build_completion_message(order_id, order_state, status):
    total_items = order_state["total_items"]
    received = order_state["results"]
    missing = [i for i in range(total_items) if i not in received]
    return {
        "orderId": order_id,
        "correlationId": order_state["correlation_id"],
        "status": status,
        "totalItems": total_items,
        "receivedItems": len(received),
        "itemResults": list(received.values()),
        "missingItemIndexes": missing,
    }


def aggregate_result(ch, method, properties, body):
    """
    Handle one message from orders.results:
    1. Parse it.
    2. Record it against the right order, keyed by itemIndex so a
       redelivered duplicate is a no-op rather than a second entry.
    3. Update that order's last-activity timestamp.
    4. If every expected item has now been recorded, build the "complete"
       message and publish it, then drop the order from in-flight state.
    5. Ack the message regardless.
    """
    try:
        result = json.loads(body)
        order_id = result['orderId']
        correlation_id = result.get('correlationId', order_id)
        item_index = result['itemIndex']
        total_items = result['totalItems']
    except (json.JSONDecodeError, KeyError) as exc:
        # A malformed result should not jam the queue behind it -- log and
        # drop it rather than block every order behind a bad message.
        print(f"[Aggregator] Dropping unparseable result: {exc}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    message_to_publish = None

    with lock:
        order_state = in_flight.setdefault(order_id, {
            "correlation_id": correlation_id,
            "total_items": total_items,
            "results": {},
            "last_seen": time.monotonic(),
        })

        # Keying by item_index makes a redelivered duplicate overwrite
        # the same slot instead of counting twice.
        order_state["results"][item_index] = result
        order_state["last_seen"] = time.monotonic()

        if len(order_state["results"]) >= order_state["total_items"]:
            message_to_publish = _build_completion_message(order_id, order_state, "complete")
            del in_flight[order_id]

    # Publish after releasing the lock -- never hold it during network I/O.
    if message_to_publish is not None:
        publish_completion(message_to_publish)
        print(f"[Aggregator] Order {order_id} complete "
              f"({message_to_publish['receivedItems']}/{message_to_publish['totalItems']})")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    """
    Runs forever in a background thread, started from main(). Every
    SWEEP_INTERVAL_SECONDS, look for orders that have gone quiet for more
    than IDLE_TIMEOUT_SECONDS and emit a partial result for each of them,
    turning "one worker never responds" from a hang into a completed-but-
    honest result.
    """
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)

        timed_out_messages = []

        with lock:
            now = time.monotonic()
            timed_out_order_ids = [
                order_id
                for order_id, order_state in in_flight.items()
                if now - order_state["last_seen"] > IDLE_TIMEOUT_SECONDS
            ]
            for order_id in timed_out_order_ids:
                order_state = in_flight.pop(order_id)
                timed_out_messages.append(
                    _build_completion_message(order_id, order_state, "partial")
                )

        # Publish after releasing the lock.
        for message in timed_out_messages:
            publish_completion(message)
            print(f"[Aggregator] Order {message['orderId']} timed out -- "
                  f"partial ({message['receivedItems']}/{message['totalItems']}), "
                  f"missing {message['missingItemIndexes']}")


def main():
    """Main entry point: connect to RabbitMQ, start the timeout sweeper,
    and start consuming results."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    # Declare queues (idempotent)
    channel.queue_declare(queue='orders.results', durable=True)
    channel.queue_declare(queue='orders.complete', durable=True)

    # Fair dispatch
    channel.basic_qos(prefetch_count=1)

    # Background thread: sweeps for orders that timed out waiting on a
    # worker that never answered.
    sweeper = threading.Thread(target=sweep_timeouts, daemon=True)
    sweeper.start()

    # Start consuming
    channel.basic_consume(queue='orders.results', on_message_callback=aggregate_result)

    print('[Aggregator] Waiting for results...')
    channel.start_consuming()


if __name__ == '__main__':
    main()
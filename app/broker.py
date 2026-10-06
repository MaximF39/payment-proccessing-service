from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import ExchangeType

from app.config import get_settings

settings = get_settings()

PAYMENTS_EXCHANGE_NAME = "payments"
DLX_EXCHANGE_NAME = "payments.dlx"
NEW_QUEUE_NAME = "payments.new"
DLQ_NAME = "payments.new.dlq"
NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.new.dlq"

RETRY_DELAYS_MS = (1000, 2000)

broker = RabbitBroker(settings.rabbitmq_url)

payments_exchange = RabbitExchange(
    PAYMENTS_EXCHANGE_NAME,
    durable=True,
    type=ExchangeType.DIRECT,
)

dlx_exchange = RabbitExchange(
    DLX_EXCHANGE_NAME,
    durable=True,
    type=ExchangeType.DIRECT,
)

new_queue = RabbitQueue(
    NEW_QUEUE_NAME,
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE_NAME,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

dlq = RabbitQueue(
    DLQ_NAME,
    durable=True,
    routing_key=DLQ_ROUTING_KEY,
)

retry_queues = [
    RabbitQueue(
        f"payments.retry.{attempt}",
        durable=True,
        routing_key=f"payments.retry.{attempt}",
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": NEW_ROUTING_KEY,
        },
    )
    for attempt, delay_ms in enumerate(RETRY_DELAYS_MS, start=1)
]


async def _bind(queue_obj, exchange_obj, routing_key: str) -> None:
    bind = getattr(queue_obj, "bind", None)
    if bind is not None:
        await bind(exchange_obj, routing_key=routing_key)


async def declare_topology(rabbit_broker: RabbitBroker) -> None:
    exchange = await rabbit_broker.declare_exchange(payments_exchange)
    dlx = await rabbit_broker.declare_exchange(dlx_exchange)
    queue = await rabbit_broker.declare_queue(new_queue)
    dead = await rabbit_broker.declare_queue(dlq)
    await _bind(queue, exchange, NEW_ROUTING_KEY)
    await _bind(dead, dlx, DLQ_ROUTING_KEY)
    for retry_queue in retry_queues:
        declared_retry = await rabbit_broker.declare_queue(retry_queue)
        await _bind(declared_retry, exchange, retry_queue.routing)

from __future__ import annotations

import logging

from faststream import FastStream
from faststream.rabbit import RabbitMessage
from sqlalchemy import select

from app.broker import (
    broker,
    declare_topology,
    dlq,
    dlx_exchange,
    new_queue,
    payments_exchange,
)
from app.config import get_settings
from app.database import SessionLocal, engine
from app.models import Payment
from app.schemas import PaymentEvent
from app.services.processor import process_payment

settings = get_settings()
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastStream(broker)


@app.after_startup
async def on_startup() -> None:
    await declare_topology(broker)
    logger.info("Consumer started")


@app.after_shutdown
async def on_shutdown() -> None:
    await engine.dispose()
    logger.info("Consumer stopped")


@broker.subscriber(new_queue, payments_exchange, no_ack=True)
async def handle_new_payment(event: PaymentEvent, msg: RabbitMessage) -> None:
    retry_count = int((msg.headers or {}).get("x-retry-count", 0) or 0)
    logger.info("Received payment event %s, attempt %s", event.payment_id, retry_count + 1)

    try:
        async with SessionLocal() as session:
            payment = await session.scalar(
                select(Payment).where(Payment.id == event.payment_id).with_for_update()
            )
            if payment is None:
                raise RuntimeError(f"Payment {event.payment_id} not found")
            await process_payment(session, payment)
    except Exception:
        logger.exception("Failed to process payment %s", event.payment_id)
        if retry_count >= settings.consumer_max_attempts - 1:
            logger.error("Payment %s moved to DLQ after %s attempts", event.payment_id, retry_count + 1)
            await msg.reject(requeue=False)
            return
        next_attempt = retry_count + 1
        try:
            await broker.publish(
                event.model_dump(mode="json"),
                exchange=payments_exchange,
                routing_key=f"payments.retry.{next_attempt}",
                headers={"x-retry-count": str(next_attempt)},
                persist=True,
            )
        except Exception:
            logger.exception("Failed to schedule retry for payment %s", event.payment_id)
            await msg.nack(requeue=True)
            return
        await msg.ack()
        logger.info("Payment %s scheduled for retry %s", event.payment_id, next_attempt)
        return

    await msg.ack()


@broker.subscriber(dlq, dlx_exchange)
async def handle_dead_letter(event: PaymentEvent) -> None:
    logger.error("Payment %s landed in DLQ after exhausting retries", event.payment_id)

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import select

from app.broker import NEW_ROUTING_KEY, broker, payments_exchange
from app.config import get_settings
from app.database import SessionLocal
from app.models import OutboxEvent

logger = logging.getLogger(__name__)


async def publish_unpublished_events() -> int:
    async with SessionLocal() as session:
        events = (
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(100)
                .with_for_update(skip_locked=True)
            )
        ).all()

        published = 0
        for event in events:
            try:
                await broker.publish(
                    event.payload,
                    exchange=payments_exchange,
                    routing_key=NEW_ROUTING_KEY,
                    persist=True,
                )
            except Exception:
                logger.exception("Failed to publish outbox event %s", event.id)
                continue
            event.published_at = datetime.now(UTC)
            published += 1

        await session.commit()
        return published


async def run_outbox_publisher(stop_event: asyncio.Event) -> None:
    settings = get_settings()
    logger.info("Outbox publisher started")
    while not stop_event.is_set():
        try:
            count = await publish_unpublished_events()
            if count:
                logger.info("Published %s outbox event(s)", count)
        except Exception:
            logger.exception("Outbox publisher iteration failed")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.outbox_poll_interval_seconds)
        except TimeoutError:
            continue
    logger.info("Outbox publisher stopped")

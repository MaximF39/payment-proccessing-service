from __future__ import annotations

import asyncio
import logging
import random

from app.config import get_settings

logger = logging.getLogger(__name__)


async def emulate_gateway() -> tuple[bool, str | None]:
    settings = get_settings()
    delay = random.uniform(settings.gateway_min_delay_seconds, settings.gateway_max_delay_seconds)
    logger.info("Emulating payment gateway, delay=%.2fs", delay)
    await asyncio.sleep(delay)
    if random.random() < settings.gateway_success_rate:
        return True, None
    return False, "Payment declined by gateway"

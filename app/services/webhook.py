from __future__ import annotations

import asyncio
import logging

import httpx

from app.config import get_settings
from app.schemas import WebhookPayload

logger = logging.getLogger(__name__)


async def send_webhook(url: str, payload: WebhookPayload) -> None:
    settings = get_settings()
    body = payload.model_dump(mode="json")
    delays = [2**attempt for attempt in range(settings.webhook_max_attempts)]

    last_error: Exception | None = None
    async with httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as client:
        for attempt, delay in enumerate(delays, start=1):
            try:
                response = await client.post(url, json=body)
                response.raise_for_status()
                logger.info("Webhook delivered to %s on attempt %s", url, attempt)
                return
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Webhook attempt %s/%s to %s failed: %s",
                    attempt,
                    settings.webhook_max_attempts,
                    url,
                    exc,
                )
                if attempt < settings.webhook_max_attempts:
                    await asyncio.sleep(delay)

    raise RuntimeError(f"Webhook delivery failed after {settings.webhook_max_attempts} attempts") from last_error

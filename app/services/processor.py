from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment, PaymentStatus
from app.schemas import WebhookPayload
from app.services.gateway import emulate_gateway
from app.services.webhook import send_webhook

logger = logging.getLogger(__name__)


def _webhook_payload(payment: Payment) -> WebhookPayload:
    return WebhookPayload(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.extra_metadata,
        status=payment.status,
        failure_reason=payment.failure_reason,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


async def process_payment(session: AsyncSession, payment: Payment) -> None:
    if payment.status == PaymentStatus.PENDING:
        succeeded, reason = await emulate_gateway()
        payment.status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
        payment.failure_reason = reason
        payment.processed_at = datetime.now(UTC)
        await session.commit()
        await session.refresh(payment)
        logger.info("Payment %s processed with status %s", payment.id, payment.status.value)
    else:
        logger.info("Payment %s already in status %s, skipping gateway", payment.id, payment.status.value)

    if payment.webhook_sent_at is None:
        await send_webhook(payment.webhook_url, _webhook_payload(payment))
        payment.webhook_sent_at = datetime.now(UTC)
        await session.commit()
        logger.info("Webhook stored as delivered for payment %s", payment.id)
    else:
        logger.info("Webhook already delivered for payment %s, skipping", payment.id)

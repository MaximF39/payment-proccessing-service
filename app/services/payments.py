from __future__ import annotations

import logging
import uuid
from decimal import Decimal, ROUND_HALF_UP

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment, PaymentStatus
from app.schemas import PaymentCreate, PaymentResponse

logger = logging.getLogger(__name__)

NEW_PAYMENT_EVENT = "payments.new"


TWOPLACES = Decimal("0.01")


def _same_payload(existing: Payment, payload: PaymentCreate) -> bool:
    return (
        existing.amount.quantize(TWOPLACES, rounding=ROUND_HALF_UP)
        == Decimal(payload.amount).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
        and existing.currency == payload.currency
        and existing.description == payload.description
        and existing.extra_metadata == payload.metadata
        and existing.webhook_url == str(payload.webhook_url)
    )


def to_response(payment: Payment) -> PaymentResponse:
    return PaymentResponse(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.extra_metadata,
        status=payment.status,
        webhook_url=payment.webhook_url,
        failure_reason=payment.failure_reason,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


async def create_payment(
    session: AsyncSession,
    payload: PaymentCreate,
    idempotency_key: str,
) -> tuple[Payment, bool]:
    existing = await session.scalar(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    if existing is not None:
        if not _same_payload(existing, payload):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key already used with a different payload",
            )
        return existing, False

    payment = Payment(
        amount=payload.amount,
        currency=payload.currency,
        description=payload.description,
        extra_metadata=payload.metadata,
        status=PaymentStatus.PENDING,
        idempotency_key=idempotency_key,
        webhook_url=str(payload.webhook_url),
    )
    session.add(payment)
    await session.flush()

    session.add(
        OutboxEvent(
            event_type=NEW_PAYMENT_EVENT,
            payload={"payment_id": str(payment.id)},
        )
    )

    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raced = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if raced is None:
            raise
        if not _same_payload(raced, payload):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key already used with a different payload",
            )
        return raced, False

    await session.refresh(payment)
    logger.info("Created payment %s", payment.id)
    return payment, True


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return payment

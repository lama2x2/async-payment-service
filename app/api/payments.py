import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.api.deps import SessionDep, verify_api_key
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from app.services import payments as service

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={409: {"description": "Idempotency key reused with a different body"}},
)
async def create_payment(
    data: PaymentCreate,
    session: SessionDep,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
) -> PaymentAccepted:
    try:
        payment = await service.create_payment(session, data, idempotency_key)
    except service.IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request",
        ) from None
    return PaymentAccepted.model_validate(payment)


@router.get("/{payment_id}", response_model=PaymentRead)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentRead:
    payment = await service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentRead.model_validate(payment)

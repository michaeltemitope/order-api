from pydantic import BaseModel, EmailStr, Field

from .models import OrderStatus


class OrderItemIn(BaseModel):
    sku: str
    qty: int = Field(gt=0)
    unit_price_cents: int = Field(gt=0)


class CreateOrderRequest(BaseModel):
    customer_email: EmailStr
    items: list[OrderItemIn] = Field(min_length=1)


class OrderOut(BaseModel):
    id: str
    customer_email: str
    items: list[dict]
    total_cents: int
    status: OrderStatus

    class Config:
        from_attributes = True


class PaymentFailedOut(BaseModel):
    id: str
    status: OrderStatus
    reason: str

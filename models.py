from pydantic import BaseModel
from typing import Optional


class TrainerRegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    phone: str = ""


class TrainerLoginRequest(BaseModel):
    email: str
    password: str


class TrainerSettingsRequest(BaseModel):
    name: str
    work_start: int
    work_end: int
    slot_duration: int
    timezone: str = "Europe/Moscow"


class ClientCreateRequest(BaseModel):
    name: str
    phone: str = ""
    gender: str = "male"
    telegram_username: str = ""
    notes: str = ""


class ClientUpdateRequest(BaseModel):
    name: str
    phone: str = ""
    notes: str = ""


class SessionCreateRequest(BaseModel):
    client_id: int
    session_date: str
    time: str
    duration_min: int = 60
    notes: str = ""


class SessionStatusRequest(BaseModel):
    status: str


class SubscriptionCreateRequest(BaseModel):
    client_id: int
    total_sessions: int
    price_total: int
    purchase_date: str
    expiry_date: Optional[str] = None


class PaymentCreateRequest(BaseModel):
    client_id: int
    amount: int
    description: str = ""
    payment_date: str
    is_paid: bool = False


class ClientBodyRequest(BaseModel):
    height_cm: float
    weight_kg: float
    age: int
    goal: str = ""
    health_notes: str = ""
    measured_at: str


class WorkoutProgramRequest(BaseModel):
    title: str
    content: str


class ClientLoginRequest(BaseModel):
    pin_code: str


class CycleRequest(BaseModel):
    cycle_start_date: str
    cycle_length_days: int = 28


class PRRequest(BaseModel):
    exercise_name: str
    weight_kg: float
    reps: int
    recorded_at: str

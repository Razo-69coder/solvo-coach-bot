import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import jwt
import hashlib
import hmac
import os as _os
import httpx
from fastapi import FastAPI, Header, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from database import (
    init_db,
    get_trainer_by_email, create_trainer, get_trainer_by_id, update_trainer_settings,
    set_trainer_active, get_all_trainers,
    get_clients_page, search_clients, get_client, create_client,
    update_client, delete_client, get_client_session_history,
    get_schedule, get_available_slots, create_session,
    update_session_status, complete_session,
    get_subscriptions, create_subscription, deactivate_subscription,
    get_active_subscription,
    get_payments, create_payment, mark_payment_paid,
    get_stats,
    get_client_body, add_client_body, get_client_body_history,
    get_workout_program, save_workout_program,
    generate_client_pin, get_client_by_pin, get_client_schedule,
    get_client_analytics,
    save_cycle_start, get_cycle_phase,
    save_pr, get_pr_list, get_pr_history,
    get_client_weekly_report,
)
from models import (
    TrainerRegisterRequest, TrainerLoginRequest, TrainerSettingsRequest,
    ClientCreateRequest, ClientUpdateRequest,
    SessionCreateRequest, SessionStatusRequest,
    SubscriptionCreateRequest, PaymentCreateRequest,
    ClientBodyRequest, WorkoutProgramRequest,
    ClientLoginRequest,
    CycleRequest,
    PRRequest,
)

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET", "sc_secret_fallback")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")


# ─── Lifespan ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title="Solvo Fit API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── JWT ──────────────────────────────────────────────────

def generate_jwt(trainer_id: int) -> str:
    payload = {"tid": trainer_id, "exp": datetime.utcnow() + timedelta(days=365)}
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def decode_jwt(token: str) -> dict | None:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.InvalidTokenError:
        return None


def create_admin_token() -> str:
    payload = {"role": "admin", "exp": datetime.utcnow() + timedelta(hours=12)}
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def generate_client_jwt(client_id: int, trainer_id: int) -> str:
    payload = {"cid": client_id, "tid": trainer_id, "role": "client",
               "exp": datetime.utcnow() + timedelta(days=365)}
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


async def get_current_client(authorization: str = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Требуется авторизация")
    payload = decode_jwt(authorization[7:])
    if not payload or payload.get("role") != "client":
        raise HTTPException(401, "Неверный токен клиента")
    return {"client_id": int(payload["cid"]), "trainer_id": int(payload["tid"])}


async def get_current_trainer_id(authorization: str = Header(None)) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Требуется авторизация")
    payload = decode_jwt(authorization[7:])
    if not payload:
        raise HTTPException(401, "Неверный или устаревший токен")
    trainer_id = payload.get("tid")
    if not trainer_id:
        raise HTTPException(401, "Неверный токен")
    return int(trainer_id)


async def verify_admin_token(authorization: str = Header(None)) -> bool:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Требуется авторизация")
    try:
        token = authorization.replace("Bearer ", "")
        data = jose_jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        if data.get("role") != "admin":
            raise HTTPException(403, "Нет доступа")
        return True
    except jose_jwt.InvalidTokenError:
        raise HTTPException(401, "Неверный токен администратора")


# ─── Telegram ─────────────────────────────────────────────

async def send_tg_message(chat_id: int, text: str):
    if not BOT_TOKEN:
        return
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            )
        except Exception:
            pass


def _verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt, key = stored_hash.split(":")
        return hmac.compare_digest(
            hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260000).hex(), key
        )
    except Exception:
        return False


# ─── Auth ─────────────────────────────────────────────────

@app.post("/api/v1/auth/register")
async def register(body: TrainerRegisterRequest):
    existing = await get_trainer_by_email(body.email)
    if existing:
        raise HTTPException(400, "Email уже занят")

    salt = _os.urandom(16).hex()
    password_hash = salt + ":" + hashlib.pbkdf2_hmac("sha256", body.password.encode(), salt.encode(), 260000).hex()
    trainer_id = await create_trainer(body.email, password_hash, body.name, body.phone)
    trainer = await get_trainer_by_id(trainer_id)
    if not trainer:
        raise HTTPException(500, "Ошибка создания тренера")

    token = generate_jwt(trainer_id)
    return {"token": token, "trainer": trainer}


@app.post("/api/v1/auth/login")
async def login(body: TrainerLoginRequest):
    trainer = await get_trainer_by_email(body.email)
    if not trainer or not _verify_password(body.password, trainer["password_hash"]):
        raise HTTPException(401, "Неверный email или пароль")

    token = generate_jwt(trainer["id"])
    trainer["password_hash"] = ""  # не возвращаем хеш
    return {"token": token, "trainer": trainer}


# ─── Trainer ──────────────────────────────────────────────

@app.get("/api/v1/trainers/me")
async def get_me(trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    trainer = await get_trainer_by_id(trainer_id)
    if not trainer:
        raise HTTPException(404, "Тренер не найден")
    return trainer


@app.put("/api/v1/trainers/me")
async def update_settings(body: TrainerSettingsRequest, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    await update_trainer_settings(
        trainer_id, body.name, body.work_start, body.work_end,
        body.slot_duration, body.timezone
    )
    return {"ok": True}


@app.get("/api/v1/trainers/me/stats")
async def get_my_stats(trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    return await get_stats(trainer_id)


# ─── Clients ──────────────────────────────────────────────

@app.get("/api/v1/clients")
async def list_clients(
    page: int = Query(0),
    search: str = Query(""),
    trainer_id: int = Depends(get_current_trainer_id),  # type: ignore
):
    if search:
        results = await search_clients(trainer_id, search)
        return {"clients": results, "total": len(results), "page": 0}
    clients, total = await get_clients_page(trainer_id, page)
    return {"clients": clients, "total": total, "page": page}


@app.post("/api/v1/clients", status_code=201)
async def create_client_endpoint(body: ClientCreateRequest, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    client_id = await create_client(
        trainer_id, body.name, body.phone, body.gender,
        body.telegram_username, body.notes
    )
    return {"id": client_id}


@app.get("/api/v1/clients/{client_id}")
async def get_client_detail(client_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    client = await get_client(client_id, trainer_id)
    if not client:
        raise HTTPException(404, "Клиент не найден")
    history = await get_client_session_history(client_id)
    active_sub = await get_active_subscription(client_id, trainer_id)
    return {"client": client, "history": history, "active_subscription": active_sub}


@app.get("/api/v1/clients/{client_id}/analytics")
async def get_client_analytics_endpoint(client_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    client = await get_client(client_id, trainer_id)
    if not client:
        raise HTTPException(404, "Клиент не найден")
    return await get_client_analytics(client_id, trainer_id)


@app.put("/api/v1/clients/{client_id}")
async def update_client_endpoint(client_id: int, body: ClientUpdateRequest, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    ok = await update_client(client_id, trainer_id, body.name, body.phone, body.notes)
    if not ok:
        raise HTTPException(404, "Клиент не найден")
    return {"ok": True}


@app.delete("/api/v1/clients/{client_id}")
async def delete_client_endpoint(client_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    ok = await delete_client(client_id, trainer_id)
    if not ok:
        raise HTTPException(404, "Клиент не найден")
    return {"ok": True}


# ─── Schedule / Sessions ──────────────────────────────────

@app.get("/api/v1/schedule")
async def get_schedule_endpoint(date: str = Query(...), trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    sessions = await get_schedule(trainer_id, date)
    return {"date": date, "sessions": sessions}


@app.get("/api/v1/slots")
async def get_slots_endpoint(date: str = Query(...), trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    trainer = await get_trainer_by_id(trainer_id)
    if not trainer:
        raise HTTPException(404, "Тренер не найден")
    slots = await get_available_slots(
        trainer_id, date, trainer["work_start"], trainer["work_end"], trainer["slot_duration"]
    )
    return {"date": date, "slots": slots}


@app.post("/api/v1/sessions", status_code=201)
async def create_session_endpoint(body: SessionCreateRequest, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    session_id = await create_session(
        trainer_id, body.client_id, body.session_date, body.time,
        body.duration_min, body.notes
    )
    return {"id": session_id}


@app.put("/api/v1/sessions/{session_id}/cancel")
async def cancel_session(session_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    ok = await update_session_status(session_id, trainer_id, "cancelled")
    if not ok:
        raise HTTPException(404, "Тренировка не найдена")
    return {"ok": True}


@app.put("/api/v1/sessions/{session_id}/complete")
async def complete_session_endpoint(session_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    ok = await complete_session(session_id, trainer_id)
    if not ok:
        raise HTTPException(404, "Тренировка не найдена")
    return {"ok": True}


# ─── Subscriptions ────────────────────────────────────────

@app.get("/api/v1/subscriptions")
async def list_subscriptions(
    client_id: Optional[int] = Query(None),
    trainer_id: int = Depends(get_current_trainer_id),  # type: ignore
):
    subs = await get_subscriptions(trainer_id, client_id)
    return {"subscriptions": subs}


@app.post("/api/v1/subscriptions", status_code=201)
async def create_subscription_endpoint(body: SubscriptionCreateRequest, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    sub_id = await create_subscription(
        trainer_id, body.client_id, body.total_sessions, body.price_total,
        body.purchase_date, body.expiry_date
    )
    return {"id": sub_id}


@app.put("/api/v1/subscriptions/{sub_id}/deactivate")
async def deactivate_subscription_endpoint(sub_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    ok = await deactivate_subscription(sub_id, trainer_id)
    if not ok:
        raise HTTPException(404, "Абонемент не найден")
    return {"ok": True}


# ─── Client Body ──────────────────────────────────────────

@app.get("/api/v1/clients/{client_id}/body")
async def get_client_body_endpoint(client_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    body = await get_client_body(client_id, trainer_id)
    if not body:
        raise HTTPException(404, "Данные не найдены")
    return body


@app.post("/api/v1/clients/{client_id}/body", status_code=201)
async def add_client_body_endpoint(client_id: int, body: ClientBodyRequest, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    row_id = await add_client_body(
        client_id, trainer_id, body.height_cm, body.weight_kg,
        body.age, body.goal, body.health_notes, body.measured_at
    )
    return {"id": row_id}


@app.get("/api/v1/clients/{client_id}/body/history")
async def get_client_body_history_endpoint(client_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    history = await get_client_body_history(client_id)
    return {"history": history}


# ─── Workout Programs ─────────────────────────────────────

@app.get("/api/v1/clients/{client_id}/program")
async def get_program_endpoint(client_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    program = await get_workout_program(client_id, trainer_id)
    if not program:
        raise HTTPException(404, "Программа не найдена")
    return program


@app.post("/api/v1/clients/{client_id}/program", status_code=201)
async def save_program_endpoint(client_id: int, body: WorkoutProgramRequest, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    row_id = await save_workout_program(client_id, trainer_id, body.title, body.content)
    return {"id": row_id}


# ─── Payments ─────────────────────────────────────────────

@app.get("/api/v1/payments")
async def list_payments(
    client_id: Optional[int] = Query(None),
    trainer_id: int = Depends(get_current_trainer_id),  # type: ignore
):
    payments, total_debt = await get_payments(trainer_id, client_id)
    return {"payments": payments, "total_debt": total_debt}


@app.post("/api/v1/payments", status_code=201)
async def create_payment_endpoint(body: PaymentCreateRequest, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    payment_id = await create_payment(
        trainer_id, body.client_id, body.amount, body.description,
        body.payment_date, body.is_paid
    )
    return {"id": payment_id}


@app.put("/api/v1/payments/{payment_id}/paid")
async def mark_paid(payment_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    ok = await mark_payment_paid(payment_id, trainer_id)
    if not ok:
        raise HTTPException(404, "Платёж не найден")
    return {"ok": True}


# ─── Admin ────────────────────────────────────────────────

@app.post("/api/v1/admin/login")
async def admin_login(body: dict):
    if body.get("password") != ADMIN_PASSWORD:
        raise HTTPException(401, "Неверный пароль администратора")
    token = create_admin_token()
    return {"token": token}


@app.get("/api/v1/admin/trainers")
async def admin_list_trainers():
    trainers = await get_all_trainers()
    return {"trainers": trainers}


@app.put("/api/v1/admin/trainers/{trainer_id}/toggle")
async def admin_toggle_active(trainer_id: int):
    trainer = await get_trainer_by_id(trainer_id)
    if not trainer:
        raise HTTPException(404, "Тренер не найден")
    new_active = not trainer["is_active"]
    await set_trainer_active(trainer_id, new_active)
    return {"ok": True, "is_active": new_active}


# ─── Client Auth ──────────────────────────────────────────

@app.post("/api/v1/client/login")
async def client_login(body: ClientLoginRequest):
    data = await get_client_by_pin(body.pin_code)
    if not data:
        raise HTTPException(401, "Неверный PIN-код")
    token = generate_client_jwt(data["client_id"], data["trainer_id"])
    return {"token": token, "client": data}


@app.post("/api/v1/clients/{client_id}/pin")
async def create_client_pin(client_id: int, trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    client = await get_client(client_id, trainer_id)
    if not client:
        raise HTTPException(404, "Клиент не найден")
    pin = await generate_client_pin(client_id, trainer_id)
    return {"pin": pin}


# ─── Client API (клиентский режим) ────────────────────────

@app.get("/api/v1/client/me")
async def client_me(current: dict = Depends(get_current_client)):
    client = await get_client(current["client_id"], current["trainer_id"])
    if not client:
        raise HTTPException(404, "Клиент не найден")
    return client


@app.get("/api/v1/client/schedule")
async def client_schedule(date: str = Query(...), current: dict = Depends(get_current_client)):
    sessions = await get_client_schedule(current["client_id"], date)
    return {"date": date, "sessions": sessions}


@app.get("/api/v1/client/program")
async def client_program(current: dict = Depends(get_current_client)):
    program = await get_workout_program(current["client_id"], current["trainer_id"])
    if not program:
        raise HTTPException(404, "Программа не найдена")
    return program


@app.get("/api/v1/client/body")
async def client_body(current: dict = Depends(get_current_client)):
    body = await get_client_body(current["client_id"], current["trainer_id"])
    if not body:
        raise HTTPException(404, "Данные не найдены")
    return body


@app.get("/api/v1/client/body/history")
async def client_body_history(current: dict = Depends(get_current_client)):
    history = await get_client_body_history(current["client_id"])
    return {"history": history}


# ─── Cycle Tracker ─────────────────────────────────────────

@app.post("/api/v1/client/cycle")
async def post_client_cycle(body: CycleRequest, current: dict = Depends(get_current_client)):
    await save_cycle_start(current["client_id"], current["trainer_id"], body.cycle_start_date, body.cycle_length_days)
    return {"message": "ok"}


@app.get("/api/v1/client/cycle")
async def get_my_cycle(current: dict = Depends(get_current_client)):
    return await get_cycle_phase(current["client_id"])


@app.get("/api/v1/clients/{client_id}/cycle")
async def get_client_cycle(client_id: int, trainer_id: int = Depends(get_current_trainer_id)):
    return await get_cycle_phase(client_id)


# ─── PR Records ─────────────────────────────────────────────

@app.post("/api/v1/client/pr")
async def add_pr(body: PRRequest, current: dict = Depends(get_current_client)):
    await save_pr(current["client_id"], current["trainer_id"], body.exercise_name, body.weight_kg, body.reps, body.recorded_at)
    return {"message": "ok"}


@app.get("/api/v1/client/pr")
async def get_my_prs(current: dict = Depends(get_current_client)):
    return await get_pr_list(current["client_id"])


@app.get("/api/v1/client/pr/{exercise_name}/history")
async def get_pr_history_route(exercise_name: str, current: dict = Depends(get_current_client)):
    return await get_pr_history(current["client_id"], exercise_name)


@app.get("/api/v1/clients/{client_id}/pr")
async def get_client_prs(client_id: int, trainer_id: int = Depends(get_current_trainer_id)):
    return await get_pr_list(client_id)


# ─── Weekly Report ───────────────────────────────────────────

@app.get("/api/v1/client/weekly-report")
async def client_weekly_report(
    week_offset: int = Query(0),
    current: dict = Depends(get_current_client),
):
    return await get_client_weekly_report(
        current["client_id"], current["trainer_id"], week_offset
    )


# ─── Health ───────────────────────────────────────────────

@app.get("/")
async def root():
    return {"ok": True, "app": "Solvo Fit API", "version": "1.0.0"}

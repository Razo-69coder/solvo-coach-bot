import os
import json
import re
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import jwt
import hashlib
import hmac
import os as _os
import httpx
import base64
from fastapi import FastAPI, Header, HTTPException, Query, Depends, File, UploadFile, Form
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
    get_client_progress_summary,
    get_muscle_map,
    get_leaderboard,
    get_client_rank,
    get_templates,
    get_template_detail,
    create_template,
    delete_template,
    apply_template_to_client,
    get_trainer_tier,
    get_cal_ai_daily_count,
    save_cal_ai_log,
    save_onboarding_meta,
    parse_import_file,
    get_churn_risk,
    get_client_profitability,
    get_supplements,
    create_supplement,
    update_supplement,
    delete_supplement,
    get_cal_ai_history,
    save_body_analysis,
    get_body_analysis_history,
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
    TemplateCreateRequest,
    OnboardingMetaRequest,
    SupplementCreateRequest,
    SupplementUpdateRequest,
)

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET", "sc_secret_fallback")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
SUPPORT_BOT_TOKEN = os.getenv("SUPPORT_BOT_TOKEN", "")
ADMIN_TG_ID = int(os.getenv("ADMIN_TG_ID", "550421233"))


# ─── Lifespan ─────────────────────────────────────────────

async def _db_keepalive():
    from database import get_pool
    while True:
        await asyncio.sleep(240)  # ping every 4 min (Neon suspends after 5 min idle)
        try:
            pool = await get_pool()
            async with pool.connection() as conn:
                await conn.execute("SELECT 1")
            print("[KEEPALIVE] DB ping ok")
        except Exception as e:
            print(f"[KEEPALIVE] DB ping failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await asyncio.wait_for(init_db(), timeout=120)
    except asyncio.TimeoutError:
        print("WARNING: init_db timed out after 120s, starting anyway")
    except Exception as e:
        print(f"WARNING: init_db failed: {e}, starting anyway")
    task = asyncio.create_task(_db_keepalive())
    yield
    task.cancel()


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
    email = body.email.strip().lower()
    print(f"[REGISTER] Email: {email}, Name: {body.name}, Phone: {body.phone}")

    existing = await get_trainer_by_email(email)
    print(f"[REGISTER] Existing check: {existing}")
    if existing:
        raise HTTPException(400, "Email уже занят")

    salt = _os.urandom(16).hex()
    password_hash = salt + ":" + hashlib.pbkdf2_hmac("sha256", body.password.encode(), salt.encode(), 260000).hex()
    trainer_id = await create_trainer(email, password_hash, body.name, body.phone)
    trainer = await get_trainer_by_id(trainer_id)
    print(f"[REGISTER] Created trainer: {trainer_id}, Trainer: {trainer}")
    if not trainer:
        raise HTTPException(500, "Ошибка создания тренера")

    token = generate_jwt(trainer_id)
    return {"token": token, "trainer": trainer}


@app.post("/api/v1/auth/login")
async def login(body: TrainerLoginRequest):
    email = body.email.strip().lower()
    print(f"[LOGIN] Email: {email}")
    trainer = await get_trainer_by_email(email)
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


@app.get("/api/v1/trainer/churn-risk")
async def churn_risk(trainer_id: int = Depends(get_current_trainer_id)):  # type: ignore
    clients = await get_churn_risk(trainer_id)
    return {"clients": clients}


@app.get("/api/v1/trainer/client-profitability")
async def client_profitability(trainer_id: int = Depends(get_current_trainer_id)):
    return {"clients": await get_client_profitability(trainer_id)}


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


# ─── Progress Summary ────────────────────────────────────────

@app.get("/api/v1/client/progress-summary")
async def client_progress_summary(current: dict = Depends(get_current_client)):
    return await get_client_progress_summary(
        current["client_id"], current["trainer_id"]
    )


# ─── Muscle Map ─────────────────────────────────────────────

@app.get("/api/v1/client/muscle-map")
async def client_muscle_map(
    days: int = Query(7),
    current: dict = Depends(get_current_client),
):
    return await get_muscle_map(current["client_id"], days)


# ─── Leaderboard ────────────────────────────────────────────

@app.get("/api/v1/trainer/leaderboard")
async def trainer_leaderboard(
    month: str = Query(...),
    trainer_id: int = Depends(get_current_trainer_id),
):
    return await get_leaderboard(trainer_id, month)


@app.get("/api/v1/client/my-rank")
async def client_my_rank(
    month: str = Query(...),
    current: dict = Depends(get_current_client),
):
    return await get_client_rank(current["client_id"], current["trainer_id"], month)


# ─── Supplements ──────────────────────────────────────────


@app.get("/api/v1/client/supplements")
async def client_supplements(current: dict = Depends(get_current_client)):
    return {"supplements": await get_supplements(current["client_id"])}


@app.post("/api/v1/client/supplements", status_code=201)
async def client_create_supplement(
    body: SupplementCreateRequest,
    current: dict = Depends(get_current_client),
):
    sid = await create_supplement(current["client_id"], body.name, body.dose, body.time_of_day, body.notes)
    return {"id": sid}


@app.put("/api/v1/client/supplements/{supplement_id}")
async def client_update_supplement(
    supplement_id: int,
    body: SupplementUpdateRequest,
    current: dict = Depends(get_current_client),
):
    ok = await update_supplement(supplement_id, body.name, body.dose, body.time_of_day, body.notes)
    if not ok:
        raise HTTPException(404, "Добавка не найдена")
    return {"ok": True}


@app.delete("/api/v1/client/supplements/{supplement_id}")
async def client_delete_supplement(
    supplement_id: int,
    current: dict = Depends(get_current_client),
):
    ok = await delete_supplement(supplement_id)
    if not ok:
        raise HTTPException(404, "Добавка не найдена")
    return {"ok": True}


@app.get("/api/v1/trainer/clients/{client_id}/supplements")
async def trainer_client_supplements(
    client_id: int,
    trainer_id: int = Depends(get_current_trainer_id),
):
    return {"supplements": await get_supplements(client_id)}


# ─── Program Templates ─────────────────────────────────────

@app.post("/api/v1/trainer/templates", status_code=201)
async def create_template_endpoint(
    body: TemplateCreateRequest,
    trainer_id: int = Depends(get_current_trainer_id),
):
    tid = await create_template(
        trainer_id, body.name, body.description,
        body.duration_weeks, body.level, body.goal,
        [d.model_dump() for d in body.days]
    )
    return {"id": tid}


@app.get("/api/v1/trainer/templates")
async def list_templates(trainer_id: int = Depends(get_current_trainer_id)):
    templates = await get_templates(trainer_id)
    return {"templates": templates}


@app.get("/api/v1/trainer/templates/{template_id}")
async def get_template_endpoint(
    template_id: int,
    trainer_id: int = Depends(get_current_trainer_id),
):
    template = await get_template_detail(template_id)
    if not template:
        raise HTTPException(404, "Шаблон не найден")
    if template["trainer_id"] != trainer_id:
        raise HTTPException(403, "Нет доступа к этому шаблону")
    return template


@app.delete("/api/v1/trainer/templates/{template_id}")
async def delete_template_endpoint(
    template_id: int,
    trainer_id: int = Depends(get_current_trainer_id),
):
    ok = await delete_template(template_id, trainer_id)
    if not ok:
        raise HTTPException(404, "Шаблон не найден")
    return {"ok": True}


@app.post("/api/v1/trainer/templates/{template_id}/apply/{client_id}")
async def apply_template_endpoint(
    template_id: int,
    client_id: int,
    trainer_id: int = Depends(get_current_trainer_id),
):
    try:
        title = await apply_template_to_client(template_id, client_id, trainer_id)
        return {"message": f"Программа '{title}' применена к клиенту", "ok": True}
    except ValueError as e:
        raise HTTPException(404, str(e))


# ─── Support ─────────────────────────────────────────────

async def send_support_message(text: str):
    if not SUPPORT_BOT_TOKEN:
        return
    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{SUPPORT_BOT_TOKEN}/sendMessage",
                json={"chat_id": ADMIN_TG_ID, "text": text, "parse_mode": "Markdown"}
            )
        except Exception:
            pass


@app.post("/api/v1/support")
async def support_message(body: dict, trainer_id: int = Depends(get_current_trainer_id)):
    trainer = await get_trainer_by_id(trainer_id)
    msg_type = body.get("type", "support")
    message = body.get("message", "")
    trainer_name = trainer.get("name", "Неизвестный") if trainer else "Неизвестный"
    trainer_email = trainer.get("email", "") if trainer else ""

    emoji = "🆘" if msg_type == "support" else "💡"
    text = f"{emoji} *{'Поддержка' if msg_type == 'support' else 'Запрос функции'}*\n\n👤 {trainer_name} ({trainer_email})\n\n💬 {message}"

    await send_support_message(text)
    return {"ok": True}


# ─── Cal AI ──────────────────────────────────────────────

CLAUDE_PROMPT_TEMPLATE = (
    "Ты эксперт-нутрициолог. Проанализируй фото еды по шагам:\n\n"
    "ШАГ 1 — ПОЛЬЗОВАТЕЛЬСКИЕ ДАННЫЕ:\n"
    "Пользователь написал: {dish_name_hint}. Общий вес если указан: {weight_g}г.\n"
    "Если пользователь указал граммы конкретного ингредиента — используй точно.\n"
    "Если граммы не указаны — определи пропорции САМОСТОЯТЕЛЬНО по фото:\n"
    "  смотри какой ингредиент занимает больше места на тарелке → ему больше граммов\n"
    "  используй визуальные пропорции: если риса в 2 раза больше курицы → риса в 2 раза больше граммов\n"
    "  если указан общий вес — распредели пропорционально визуальным объёмам на фото\n"
    "Источник фото: {photo_source}. "
    "Если photo_source=gallery — LiDAR недоступен, оцени объём визуально: "
    "используй размер тарелки, высоту еды, плотность ингредиентов.\n"
    "Если extra={extra} — учти обязательно.\n\n"
    "ШАГ 2 — АНАЛИЗ ФОТО:\n"
    "- Используй тарелку/ладонь/столовые приборы на фото как ориентир размера\n"
    "- Стандартная тарелка = 24-26 см диаметром. Оцени какую часть тарелки занимает еда\n"
    "- Если пользователь указал вес — используй его. Если нет — оцени по ориентирам на фото\n"
    "- Ингредиенты: называй то что видишь, для овощей допустима небольшая неточность в названии "
    "(помидор vs красный перец) — важнее точный вес\n\n"
    "ШАГ 3 — КБЖУ по USDA:\n"
    "Рассчитай КБЖУ для каждого ингредиента по весу, затем сложи итог.\n\n"
    "Верни ТОЛЬКО JSON:\n"
    "{{\"calories\": int, \"protein\": float, \"fat\": float, \"carbs\": float, "
    "\"dish_name\": string, \"confidence\": \"высокая/средняя/низкая\", \"note\": string}}\n"
    "В note — напиши из чего считал и какой вес взял за основу."
)

AUTO_CAL_AI_PROMPT = (
    "You are a nutrition expert. Analyze this food photo carefully.\n\n"
    "PORTION SIZE — use visual references on the plate:\n"
    "- Standard plate diameter = 24-26 cm. Estimate what fraction of the plate the food occupies\n"
    "- Use any visible reference objects (spoon, fork, hand) to estimate scale\n"
    "- Count individual pieces if visible (e.g. '5 small chicken pieces ≈ 250g total')\n"
    "- For soups/stews: 60-80% is liquid, estimate solids separately\n\n"
    "Identify: dish name, all ingredients with individual weights, cooking method.\n\n"
    "Respond ONLY with JSON:\n"
    "{{\"dish_name\": \"название на русском\", \"weight_g\": int, \"calories\": int, "
    "\"protein\": float, \"fat\": float, \"carbs\": float, \"fiber_g\": float, "
    "\"ingredients\": [\"ingredient 100g\", \"ingredient2 150g\"], "
    "\"cooking_method\": \"варёное/жареное/запечённое/тушёное\", "
    "\"confidence\": \"high/medium/low\", "
    "\"note\": \"как определил вес\"}}"
)


async def call_claude_vision(photo_base64: str, prompt: str) -> dict:
    print("ANTHROPIC_API_KEY present:", bool(os.getenv("ANTHROPIC_API_KEY")))
    print("Image size:", len(photo_base64) if photo_base64 else 0)
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY не настроен")
    body = {
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 1024,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image", "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": photo_base64,
                }},
                {"type": "text", "text": prompt},
            ],
        }],
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=body,
        )
    if resp.status_code != 200:
        print("Claude response error:", resp.text)
        raise HTTPException(502, f"Claude API error: {resp.status_code} {resp.text}")
    data = resp.json()
    content = data.get("content", [])
    for block in content:
        if block.get("type") == "text":
            text = block["text"].strip()
            m = re.search(r"\{.*\}", text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group())
                except json.JSONDecodeError as e:
                    raise HTTPException(502, f"Не удалось распарсить ответ Claude: {e}")
    raise HTTPException(502, "Claude не вернул текстовый ответ")


def _get_cal_ai_user(authorization: str) -> dict:
    """Parse JWT and return {user_id, user_role, trainer_id}."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Требуется авторизация")
    payload = decode_jwt(authorization[7:])
    if not payload:
        raise HTTPException(401, "Неверный токен")
    role = payload.get("role", "trainer")
    if role == "client":
        return {
            "user_id": int(payload["cid"]),
            "user_role": "client",
            "trainer_id": int(payload["tid"]),
        }
    return {
        "user_id": int(payload["tid"]),
        "user_role": "trainer",
        "trainer_id": int(payload["tid"]),
    }


@app.post("/api/v1/cal-ai/analyze")
async def cal_ai_analyze(
    photo: UploadFile = File(...),
    dish_type: str = Form(...),
    cooking_method: str = Form(...),
    sauce: str = Form(...),
    portion_size: str = Form(...),
    extra: str = Form(""),
    dish_name_hint: str = Form(""),
    weight_g: str = Form(""),
    lidar_data: str = Form(""),
    photo_source: str = Form("camera"),
    authorization: str = Header(None),
):
    user = _get_cal_ai_user(authorization)
    tier = await get_trainer_tier(user["trainer_id"])

    if user["user_role"] == "client":
        if tier < 2:
            raise HTTPException(403, "Cal AI недоступен на вашем тарифе")
        daily = await get_cal_ai_daily_count(user["user_id"])
        if tier == 2 and daily >= 4:
            raise HTTPException(429, "Дневной лимит 4 запроса исчерпан")
        if tier >= 3 and daily >= 8:
            raise HTTPException(429, "Дневной лимит 8 запросов исчерпан")

    photo_data = await photo.read()
    print("Image received, size:", len(photo_data) if photo_data else 0)
    photo_base64 = base64.b64encode(photo_data).decode()

    if not dish_type or dish_type == "auto":
        prompt = AUTO_CAL_AI_PROMPT
    else:
        prompt = CLAUDE_PROMPT_TEMPLATE.format(
            dish_name_hint=dish_name_hint or "не указано",
            dish_type=dish_type,
            cooking_method=cooking_method,
            sauce=sauce,
            portion_size=portion_size,
            extra=extra,
            weight_g=weight_g if weight_g else "не указан",
            lidar_data=lidar_data if lidar_data else "нет",
            photo_source=photo_source,
        )

    result = await call_claude_vision(photo_base64, prompt)

    request_data = {
        "dish_type": dish_type,
        "cooking_method": cooking_method,
        "sauce": sauce,
        "portion_size": portion_size,
        "extra": extra,
    }
    await save_cal_ai_log(
        user_id=user["user_id"],
        user_role=user["user_role"],
        trainer_id=user["trainer_id"],
        request_data=request_data,
        result=result,
    )

    return result


@app.get("/api/v1/cal-ai/history")
async def cal_ai_history(authorization: str = Header(None)):
    try:
        user = _get_cal_ai_user(authorization)
        items = await get_cal_ai_history(user["user_id"])
        print(f"[cal_ai_history] user_id={user['user_id']} items_count={len(items)}")
        return {"items": items}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[cal_ai_history] error: {e}")
        raise HTTPException(500, "Внутренняя ошибка сервера")


# ─── Onboarding ────────────────────────────────────────────

@app.post("/api/v1/trainer/onboarding-meta")
async def save_onboarding_meta_endpoint(body: OnboardingMetaRequest):
    await save_onboarding_meta(
        body.clients_count, body.work_type,
        body.experience, body.current_tool, body.referral_source
    )
    return {"ok": True, "message": "Метаданные онбординга сохранены"}


# ─── Import Excel ─────────────────────────────────────────

@app.post("/api/v1/import/excel")
async def import_excel(file: UploadFile = File(...)):
    data = await file.read()
    clients, errors = await parse_import_file(data, file.filename or "file.csv")
    return {"clients": clients, "total": len(clients), "errors": errors}


# ─── Body Analysis ───────────────────────────────────────

MOCK_BODY_ANALYSIS_RESPONSE = {
    "body_analysis": "Эктоморфное телосложение, узкие плечи, небольшая сутулость в грудном отделе. "
                     "Слабые группы: спина, задняя поверхность бедра, ягодицы. "
                     "Рекомендуется акцент на тяговые движения и укрепление кора.",
    "program": "Программа на месяц: 3 тренировки в неделю. Первые 2 недели — "
               "адаптация и постановка техники. Третья неделя — прогрессия нагрузки. "
               "Четвёртая неделя — интенсивный цикл с суперсетами.",
    "weeks": [
        {
            "week": 1,
            "days": [
                {
                    "day": 1,
                    "exercises": [
                        {"name": "Приседания с собственным весом", "sets": 3, "reps": 12, "note": "Медленный темп"},
                        {"name": "Тяга гантели к поясу", "sets": 3, "reps": 10, "note": "Лёгкий вес"},
                        {"name": "Ягодичный мостик", "sets": 3, "reps": 15, "note": ""}
                    ]
                },
                {
                    "day": 2,
                    "exercises": [
                        {"name": "Отжимания от пола", "sets": 3, "reps": 8, "note": "С колен если тяжело"},
                        {"name": "Планка", "sets": 3, "reps": 1, "note": "30 секунд"},
                        {"name": "Выпады назад", "sets": 3, "reps": 10, "note": "На каждую ногу"}
                    ]
                }
            ]
        },
        {
            "week": 2,
            "days": [
                {
                    "day": 1,
                    "exercises": [
                        {"name": "Приседания с гантелью", "sets": 3, "reps": 10, "note": "Вес 6-8 кг"},
                        {"name": "Тяга гантели двумя руками", "sets": 3, "reps": 12, "note": ""},
                        {"name": "Подъём ног лёжа", "sets": 3, "reps": 12, "note": ""}
                    ]
                }
            ]
        },
        {
            "week": 3,
            "days": [
                {
                    "day": 1,
                    "exercises": [
                        {"name": "Болгарские выпады", "sets": 3, "reps": 8, "note": "На каждую ногу"},
                        {"name": "Тяга штанги в наклоне", "sets": 4, "reps": 8, "note": "Техника важнее веса"},
                        {"name": "Гиперэкстензия", "sets": 3, "reps": 12, "note": ""}
                    ]
                }
            ]
        },
        {
            "week": 4,
            "days": [
                {
                    "day": 1,
                    "exercises": [
                        {"name": "Суперсет: присед + тяга", "sets": 3, "reps": 10, "note": "Без отдыха между"},
                        {"name": "Бёрпи", "sets": 3, "reps": 8, "note": ""},
                        {"name": "Становая тяга с гантелями", "sets": 3, "reps": 10, "note": "Контролируй поясницу"}
                    ]
                }
            ]
        }
    ]
}

BODY_ANALYSIS_PROMPT_TEMPLATE = (
    "Ты элитный персональный тренер с 15 годами опыта. Проанализируй фото тела клиента по шагам.\n\n"
    "Параметры клиента: цель={goal}, уровень подготовки={level}, "
    "доступное оборудование={equipment}, ограничения={limitations}.\n"
    "Данные Apple Vision (объективные измерения): {posture_data}\n"
    "ОБЯЗАТЕЛЬНО процитируй эти данные ДОСЛОВНО в разделе body_analysis, включая все цифры градусов — "
    "например: 'Apple Vision зафиксировал: небольшой наклон плеч 4°, сутулость 3°'. "
    "Никогда не перефразируй и не опускай числовые значения из posture_data. "
    "Затем дай рекомендацию под конкретные выявленные проблемы.\n"
    "Если posture_data содержит 'осанка в норме' — напиши это явно.\n"
    "Если posture_data пустой или 'не получены' — анализируй осанку визуально по фото как обычно.\n\n"
    "ШАГ 1 — АНАЛИЗ ТЕЛОСЛОЖЕНИЯ:\n"
    "- Определи пол клиента по фото (мужчина/женщина) — это критично для программы\n"
    "- Определи соматотип (эктоморф/мезоморф/эндоморф или смешанный)\n"
    "- Оцени % жира визуально (диапазон, например 18-22%)\n"
    "- Определи развитые и отстающие группы мышц\n"
    "- Отметь осанку: наклон таза, сутулость, асимметрии\n"
    "- Укажи 2-3 приоритетные зоны исходя из пола, цели и фото\n\n"
    "ШАГ 2 — ЛОГИКА ПРОГРАММЫ (с учётом пола):\n"
    "- Программа ВСЕГДА включает все группы мышц — никакие не пропускать\n"
    "- Для мужчины: приоритет грудь/спина/руки/плечи, ноги обязательно 1-2 раза в неделю для баланса\n"
    "- Для женщины: приоритет ягодицы/ноги/кор, верх тела обязательно для тонуса (не объём)\n"
    "- Какой split подходит (full body / upper-lower / PPL)\n"
    "- Какой диапазон повторений нужен (сила / гипертрофия / выносливость)\n"
    "- Как учесть ограничения\n\n"
    "ШАГ 3 — ПРОГРАММА НА 4 НЕДЕЛИ:\n"
    "Составь прогрессирующую программу под конкретный пол и цель: "
    "каждая неделя сложнее предыдущей. Упражнения — только под доступное оборудование.\n\n"
    "Верни ТОЛЬКО JSON без пояснений вне JSON:\n"
    "{{\"body_analysis\": \"детальный анализ из шага 1 включая определённый пол (3-5 предложений)\", "
    "\"program\": \"логика из шага 2 с учётом пола (2-3 предложения)\", "
    "\"weeks\": [{{\"week\": int, \"days\": [{{\"day\": int, "
    "\"exercises\": [{{\"name\": str, \"sets\": int, \"reps\": int, \"note\": str}}]}}]}}]}}\n\n"
    "Отвечай КРАТКО. Максимум 1500 токенов. Только JSON без лишних слов."
)


@app.post("/api/v1/body-analysis/analyze")
async def body_analysis_analyze(
    photo_front: UploadFile = File(None),
    photo_side: UploadFile = File(None),
    goal: str = Form(...),
    level: str = Form(...),
    equipment: str = Form(...),
    limitations: str = Form(""),
    client_id: int = Form(0),
    client_name: str = Form(""),
    posture_data: str = Form(""),
    authorization: str = Header(None),
):
    user = _get_cal_ai_user(authorization)

    print(f"[body_analysis] posture_data='{posture_data}', client_name='{client_name}', client_id={client_id}")

    photo_base64_front = None
    photo_base64_side = None
    if photo_front:
        photo_base64_front = base64.b64encode(await photo_front.read()).decode()
    if photo_side:
        photo_base64_side = base64.b64encode(await photo_side.read()).decode()

    prompt = BODY_ANALYSIS_PROMPT_TEMPLATE.format(
        goal=goal, level=level, equipment=equipment, limitations=limitations,
        posture_data=posture_data if posture_data else "не получены",
    )

    result = None
    if ANTHROPIC_API_KEY:
        result = await call_claude_vision_opus(photo_base64_front, photo_base64_side, prompt)
        print("Body Analysis result:", result is not None, str(result)[:200] if result else "None")

    if result is None:
        result = dict(MOCK_BODY_ANALYSIS_RESPONSE)
        if ANTHROPIC_API_KEY:
            result["note"] = "Ошибка анализа, использована заглушка"

    if client_name:
        result["client_name"] = client_name

    await save_body_analysis(
        trainer_id=user["trainer_id"],
        client_id=client_id if client_id > 0 else None,
        result=result,
    )

    return result


@app.get("/api/v1/body-analysis/history")
async def body_analysis_history(
    client_id: int = Query(0),
    authorization: str = Header(None),
):
    try:
        trainer_id = await get_current_trainer_id(authorization)
        items = await get_body_analysis_history(
            trainer_id=trainer_id,
            client_id=client_id if client_id > 0 else None,
        )
        print(f"[body_analysis_history] trainer_id={trainer_id} items_count={len(items)}")
        return {"items": items}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[body_analysis_history] error: {e}")
        raise HTTPException(500, "Внутренняя ошибка сервера")


async def call_claude_vision_opus(
    photo_base64_front: str | None,
    photo_base64_side: str | None,
    prompt: str,
) -> dict | None:
    print("Body Analysis called, front photo:", bool(photo_base64_front), "side:", bool(photo_base64_side))
    if not ANTHROPIC_API_KEY:
        return None
    try:
        content = []
        if photo_base64_front:
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": photo_base64_front,
                },
            })
        if photo_base64_side:
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": photo_base64_side,
                },
            })
        content.append({"type": "text", "text": prompt})

        body = {
        "model": "claude-sonnet-4-6",
            "max_tokens": 2048,
            "messages": [{"role": "user", "content": content}],
        }
        async with httpx.AsyncClient(timeout=25) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json=body,
            )
        if resp.status_code != 200:
            print("Body Analysis Claude error:", resp.status_code, (resp.text[:500] if resp.text else ""))
            return None
        data = resp.json()
        for block in data.get("content", []):
            if block.get("type") == "text":
                text = block["text"].strip()
                m = re.search(r"\{.*\}", text, re.DOTALL)
                if m:
                    try:
                        return json.loads(m.group())
                    except json.JSONDecodeError:
                        return None
        return None
    except Exception as e:
        print("Body Analysis exception:", str(e))
        return None


# ─── Health ───────────────────────────────────────────────

@app.get("/")
async def root():
    return {"ok": True, "app": "Solvo Fit API", "version": "1.1.0", "build": "2026-05-16-history"}

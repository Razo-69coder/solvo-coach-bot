import os
import json
import io
import csv
from dotenv import load_dotenv
from typing import Optional
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql://", "postgresql://")
_pool: AsyncConnectionPool | None = None


async def get_pool() -> AsyncConnectionPool:
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=10, open=False)
        await _pool.open()
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.connection() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trainers (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                phone TEXT DEFAULT '',
                work_start INTEGER DEFAULT 9,
                work_end INTEGER DEFAULT 21,
                slot_duration INTEGER DEFAULT 60,
                timezone TEXT DEFAULT 'Europe/Moscow',
                theme TEXT DEFAULT 'steel',
                telegram_id BIGINT,
                booking_link TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                trainer_id INTEGER NOT NULL REFERENCES trainers(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                phone TEXT DEFAULT '',
                gender TEXT DEFAULT 'male',
                telegram_username TEXT DEFAULT '',
                telegram_id BIGINT,
                notes TEXT DEFAULT '',
                birthday TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY,
                trainer_id INTEGER NOT NULL REFERENCES trainers(id) ON DELETE CASCADE,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                session_date TEXT NOT NULL,
                time TEXT NOT NULL,
                duration_min INTEGER DEFAULT 60,
                notes TEXT DEFAULT '',
                status TEXT DEFAULT 'scheduled',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id SERIAL PRIMARY KEY,
                trainer_id INTEGER NOT NULL REFERENCES trainers(id) ON DELETE CASCADE,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                total_sessions INTEGER NOT NULL,
                used_sessions INTEGER DEFAULT 0,
                price_total INTEGER NOT NULL,
                purchase_date TEXT NOT NULL,
                expiry_date TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS client_body (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                trainer_id INTEGER NOT NULL,
                height_cm FLOAT,
                weight_kg FLOAT,
                age INTEGER,
                goal TEXT DEFAULT '',
                health_notes TEXT DEFAULT '',
                measured_at TEXT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS workout_programs (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                trainer_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS client_auth (
                id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
                trainer_id INTEGER REFERENCES trainers(id) ON DELETE CASCADE,
                pin_code VARCHAR(6) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS client_pr (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                trainer_id INTEGER NOT NULL,
                exercise_name TEXT NOT NULL,
                weight_kg REAL NOT NULL,
                reps INTEGER NOT NULL,
                recorded_at DATE DEFAULT CURRENT_DATE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS client_cycle (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                trainer_id INTEGER NOT NULL,
                cycle_start_date DATE NOT NULL,
                cycle_length_days INTEGER DEFAULT 28,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS session_exercises (
                id SERIAL PRIMARY KEY,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                exercise_name TEXT NOT NULL,
                muscle_group TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                trainer_id INTEGER NOT NULL REFERENCES trainers(id) ON DELETE CASCADE,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                amount INTEGER NOT NULL,
                description TEXT DEFAULT '',
                payment_date TEXT NOT NULL,
                is_paid BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS program_templates (
                id SERIAL PRIMARY KEY,
                trainer_id INTEGER REFERENCES trainers(id) ON DELETE CASCADE,
                name VARCHAR(255) NOT NULL,
                description TEXT DEFAULT '',
                duration_weeks INTEGER DEFAULT 4,
                level VARCHAR(50) DEFAULT 'beginner',
                goal VARCHAR(100) DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS template_days (
                id SERIAL PRIMARY KEY,
                template_id INTEGER REFERENCES program_templates(id) ON DELETE CASCADE,
                day_number INTEGER NOT NULL,
                name VARCHAR(255) DEFAULT '',
                exercises JSONB DEFAULT '[]'::jsonb
            )
        """)
        await conn.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                    WHERE table_name='trainers' AND column_name='tier') THEN
                    ALTER TABLE trainers ADD COLUMN tier INTEGER DEFAULT 1;
                END IF;
            END $$;
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS cal_ai_logs (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                user_role VARCHAR(20) NOT NULL,
                trainer_id INTEGER REFERENCES trainers(id) ON DELETE CASCADE,
                request_data JSONB,
                result JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trainer_meta (
                id SERIAL PRIMARY KEY,
                clients_count TEXT DEFAULT '',
                work_type TEXT DEFAULT '',
                experience TEXT DEFAULT '',
                current_tool TEXT DEFAULT '',
                referral_source TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)


async def _fetchrow(conn, sql, *args):
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, args)
        return await cur.fetchone()


async def _fetch(conn, sql, *args):
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, args)
        return await cur.fetchall()


async def _fetchval(conn, sql, *args):
    async with conn.cursor() as cur:
        await cur.execute(sql, args)
        row = await cur.fetchone()
        return row[0] if row else None


async def _execute(conn, sql, *args):
    async with conn.cursor() as cur:
        await cur.execute(sql, args)
        return cur.statusmessage or ""


# ─── Auth ────────────────────────────────────────────────

async def get_trainer_by_email(email: str) -> dict | None:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetchrow(conn,
            "SELECT id, email, password_hash, name, phone, work_start, work_end, "
            "slot_duration, timezone, theme, telegram_id, booking_link, is_active "
            "FROM trainers WHERE email = %s", email)


async def create_trainer(email: str, password_hash: str, name: str, phone: str) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        row = await _fetchrow(conn,
            "INSERT INTO trainers (email, password_hash, name, phone) VALUES (%s, %s, %s, %s) RETURNING id",
            email, password_hash, name, phone)
    return row["id"]


async def get_trainer_by_id(trainer_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetchrow(conn,
            "SELECT id, email, name, phone, work_start, work_end, slot_duration, "
            "timezone, theme, telegram_id, booking_link, is_active "
            "FROM trainers WHERE id = %s", trainer_id)


async def update_trainer_settings(trainer_id: int, name: str, work_start: int, work_end: int, slot_duration: int, timezone: str) -> None:
    pool = await get_pool()
    async with pool.connection() as conn:
        await _execute(conn,
            "UPDATE trainers SET name=%s, work_start=%s, work_end=%s, slot_duration=%s, timezone=%s WHERE id=%s",
            name, work_start, work_end, slot_duration, timezone, trainer_id)


async def set_trainer_active(trainer_id: int, is_active: bool) -> None:
    pool = await get_pool()
    async with pool.connection() as conn:
        await _execute(conn, "UPDATE trainers SET is_active=%s WHERE id=%s", is_active, trainer_id)


async def get_all_trainers() -> list[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetch(conn, """
            SELECT t.id, t.name, t.email, t.phone, t.created_at,
                   t.theme, t.is_active, t.booking_link,
                   COUNT(DISTINCT c.id) as clients_count,
                   COUNT(DISTINCT s.id) as sessions_count
            FROM trainers t
            LEFT JOIN clients c ON c.trainer_id = t.id
            LEFT JOIN sessions s ON s.trainer_id = t.id
            GROUP BY t.id
            ORDER BY t.created_at DESC
        """)


# ─── Clients ──────────────────────────────────────────────

async def get_clients_page(trainer_id: int, page: int, page_size: int = 20) -> tuple[list, int]:
    pool = await get_pool()
    async with pool.connection() as conn:
        total = await _fetchval(conn, "SELECT COUNT(*) FROM clients WHERE trainer_id=%s", trainer_id)
        rows = await _fetch(conn, """
            SELECT c.id, c.name, c.phone, c.gender, c.notes, c.birthday,
                   c.telegram_username, c.telegram_id,
                   (SELECT MAX(s.session_date) FROM sessions s WHERE s.client_id = c.id) as last_session
            FROM clients c
            WHERE c.trainer_id = %s
            ORDER BY c.name
            LIMIT %s OFFSET %s
        """, trainer_id, page_size, page * page_size)
    return rows, total


async def search_clients(trainer_id: int, query: str) -> list:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetch(conn, """
            SELECT c.id, c.name, c.phone, c.gender, c.notes, c.birthday
            FROM clients c
            WHERE c.trainer_id = %s AND LOWER(c.name) LIKE %s
            ORDER BY c.name
        """, trainer_id, f"%{query.lower()}%")


async def get_client(client_id: int, trainer_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetchrow(conn,
            "SELECT id, name, phone, gender, notes, telegram_username, telegram_id, birthday "
            "FROM clients WHERE id=%s AND trainer_id=%s",
            client_id, trainer_id)


async def create_client(trainer_id: int, name: str, phone: str, gender: str, telegram_username: str, notes: str) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        row = await _fetchrow(conn,
            "INSERT INTO clients (trainer_id, name, phone, gender, telegram_username, notes) "
            "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
            trainer_id, name, phone, gender, telegram_username, notes)
    return row["id"]


async def update_client(client_id: int, trainer_id: int, name: str, phone: str, notes: str) -> bool:
    pool = await get_pool()
    async with pool.connection() as conn:
        result = await _execute(conn,
            "UPDATE clients SET name=%s, phone=%s, notes=%s WHERE id=%s AND trainer_id=%s",
            name, phone, notes, client_id, trainer_id)
    return "UPDATE 1" in result


async def delete_client(client_id: int, trainer_id: int) -> bool:
    pool = await get_pool()
    async with pool.connection() as conn:
        await _execute(conn, "DELETE FROM sessions WHERE client_id=%s AND trainer_id=%s", client_id, trainer_id)
        await _execute(conn, "DELETE FROM subscriptions WHERE client_id=%s AND trainer_id=%s", client_id, trainer_id)
        await _execute(conn, "DELETE FROM payments WHERE client_id=%s AND trainer_id=%s", client_id, trainer_id)
        result = await _execute(conn, "DELETE FROM clients WHERE id=%s AND trainer_id=%s", client_id, trainer_id)
    return "DELETE 1" in result


async def get_client_session_history(client_id: int) -> list:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetch(conn,
            "SELECT session_date, time, duration_min, status, notes FROM sessions "
            "WHERE client_id=%s ORDER BY session_date DESC, time DESC",
            client_id)


# ─── Sessions ─────────────────────────────────────────────

async def get_schedule(trainer_id: int, date_str: str) -> list:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetch(conn, """
            SELECT s.id, s.client_id, s.trainer_id, s.session_date, s.time,
                   s.duration_min, s.notes, s.status,
                   c.name as client_name, c.phone as client_phone
            FROM sessions s
            JOIN clients c ON c.id = s.client_id
            WHERE s.trainer_id=%s AND s.session_date=%s
            ORDER BY s.time, s.id
        """, trainer_id, date_str)


async def get_available_slots(trainer_id: int, date_str: str, work_start: int, work_end: int, slot_duration: int) -> list[str]:
    pool = await get_pool()
    async with pool.connection() as conn:
        busy_rows = await _fetch(conn,
            "SELECT time FROM sessions WHERE trainer_id=%s AND session_date=%s AND status NOT IN ('cancelled', 'no_show')",
            trainer_id, date_str)
    busy = set(r["time"] for r in busy_rows)
    slots = []
    total_min = work_start * 60
    end_min = work_end * 60
    while total_min + slot_duration <= end_min:
        h, m = divmod(total_min, 60)
        t = f"{h:02d}:{m:02d}"
        if t not in busy:
            slots.append(t)
        total_min += slot_duration
    return slots


async def create_session(trainer_id: int, client_id: int, session_date: str, time: str, duration_min: int, notes: str) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        row = await _fetchrow(conn,
            "INSERT INTO sessions (trainer_id, client_id, session_date, time, duration_min, notes) "
            "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
            trainer_id, client_id, session_date, time, duration_min, notes)
    return row["id"]


async def update_session_status(session_id: int, trainer_id: int, status: str) -> bool:
    pool = await get_pool()
    async with pool.connection() as conn:
        result = await _execute(conn,
            "UPDATE sessions SET status=%s WHERE id=%s AND trainer_id=%s",
            status, session_id, trainer_id)
    return "UPDATE 1" in result


async def complete_session(session_id: int, trainer_id: int) -> bool:
    pool = await get_pool()
    async with pool.connection() as conn:
        result = await _execute(conn,
            "UPDATE sessions SET status='completed' WHERE id=%s AND trainer_id=%s",
            session_id, trainer_id)
        if "UPDATE 1" not in result:
            return False
        row = await _fetchrow(conn, "SELECT client_id FROM sessions WHERE id=%s", session_id)
        if row:
            sub = await _fetchrow(conn,
                "SELECT id, used_sessions, total_sessions FROM subscriptions "
                "WHERE client_id=%s AND trainer_id=%s AND is_active=TRUE "
                "ORDER BY created_at DESC LIMIT 1",
                row["client_id"], trainer_id)
            if sub:
                new_used = sub["used_sessions"] + 1
                if new_used >= sub["total_sessions"]:
                    await _execute(conn,
                        "UPDATE subscriptions SET used_sessions=%s, is_active=FALSE WHERE id=%s",
                        new_used, sub["id"])
                else:
                    await _execute(conn,
                        "UPDATE subscriptions SET used_sessions=%s WHERE id=%s",
                        new_used, sub["id"])
    return True


# ─── Subscriptions ────────────────────────────────────────

async def get_subscriptions(trainer_id: int, client_id: Optional[int] = None) -> list:
    pool = await get_pool()
    async with pool.connection() as conn:
        if client_id:
            return await _fetch(conn,
                "SELECT s.id, s.client_id, c.name as client_name, s.total_sessions, s.used_sessions, "
                "s.price_total, s.purchase_date, s.expiry_date, s.is_active "
                "FROM subscriptions s JOIN clients c ON c.id = s.client_id "
                "WHERE s.trainer_id=%s AND s.client_id=%s ORDER BY s.created_at DESC",
                trainer_id, client_id)
        return await _fetch(conn,
            "SELECT s.id, s.client_id, c.name as client_name, s.total_sessions, s.used_sessions, "
            "s.price_total, s.purchase_date, s.expiry_date, s.is_active "
            "FROM subscriptions s JOIN clients c ON c.id = s.client_id "
            "WHERE s.trainer_id=%s ORDER BY s.created_at DESC",
            trainer_id)


async def create_subscription(trainer_id: int, client_id: int, total_sessions: int, price_total: int, purchase_date: str, expiry_date: Optional[str]) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        row = await _fetchrow(conn,
            "INSERT INTO subscriptions (trainer_id, client_id, total_sessions, price_total, purchase_date, expiry_date) "
            "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
            trainer_id, client_id, total_sessions, price_total, purchase_date, expiry_date)
    return row["id"]


async def deactivate_subscription(sub_id: int, trainer_id: int) -> bool:
    pool = await get_pool()
    async with pool.connection() as conn:
        result = await _execute(conn,
            "UPDATE subscriptions SET is_active=FALSE WHERE id=%s AND trainer_id=%s",
            sub_id, trainer_id)
    return "UPDATE 1" in result


async def get_active_subscription(client_id: int, trainer_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetchrow(conn,
            "SELECT id, total_sessions, used_sessions, price_total, purchase_date, expiry_date "
            "FROM subscriptions WHERE client_id=%s AND trainer_id=%s AND is_active=TRUE "
            "ORDER BY created_at DESC LIMIT 1",
            client_id, trainer_id)


# ─── Payments ─────────────────────────────────────────────

async def get_payments(trainer_id: int, client_id: Optional[int] = None) -> tuple[list, int]:
    pool = await get_pool()
    async with pool.connection() as conn:
        if client_id:
            rows = await _fetch(conn,
                "SELECT p.id, p.client_id, c.name as client_name, p.amount, p.payment_date, p.description, p.is_paid "
                "FROM payments p JOIN clients c ON c.id = p.client_id "
                "WHERE p.trainer_id=%s AND p.client_id=%s ORDER BY p.payment_date DESC",
                trainer_id, client_id)
            total_debt = await _fetchval(conn,
                "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=%s AND client_id=%s AND is_paid=FALSE",
                trainer_id, client_id)
        else:
            rows = await _fetch(conn,
                "SELECT p.id, p.client_id, c.name as client_name, p.amount, p.payment_date, p.description, p.is_paid "
                "FROM payments p JOIN clients c ON c.id = p.client_id "
                "WHERE p.trainer_id=%s ORDER BY p.payment_date DESC",
                trainer_id)
            total_debt = await _fetchval(conn,
                "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=%s AND is_paid=FALSE",
                trainer_id)
    return rows, total_debt or 0


async def create_payment(trainer_id: int, client_id: int, amount: int, description: str, payment_date: str, is_paid: bool) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        row = await _fetchrow(conn,
            "INSERT INTO payments (trainer_id, client_id, amount, description, payment_date, is_paid) "
            "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
            trainer_id, client_id, amount, description, payment_date, is_paid)
    return row["id"]


async def mark_payment_paid(payment_id: int, trainer_id: int) -> bool:
    pool = await get_pool()
    async with pool.connection() as conn:
        result = await _execute(conn,
            "UPDATE payments SET is_paid=TRUE WHERE id=%s AND trainer_id=%s",
            payment_id, trainer_id)
    return "UPDATE 1" in result


# ─── Stats ────────────────────────────────────────────────

async def get_stats(trainer_id: int) -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        total_clients    = await _fetchval(conn, "SELECT COUNT(*) FROM clients WHERE trainer_id=%s", trainer_id)
        total_sessions   = await _fetchval(conn, "SELECT COUNT(*) FROM sessions WHERE trainer_id=%s AND status='completed'", trainer_id)
        month_sessions   = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE trainer_id=%s AND status='completed' "
            "AND to_date(session_date, 'YYYY-MM-DD') >= date_trunc('month', CURRENT_DATE)", trainer_id)
        total_earnings   = await _fetchval(conn, "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=%s AND is_paid=TRUE", trainer_id)
        month_earnings   = await _fetchval(conn,
            "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=%s AND is_paid=TRUE "
            "AND to_date(payment_date, 'YYYY-MM-DD') >= date_trunc('month', CURRENT_DATE)", trainer_id)
        total_debt       = await _fetchval(conn, "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=%s AND is_paid=FALSE", trainer_id)
        active_subs      = await _fetchval(conn, "SELECT COUNT(*) FROM subscriptions WHERE trainer_id=%s AND is_active=TRUE", trainer_id)
    return {
        "total_clients": total_clients or 0,
        "total_sessions": total_sessions or 0,
        "month_sessions": month_sessions or 0,
        "total_earnings": total_earnings or 0,
        "month_earnings": month_earnings or 0,
        "total_debt": total_debt or 0,
        "active_subscriptions": active_subs or 0,
    }


# ─── Client Body ───────────────────────────────────────────

async def get_client_body(client_id: int, trainer_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetchrow(conn,
            "SELECT id, client_id, height_cm, weight_kg, age, goal, health_notes, measured_at "
            "FROM client_body WHERE client_id=%s AND trainer_id=%s "
            "ORDER BY id DESC LIMIT 1", client_id, trainer_id)


async def add_client_body(client_id: int, trainer_id: int, height_cm: float, weight_kg: float,
                          age: int, goal: str, health_notes: str, measured_at: str) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        row = await _fetchrow(conn,
            "INSERT INTO client_body (client_id, trainer_id, height_cm, weight_kg, age, goal, health_notes, measured_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
            client_id, trainer_id, height_cm, weight_kg, age, goal, health_notes, measured_at)
    return row["id"]


async def get_client_body_history(client_id: int) -> list:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetch(conn,
            "SELECT id, height_cm, weight_kg, age, goal, health_notes, measured_at "
            "FROM client_body WHERE client_id=%s ORDER BY measured_at DESC", client_id)


# ─── Workout Programs ──────────────────────────────────────

async def get_workout_program(client_id: int, trainer_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetchrow(conn,
            "SELECT id, client_id, title, content, created_at "
            "FROM workout_programs WHERE client_id=%s AND trainer_id=%s "
            "ORDER BY created_at DESC LIMIT 1", client_id, trainer_id)


async def save_workout_program(client_id: int, trainer_id: int, title: str, content: str) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        row = await _fetchrow(conn,
            "INSERT INTO workout_programs (client_id, trainer_id, title, content) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            client_id, trainer_id, title, content)
    return row["id"]


# ─── Client Analytics ─────────────────────────────────────

async def get_client_analytics(client_id: int, trainer_id: int) -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        total = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND trainer_id=%s",
            client_id, trainer_id) or 0
        completed = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND trainer_id=%s AND status='completed'",
            client_id, trainer_id) or 0
        cancelled = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND trainer_id=%s AND status='cancelled'",
            client_id, trainer_id) or 0
        revenue = await _fetchval(conn,
            "SELECT COALESCE(SUM(amount),0) FROM payments WHERE client_id=%s AND trainer_id=%s AND is_paid=true",
            client_id, trainer_id) or 0
        debt = await _fetchval(conn,
            "SELECT COALESCE(SUM(amount),0) FROM payments WHERE client_id=%s AND trainer_id=%s AND is_paid=false",
            client_id, trainer_id) or 0
        last_session = await _fetchval(conn,
            "SELECT session_date FROM sessions WHERE client_id=%s AND trainer_id=%s ORDER BY session_date DESC LIMIT 1",
            client_id, trainer_id)
    attendance = round((completed / total * 100) if total > 0 else 0, 1)
    days_inactive = 0
    if last_session and total > 0:
        from datetime import date
        try:
            parts = last_session.split("-")
            last_d = date(int(parts[0]), int(parts[1]), int(parts[2]))
            days_inactive = (date.today() - last_d).days
        except Exception:
            days_inactive = 0
    return {
        "total_sessions": total,
        "completed_sessions": completed,
        "cancelled_sessions": cancelled,
        "attendance_percent": attendance,
        "revenue": float(revenue),
        "debt": float(debt),
        "days_inactive": days_inactive,
        "churn_risk": days_inactive >= 14,
    }


# ─── Client Auth ──────────────────────────────────────────

async def generate_client_pin(client_id: int, trainer_id: int) -> str:
    import random
    import string
    pool = await get_pool()
    async with pool.connection() as conn:
        while True:
            pin = ''.join(random.choices(string.digits, k=6))
            existing = await _fetchval(conn, "SELECT id FROM client_auth WHERE pin_code=%s", pin)
            if not existing:
                break
        await _execute(conn,
            "INSERT INTO client_auth (client_id, trainer_id, pin_code) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (client_id) DO UPDATE SET pin_code=%s",
            client_id, trainer_id, pin, pin)
    return pin


async def get_client_by_pin(pin: str) -> dict | None:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetchrow(conn, """
            SELECT ca.client_id, ca.trainer_id, ca.pin_code,
                   c.name, c.phone, c.gender, c.telegram_username
            FROM client_auth ca
            JOIN clients c ON c.id = ca.client_id
            WHERE ca.pin_code = %s
        """, pin)


# ─── PR Records ─────────────────────────────────────────────

async def save_pr(client_id: int, trainer_id: int, exercise_name: str, weight_kg: float, reps: int, recorded_at: str) -> None:
    pool = await get_pool()
    async with pool.connection() as conn:
        await _execute(conn,
            "INSERT INTO client_pr (client_id, trainer_id, exercise_name, weight_kg, reps, recorded_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            client_id, trainer_id, exercise_name, weight_kg, reps, recorded_at)


async def get_pr_list(client_id: int) -> list:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetch(conn, """
            SELECT DISTINCT ON (exercise_name)
                id, exercise_name, weight_kg, reps, recorded_at
            FROM client_pr
            WHERE client_id = %s
            ORDER BY exercise_name, weight_kg DESC
        """, client_id)


async def get_pr_history(client_id: int, exercise_name: str) -> list:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetch(conn, """
            SELECT id, exercise_name, weight_kg, reps, recorded_at
            FROM client_pr
            WHERE client_id = %s AND exercise_name = %s
            ORDER BY recorded_at ASC
        """, client_id, exercise_name)


# ─── Cycle Tracker ──────────────────────────────────────────

async def save_cycle_start(client_id: int, trainer_id: int, cycle_start_date: str, cycle_length_days: int = 28) -> None:
    pool = await get_pool()
    async with pool.connection() as conn:
        await _execute(conn,
            "INSERT INTO client_cycle (client_id, trainer_id, cycle_start_date, cycle_length_days) "
            "VALUES (%s, %s, %s, %s)",
            client_id, trainer_id, cycle_start_date, cycle_length_days)


async def get_cycle_phase(client_id: int) -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        row = await _fetchrow(conn,
            "SELECT cycle_start_date, cycle_length_days "
            "FROM client_cycle WHERE client_id = %s "
            "ORDER BY created_at DESC LIMIT 1", client_id)
    if not row:
        return {"phase": None, "label": None, "day": None, "hint": None}
    from datetime import date
    today = date.today()
    start = row["cycle_start_date"]
    length = row["cycle_length_days"]
    day = (today - start).days % length + 1
    if day <= 5:
        phase = "menstruation"
        label = "Менструация"
        hint = "Снизить интенсивность, больше растяжки"
    elif day <= 13:
        phase = "follicular"
        label = "Фолликулярная"
        hint = "Хорошее время для силовых нагрузок"
    elif day <= 16:
        phase = "ovulation"
        label = "Овуляция"
        hint = "Пик формы — максимальная интенсивность"
    else:
        phase = "luteal"
        label = "Лютеиновая"
        hint = "Умеренная нагрузка, акцент на технику"
    return {"phase": phase, "label": label, "day": day, "hint": hint}


async def get_client_weekly_report(client_id: int, trainer_id: int, week_offset: int = 0) -> dict:
    from datetime import date, timedelta
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    week_start = monday - timedelta(weeks=week_offset)
    week_end = week_start + timedelta(days=6)
    ws = week_start.isoformat()
    we = week_end.isoformat()

    pool = await get_pool()
    async with pool.connection() as conn:
        sessions_done = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND trainer_id=%s "
            "AND status='completed' AND session_date>=%s AND session_date<=%s",
            client_id, trainer_id, ws, we) or 0

        sessions_planned = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND trainer_id=%s "
            "AND status NOT IN ('cancelled') AND session_date>=%s AND session_date<=%s",
            client_id, trainer_id, ws, we) or 0

        sessions_missed = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND trainer_id=%s "
            "AND status IN ('cancelled','no_show') AND session_date>=%s AND session_date<=%s",
            client_id, trainer_id, ws, we) or 0

        ws_row = await _fetchrow(conn,
            "SELECT weight_kg FROM client_body WHERE client_id=%s AND measured_at<=%s "
            "ORDER BY measured_at DESC LIMIT 1", client_id, ws)
        weight_start = float(ws_row["weight_kg"]) if ws_row else None

        we_row = await _fetchrow(conn,
            "SELECT weight_kg FROM client_body WHERE client_id=%s AND measured_at>=%s AND measured_at<=%s "
            "ORDER BY measured_at DESC LIMIT 1", client_id, ws, we)
        weight_end = float(we_row["weight_kg"]) if we_row else (weight_start if weight_start else None)

        new_prs = await _fetch(conn,
            "SELECT id, exercise_name, weight_kg, reps, recorded_at "
            "FROM client_pr WHERE client_id=%s AND recorded_at>=%s AND recorded_at<=%s "
            "ORDER BY recorded_at ASC", client_id, ws, we)

    return {
        "week_start": ws,
        "week_end": we,
        "sessions_done": sessions_done,
        "sessions_planned": sessions_planned,
        "sessions_missed": sessions_missed,
        "weight_start": weight_start,
        "weight_end": weight_end,
        "new_prs": new_prs,
    }


async def get_client_progress_summary(client_id: int, trainer_id: int) -> dict:
    from datetime import date, timedelta
    today = date.today()
    monday = today - timedelta(days=today.weekday())

    pool = await get_pool()
    async with pool.connection() as conn:
        total_sessions = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND trainer_id=%s AND status='completed'",
            client_id, trainer_id) or 0

        week_done = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND trainer_id=%s "
            "AND status='completed' AND session_date>=%s",
            client_id, trainer_id, monday.isoformat()) or 0

        week_planned = await _fetchval(conn,
            "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND trainer_id=%s "
            "AND status NOT IN ('cancelled') AND session_date>=%s",
            client_id, trainer_id, monday.isoformat()) or 0

        pr_row = await _fetchrow(conn,
            "SELECT exercise_name, weight_kg FROM client_pr "
            "WHERE client_id=%s ORDER BY weight_kg DESC LIMIT 1", client_id)

        ws_row = await _fetchrow(conn,
            "SELECT weight_kg FROM client_body WHERE client_id=%s "
            "ORDER BY measured_at ASC LIMIT 1", client_id)

        wc_row = await _fetchrow(conn,
            "SELECT weight_kg FROM client_body WHERE client_id=%s "
            "ORDER BY measured_at DESC LIMIT 1", client_id)

        # Streak: consecutive days with at least one completed session
        streak = 0
        check = today
        while True:
            ds = check.isoformat()
            cnt = await _fetchval(conn,
                "SELECT COUNT(*) FROM sessions WHERE client_id=%s AND status='completed' AND session_date=%s",
                client_id, ds) or 0
            if cnt > 0:
                streak += 1
                check -= timedelta(days=1)
            else:
                break

    return {
        "streak_days": streak,
        "total_sessions": total_sessions,
        "weight_start": float(ws_row["weight_kg"]) if ws_row else None,
        "weight_current": float(wc_row["weight_kg"]) if wc_row else None,
        "best_pr_kg": float(pr_row["weight_kg"]) if pr_row else None,
        "best_pr_exercise": pr_row["exercise_name"] if pr_row else None,
        "week_done": week_done,
        "week_planned": week_planned,
    }


MUSCLE_GROUP_MAP = {
    "Грудь": ["жим лёжа", "жим лежа", "отжимания", "сведение в кроссовере", "кроссовер", "жим гантелей лёжа", "жим гантелей лежа"],
    "Спина": ["тяга верхнего блока", "тяга штанги", "подтягивания", "тяга гантели", "тяга в наклоне", "пуловер"],
    "Ноги":  ["присед", "приседания", "жим ногами", "выпады", "румынская тяга", "разгибание ног", "сгибание ног", "ягодичный мост"],
    "Плечи": ["жим стоя", "жим гантелей сидя", "разводка", "тяга к подбородку", "армейский жим", "махи в стороны"],
    "Руки":  ["подъём на бицепс", "подъем на бицепс", "трицепсовый блок", "молотки", "французский жим", "разгибание на блоке"],
    "Пресс": ["скручивания", "планка", "подъём ног", "подъем ног", "вакуум", "качать пресс"],
}
MUSCLE_GROUP_NAMES = list(MUSCLE_GROUP_MAP.keys())


def classify_exercise(name: str) -> str:
    nl = name.lower().strip()
    for group, keywords in MUSCLE_GROUP_MAP.items():
        for kw in keywords:
            if kw in nl:
                return group
    return "Прочее"


async def get_muscle_map(client_id: int, days: int = 7) -> dict:
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    pool = await get_pool()
    groups = {g: 0 for g in MUSCLE_GROUP_NAMES}
    async with pool.connection() as conn:
        rows = await _fetch(conn, """
            SELECT exercise_name FROM session_exercises
            WHERE client_id=%s AND created_at::date>=%s
        """, client_id, cutoff)
        for r in rows:
            mg = classify_exercise(r["exercise_name"])
            if mg in groups:
                groups[mg] += 1
    result = [{"name": g, "count": groups[g]} for g in MUSCLE_GROUP_NAMES]
    return {"muscle_groups": result}


async def get_leaderboard(trainer_id: int, month: str) -> dict:
    from datetime import date
    year, m = map(int, month.split("-"))
    ms = date(year, m, 1).isoformat()
    me = date(year + 1, 1, 1).isoformat() if m == 12 else date(year, m + 1, 1).isoformat()

    pool = await get_pool()
    async with pool.connection() as conn:
        rows = await _fetch(conn, """
            SELECT c.id, c.name, COUNT(s.id) as cnt
            FROM clients c
            LEFT JOIN sessions s ON s.client_id = c.id AND s.trainer_id = c.trainer_id
                AND s.status = 'completed'
                AND s.session_date >= %s AND s.session_date < %s
            WHERE c.trainer_id = %s
            GROUP BY c.id, c.name
            ORDER BY cnt DESC, c.name
        """, ms, me, trainer_id)
    result = []
    for i, r in enumerate(rows):
        result.append({
            "client_id": r["id"],
            "name": r["name"],
            "sessions_count": r["cnt"],
            "rank": i + 1,
        })
    return {"leaderboard": result, "month": month}


async def get_client_rank(client_id: int, trainer_id: int, month: str) -> dict:
    lb = await get_leaderboard(trainer_id, month)
    entries = lb["leaderboard"]
    total = len(entries)
    my_idx = None
    my_sessions = 0
    for i, e in enumerate(entries):
        if e["client_id"] == client_id:
            my_idx = i
            my_sessions = e["sessions_count"]
            break
    rank = my_idx + 1 if my_idx is not None else total + 1
    sessions_to_next = None
    if my_idx is not None and my_idx > 0:
        sessions_to_next = entries[my_idx - 1]["sessions_count"] - my_sessions
        if sessions_to_next < 0:
            sessions_to_next = 0
    return {
        "rank": rank,
        "total_clients": total,
        "sessions_count": my_sessions,
        "sessions_to_next": sessions_to_next,
    }


# ─── Cal AI ────────────────────────────────────────────────

async def get_trainer_tier(trainer_id: int) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        val = await _fetchval(conn, "SELECT tier FROM trainers WHERE id=%s", trainer_id)
    return val or 1


async def get_cal_ai_daily_count(client_id: int) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        val = await _fetchval(conn,
            "SELECT COUNT(*) FROM cal_ai_logs "
            "WHERE user_id=%s AND user_role='client' AND created_at::date=CURRENT_DATE",
            client_id)
    return val or 0


async def save_cal_ai_log(user_id: int, user_role: str, trainer_id: int,
                          request_data: dict, result: dict) -> None:
    pool = await get_pool()
    async with pool.connection() as conn:
        await _execute(conn,
            "INSERT INTO cal_ai_logs (user_id, user_role, trainer_id, request_data, result) "
            "VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)",
            user_id, user_role, trainer_id,
            json.dumps(request_data, ensure_ascii=False),
            json.dumps(result, ensure_ascii=False))


async def get_client_schedule(client_id: int, date: str) -> list:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetch(conn, """
            SELECT s.*, c.name as client_name
            FROM sessions s
            JOIN clients c ON c.id = s.client_id
            WHERE s.client_id = %s AND s.session_date = %s
            ORDER BY s.time
        """, client_id, date)


# ─── Program Templates ──────────────────────────────────────

async def create_template(trainer_id: int, name: str, description: str,
                           duration_weeks: int, level: str, goal: str,
                           days: list) -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        row = await _fetchrow(conn,
            "INSERT INTO program_templates (trainer_id, name, description, duration_weeks, level, goal) "
            "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
            trainer_id, name, description, duration_weeks, level, goal)
        template_id = row["id"]
        for d in days:
            await _execute(conn,
                "INSERT INTO template_days (template_id, day_number, name, exercises) "
                "VALUES (%s, %s, %s, %s::jsonb)",
                template_id, d["day_number"], d["name"],
                json.dumps(d["exercises"], ensure_ascii=False))
    return template_id


async def get_templates(trainer_id: int) -> list:
    pool = await get_pool()
    async with pool.connection() as conn:
        return await _fetch(conn,
            "SELECT id, trainer_id, name, description, duration_weeks, level, goal, created_at "
            "FROM program_templates WHERE trainer_id=%s ORDER BY created_at DESC",
            trainer_id)


async def get_template_detail(template_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.connection() as conn:
        template = await _fetchrow(conn,
            "SELECT id, trainer_id, name, description, duration_weeks, level, goal, created_at "
            "FROM program_templates WHERE id=%s", template_id)
        if not template:
            return None
        days = await _fetch(conn,
            "SELECT id, day_number, name, exercises "
            "FROM template_days WHERE template_id=%s ORDER BY day_number",
            template_id)
        template["days"] = days
        return template


async def delete_template(template_id: int, trainer_id: int) -> bool:
    pool = await get_pool()
    async with pool.connection() as conn:
        result = await _execute(conn,
            "DELETE FROM program_templates WHERE id=%s AND trainer_id=%s",
            template_id, trainer_id)
    return "DELETE 1" in result


async def apply_template_to_client(template_id: int, client_id: int, trainer_id: int) -> str:
    pool = await get_pool()
    async with pool.connection() as conn:
        template = await _fetchrow(conn,
            "SELECT name, description, duration_weeks, level, goal "
            "FROM program_templates WHERE id=%s AND trainer_id=%s",
            template_id, trainer_id)
        if not template:
            raise ValueError("Шаблон не найден")

        client = await _fetchrow(conn,
            "SELECT id FROM clients WHERE id=%s AND trainer_id=%s",
            client_id, trainer_id)
        if not client:
            raise ValueError("Клиент не найден")

        days = await _fetch(conn,
            "SELECT day_number, name, exercises "
            "FROM template_days WHERE template_id=%s ORDER BY day_number",
            template_id)

        content = f"Программа: {template['name']}\n"
        content += f"Уровень: {template['level']}, Цель: {template['goal']}, "
        content += f"Недель: {template['duration_weeks']}\n\n"
        for d in days:
            content += f"--- День {d['day_number']}: {d['name']} ---\n"
            exercises = d["exercises"]
            if isinstance(exercises, str):
                exercises = json.loads(exercises)
            for ex in exercises:
                wn = f" ({ex.get('weight_note', '')})" if ex.get('weight_note') else ""
                content += f"  {ex['name']}: {ex['sets']}x{ex['reps']}{wn}\n"
            content += "\n"

        title = template["name"]
        await _execute(conn,
            "INSERT INTO workout_programs (client_id, trainer_id, title, content) "
            "VALUES (%s, %s, %s, %s)",
            client_id, trainer_id, title, content)

    return title


# ─── Churn Risk ──────────────────────────────────────────


async def get_churn_risk(trainer_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        rows = await _fetch(conn, """
            SELECT
                c.id AS client_id,
                c.name,
                COUNT(s.id) AS missed_count,
                MAX(s.session_date) AS last_session_date
            FROM clients c
            LEFT JOIN sessions s ON c.id = s.client_id
                AND s.status IN ('no_show', 'cancelled')
                AND s.session_date::date >= CURRENT_DATE - 14
            WHERE c.trainer_id = %s
            GROUP BY c.id, c.name
            HAVING COUNT(s.id) >= 2
            ORDER BY missed_count DESC
        """, trainer_id)
        result = []
        for r in rows:
            risk = "high" if r["missed_count"] >= 3 else "medium"
            result.append({
                "client_id": r["client_id"],
                "name": r["name"],
                "missed_count": r["missed_count"],
                "last_session_date": r["last_session_date"] or "",
                "risk_level": risk,
            })
        return result


# ─── Onboarding Meta ──────────────────────────────────────


async def save_onboarding_meta(clients_count: str, work_type: str, experience: str,
                                current_tool: str, referral_source: str):
    pool = await get_pool()
    async with pool.connection() as conn:
        await _execute(conn,
            "INSERT INTO trainer_meta (clients_count, work_type, experience, current_tool, referral_source) "
            "VALUES (%s, %s, %s, %s, %s)",
            clients_count, work_type, experience, current_tool, referral_source)
        return True


# ─── Import Excel ──────────────────────────────────────────


async def parse_import_file(file_data: bytes, file_name: str) -> tuple[list[dict], list[str]]:
    """Parse CSV or XLSX file and return list of {name, phone} and errors."""
    results = []
    errors = []

    if file_name.endswith(".csv"):
        text = file_data.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        found_cols = _find_cols(reader.fieldnames or [])
        for row in reader:
            parsed, err = _parse_row(row, found_cols)
            if parsed:
                results.append(parsed)
            if err:
                errors.append(err)

    elif file_name.endswith(".xlsx"):
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(file_data), read_only=True)
        ws = wb.active
        if ws is None:
            return results, ["Файл пуст"]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return results, ["Файл пуст"]
        headers = [str(c or "") for c in rows[0]]
        found_cols = _find_cols(headers)
        for row in rows[1:]:
            row_dict = dict(zip(headers, [str(v or "") for v in row]))
            parsed, err = _parse_row(row_dict, found_cols)
            if parsed:
                results.append(parsed)
            if err:
                errors.append(err)

    return results, errors


def _find_cols(headers: list[str]) -> dict:
    name_keys = {"имя", "name", "клиент", "client", "фио", "fio"}
    phone_keys = {"телефон", "phone", "номер", "number", "тел"}
    found = {"name": None, "phone": None}
    for h in headers:
        hl = h.lower().strip()
        if hl in name_keys and found["name"] is None:
            found["name"] = h
        if hl in phone_keys and found["phone"] is None:
            found["phone"] = h
    return found


def _parse_row(row: dict, cols: dict) -> tuple[dict | None, str | None]:
    name = row.get(cols["name"]) if cols["name"] else None
    phone = row.get(cols["phone"]) if cols["phone"] else None
    if not name and not phone:
        return None, f"Строка без имени и телефона: {row}"
    if not name:
        return None, f"Строка без имени, телефон: {phone}"
    return {"name": name.strip(), "phone": (phone or "").strip()}, None

import os
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

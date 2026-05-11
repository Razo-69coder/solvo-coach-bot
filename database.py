import asyncpg
import os
from contextlib import asynccontextmanager
from datetime import date, timedelta
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
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


# ─── Auth ────────────────────────────────────────────────

async def get_trainer_by_email(email: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email, password_hash, name, phone, work_start, work_end, "
            "slot_duration, timezone, theme, telegram_id, booking_link, is_active "
            "FROM trainers WHERE email = $1", email
        )
    if not row:
        return None
    return dict(row)


async def create_trainer(email: str, password_hash: str, name: str, phone: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO trainers (email, password_hash, name, phone) VALUES ($1, $2, $3, $4) RETURNING id",
            email, password_hash, name, phone
        )
    return row["id"]


async def get_trainer_by_id(trainer_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email, name, phone, work_start, work_end, slot_duration, "
            "timezone, theme, telegram_id, booking_link, is_active "
            "FROM trainers WHERE id = $1", trainer_id
        )
    if not row:
        return None
    return dict(row)


async def update_trainer_settings(trainer_id: int, name: str, work_start: int, work_end: int, slot_duration: int, timezone: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE trainers SET name=$1, work_start=$2, work_end=$3, slot_duration=$4, timezone=$5 WHERE id=$6",
            name, work_start, work_end, slot_duration, timezone, trainer_id
        )


async def set_trainer_active(trainer_id: int, is_active: bool) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE trainers SET is_active=$1 WHERE id=$2", is_active, trainer_id)


async def get_all_trainers() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
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
    return [dict(r) for r in rows]


# ─── Clients ──────────────────────────────────────────────

async def get_clients_page(trainer_id: int, page: int, page_size: int = 20) -> tuple[list, int]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM clients WHERE trainer_id=$1", trainer_id)
        rows = await conn.fetch("""
            SELECT c.id, c.name, c.phone, c.gender, c.notes, c.birthday,
                   c.telegram_username, c.telegram_id,
                   (SELECT MAX(s.session_date) FROM sessions s WHERE s.client_id = c.id) as last_session
            FROM clients c
            WHERE c.trainer_id = $1
            ORDER BY c.name
            LIMIT $2 OFFSET $3
        """, trainer_id, page_size, page * page_size)
    clients = [dict(r) for r in rows]
    return clients, total


async def search_clients(trainer_id: int, query: str) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.id, c.name, c.phone, c.gender, c.notes, c.birthday
            FROM clients c
            WHERE c.trainer_id = $1 AND LOWER(c.name) LIKE $2
            ORDER BY c.name
        """, trainer_id, f"%{query.lower()}%")
    return [dict(r) for r in rows]


async def get_client(client_id: int, trainer_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, phone, gender, notes, telegram_username, telegram_id, birthday "
            "FROM clients WHERE id=$1 AND trainer_id=$2",
            client_id, trainer_id
        )
    if not row:
        return None
    return dict(row)


async def create_client(trainer_id: int, name: str, phone: str, gender: str, telegram_username: str, notes: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO clients (trainer_id, name, phone, gender, telegram_username, notes) "
            "VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
            trainer_id, name, phone, gender, telegram_username, notes
        )
    return row["id"]


async def update_client(client_id: int, trainer_id: int, name: str, phone: str, notes: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE clients SET name=$1, phone=$2, notes=$3 WHERE id=$4 AND trainer_id=$5",
            name, phone, notes, client_id, trainer_id
        )
    return "UPDATE 1" in result


async def delete_client(client_id: int, trainer_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM sessions WHERE client_id=$1 AND trainer_id=$2", client_id, trainer_id)
        await conn.execute("DELETE FROM subscriptions WHERE client_id=$1 AND trainer_id=$2", client_id, trainer_id)
        await conn.execute("DELETE FROM payments WHERE client_id=$1 AND trainer_id=$2", client_id, trainer_id)
        result = await conn.execute("DELETE FROM clients WHERE id=$1 AND trainer_id=$2", client_id, trainer_id)
    return "DELETE 1" in result


async def get_client_session_history(client_id: int) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT session_date, time, duration_min, status, notes FROM sessions "
            "WHERE client_id=$1 ORDER BY session_date DESC, time DESC",
            client_id
        )
    return [dict(r) for r in rows]


# ─── Sessions ─────────────────────────────────────────────

async def get_schedule(trainer_id: int, date_str: str) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.id, s.client_id, s.trainer_id, s.session_date, s.time,
                   s.duration_min, s.notes, s.status,
                   c.name as client_name, c.phone as client_phone
            FROM sessions s
            JOIN clients c ON c.id = s.client_id
            WHERE s.trainer_id=$1 AND s.session_date=$2
            ORDER BY s.time, s.id
        """, trainer_id, date_str)
    return [dict(r) for r in rows]


async def get_available_slots(trainer_id: int, date_str: str, work_start: int, work_end: int, slot_duration: int) -> list[str]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        busy_rows = await conn.fetch(
            "SELECT time FROM sessions WHERE trainer_id=$1 AND session_date=$2 AND status NOT IN ('cancelled', 'no_show')",
            trainer_id, date_str
        )
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
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO sessions (trainer_id, client_id, session_date, time, duration_min, notes) "
            "VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
            trainer_id, client_id, session_date, time, duration_min, notes
        )
    return row["id"]


async def update_session_status(session_id: int, trainer_id: int, status: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE sessions SET status=$1 WHERE id=$2 AND trainer_id=$3",
            status, session_id, trainer_id
        )
    return "UPDATE 1" in result


async def complete_session(session_id: int, trainer_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE sessions SET status='completed' WHERE id=$1 AND trainer_id=$2",
            session_id, trainer_id
        )
        if "UPDATE 1" not in result:
            return False
        row = await conn.fetchrow(
            "SELECT client_id FROM sessions WHERE id=$1", session_id
        )
        if row:
            client_id = row["client_id"]
            sub = await conn.fetchrow(
                "SELECT id, used_sessions, total_sessions FROM subscriptions "
                "WHERE client_id=$1 AND trainer_id=$2 AND is_active=TRUE "
                "ORDER BY created_at DESC LIMIT 1",
                client_id, trainer_id
            )
            if sub:
                new_used = sub["used_sessions"] + 1
                if new_used >= sub["total_sessions"]:
                    await conn.execute(
                        "UPDATE subscriptions SET used_sessions=$1, is_active=FALSE WHERE id=$2",
                        new_used, sub["id"]
                    )
                else:
                    await conn.execute(
                        "UPDATE subscriptions SET used_sessions=$1 WHERE id=$2",
                        new_used, sub["id"]
                    )
    return True


# ─── Subscriptions ────────────────────────────────────────

async def get_subscriptions(trainer_id: int, client_id: Optional[int] = None) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if client_id:
            rows = await conn.fetch(
                "SELECT s.id, s.client_id, c.name as client_name, s.total_sessions, s.used_sessions, "
                "s.price_total, s.purchase_date, s.expiry_date, s.is_active "
                "FROM subscriptions s JOIN clients c ON c.id = s.client_id "
                "WHERE s.trainer_id=$1 AND s.client_id=$2 ORDER BY s.created_at DESC",
                trainer_id, client_id
            )
        else:
            rows = await conn.fetch(
                "SELECT s.id, s.client_id, c.name as client_name, s.total_sessions, s.used_sessions, "
                "s.price_total, s.purchase_date, s.expiry_date, s.is_active "
                "FROM subscriptions s JOIN clients c ON c.id = s.client_id "
                "WHERE s.trainer_id=$1 ORDER BY s.created_at DESC",
                trainer_id
            )
    return [dict(r) for r in rows]


async def create_subscription(trainer_id: int, client_id: int, total_sessions: int, price_total: int, purchase_date: str, expiry_date: Optional[str]) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO subscriptions (trainer_id, client_id, total_sessions, price_total, purchase_date, expiry_date) "
            "VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
            trainer_id, client_id, total_sessions, price_total, purchase_date, expiry_date
        )
    return row["id"]


async def deactivate_subscription(sub_id: int, trainer_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE subscriptions SET is_active=FALSE WHERE id=$1 AND trainer_id=$2",
            sub_id, trainer_id
        )
    return "UPDATE 1" in result


async def get_active_subscription(client_id: int, trainer_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, total_sessions, used_sessions, price_total, purchase_date, expiry_date "
            "FROM subscriptions WHERE client_id=$1 AND trainer_id=$2 AND is_active=TRUE "
            "ORDER BY created_at DESC LIMIT 1",
            client_id, trainer_id
        )
    if not row:
        return None
    return dict(row)


# ─── Payments ─────────────────────────────────────────────

async def get_payments(trainer_id: int, client_id: Optional[int] = None) -> tuple[list, int]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if client_id:
            rows = await conn.fetch(
                "SELECT p.id, p.client_id, c.name as client_name, p.amount, p.payment_date, p.description, p.is_paid "
                "FROM payments p JOIN clients c ON c.id = p.client_id "
                "WHERE p.trainer_id=$1 AND p.client_id=$2 ORDER BY p.payment_date DESC",
                trainer_id, client_id
            )
            total_debt = await conn.fetchval(
                "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=$1 AND client_id=$2 AND is_paid=FALSE",
                trainer_id, client_id
            )
        else:
            rows = await conn.fetch(
                "SELECT p.id, p.client_id, c.name as client_name, p.amount, p.payment_date, p.description, p.is_paid "
                "FROM payments p JOIN clients c ON c.id = p.client_id "
                "WHERE p.trainer_id=$1 ORDER BY p.payment_date DESC",
                trainer_id
            )
            total_debt = await conn.fetchval(
                "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=$1 AND is_paid=FALSE",
                trainer_id
            )
    return [dict(r) for r in rows], total_debt


async def create_payment(trainer_id: int, client_id: int, amount: int, description: str, payment_date: str, is_paid: bool) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO payments (trainer_id, client_id, amount, description, payment_date, is_paid) "
            "VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
            trainer_id, client_id, amount, description, payment_date, is_paid
        )
    return row["id"]


async def mark_payment_paid(payment_id: int, trainer_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE payments SET is_paid=TRUE WHERE id=$1 AND trainer_id=$2",
            payment_id, trainer_id
        )
    return "UPDATE 1" in result


# ─── Stats ────────────────────────────────────────────────

async def get_stats(trainer_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        total_clients = await conn.fetchval(
            "SELECT COUNT(*) FROM clients WHERE trainer_id=$1", trainer_id
        )
        total_sessions = await conn.fetchval(
            "SELECT COUNT(*) FROM sessions WHERE trainer_id=$1 AND status='completed'", trainer_id
        )
        month_sessions = await conn.fetchval(
            "SELECT COUNT(*) FROM sessions WHERE trainer_id=$1 AND status='completed' "
            "AND to_date(session_date, 'YYYY-MM-DD') >= date_trunc('month', CURRENT_DATE)",
            trainer_id
        )
        total_earnings = await conn.fetchval(
            "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=$1 AND is_paid=TRUE",
            trainer_id
        )
        month_earnings = await conn.fetchval(
            "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=$1 AND is_paid=TRUE "
            "AND to_date(payment_date, 'YYYY-MM-DD') >= date_trunc('month', CURRENT_DATE)",
            trainer_id
        )
        total_debt = await conn.fetchval(
            "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE trainer_id=$1 AND is_paid=FALSE",
            trainer_id
        )
        active_subscriptions = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE trainer_id=$1 AND is_active=TRUE",
            trainer_id
        )
    return {
        "total_clients": total_clients or 0,
        "total_sessions": total_sessions or 0,
        "month_sessions": month_sessions or 0,
        "total_earnings": total_earnings or 0,
        "month_earnings": month_earnings or 0,
        "total_debt": total_debt or 0,
        "active_subscriptions": active_subscriptions or 0,
    }

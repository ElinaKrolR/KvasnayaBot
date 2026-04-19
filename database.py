import os
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Определяем тип базы данных
DATABASE_URL = os.getenv("DATABASE_URL", "")
USE_POSTGRES = DATABASE_url.startswith("postgresql")

if USE_POSTGRES:
    import asyncpg
else:
    import sqlite3

DB_NAME = "trainings.db"

# ============================================================
# === ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ (ОБЩИЕ) ==============
# ============================================================

def is_date_passed(date_str: str) -> bool:
    """Проверить, прошла ли дата"""
    today = datetime.now().date()
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    return date_obj < today

# ============================================================
# === ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ==============================
# ============================================================

async def init_db():
    """Создание всех таблиц (работает и с SQLite, и с PostgreSQL)"""
    
    if USE_POSTGRES:
        # === POSTGRESQL ===
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Таблица пользователей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                package_total INTEGER DEFAULT 0,
                package_left INTEGER DEFAULT 0,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица тренировок
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS trainings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                datetime TIMESTAMP,
                status TEXT,
                original_datetime TIMESTAMP,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица заявок на запись
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS booking_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                datetime TIMESTAMP,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица открытых окон
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS open_slots (
                id SERIAL PRIMARY KEY,
                week_start DATE,
                datetime TIMESTAMP UNIQUE,
                is_booked BOOLEAN DEFAULT FALSE,
                booked_by BIGINT DEFAULT NULL
            )
        ''')
        
        # Таблица постоянных записей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS recurring_bookings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                weekday INTEGER,
                time TEXT,
                start_date DATE,
                end_date DATE,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица временных отмен постоянных записей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS recurring_cancellations (
                id SERIAL PRIMARY KEY,
                recurring_id INTEGER REFERENCES recurring_bookings(id),
                cancel_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Создаем индексы для ускорения
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_trainings_user_id ON trainings(user_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_trainings_datetime ON trainings(datetime)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_open_slots_week_start ON open_slots(week_start)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_recurring_bookings_user_id ON recurring_bookings(user_id)')
        
        await conn.close()
        print("✅ PostgreSQL: таблицы созданы/проверены")
        
    else:
        # === SQLite ===
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                package_total INTEGER DEFAULT 0,
                package_left INTEGER DEFAULT 0,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица тренировок
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trainings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                datetime TEXT,
                status TEXT,
                original_datetime TEXT,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица заявок на запись
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS booking_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                datetime TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица открытых окон
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS open_slots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start TEXT,
                datetime TEXT UNIQUE,
                is_booked BOOLEAN DEFAULT 0,
                booked_by INTEGER DEFAULT NULL
            )
        ''')
        
        # Таблица постоянных записей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS recurring_bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                weekday INTEGER,
                time TEXT,
                start_date TEXT,
                end_date TEXT,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица временных отмен
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS recurring_cancellations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recurring_id INTEGER,
                cancel_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (recurring_id) REFERENCES recurring_bookings (id)
            )
        ''')
        
        conn.commit()
        conn.close()
        print("✅ SQLite: таблицы созданы/проверены")

# ============================================================
# === ФУНКЦИИ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ =============================
# ============================================================

async def get_user(user_id: int) -> Optional[Dict]:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        await conn.close()
        return dict(row) if row else None
    else:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        user = cursor.fetchone()
        conn.close()
        return dict(user) if user else None

async def get_user_by_username(username: str) -> Optional[Dict]:
    clean_username = username.lstrip('@').lower()
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        row = await conn.fetchrow("SELECT * FROM users WHERE LOWER(username) = $1", clean_username)
        await conn.close()
        return dict(row) if row else None
    else:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE LOWER(username) = ?", (clean_username,))
        user = cursor.fetchone()
        conn.close()
        return dict(user) if user else None

async def get_all_users() -> List[Dict]:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("SELECT user_id, username, full_name, package_left, package_total FROM users ORDER BY full_name")
        await conn.close()
        return [dict(row) for row in rows]
    else:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, username, full_name, package_left, package_total FROM users ORDER BY full_name")
        users = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return users

async def add_user(user_id: int, username: str, full_name: str):
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute('''
            INSERT INTO users (user_id, username, full_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO NOTHING
        ''', user_id, username, full_name)
        await conn.close()
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO users (user_id, username, full_name)
            VALUES (?, ?, ?)
        ''', (user_id, username, full_name))
        conn.commit()
        conn.close()

async def update_package(user_id: int, delta: int):
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        result = await conn.fetchrow("SELECT package_left, package_total FROM users WHERE user_id = $1", user_id)
        if result:
            current_left, current_total = result['package_left'], result['package_total']
            new_left = max(0, current_left + delta)
            if delta > 0:
                new_total = current_total + delta
                await conn.execute('''
                    UPDATE users SET package_left = $1, package_total = $2 WHERE user_id = $3
                ''', new_left, new_total, user_id)
            else:
                await conn.execute('''
                    UPDATE users SET package_left = $1 WHERE user_id = $2
                ''', new_left, user_id)
        else:
            if delta > 0:
                await conn.execute('''
                    INSERT INTO users (user_id, package_total, package_left)
                    VALUES ($1, $2, $3)
                ''', user_id, delta, delta)
        await conn.close()
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT package_left, package_total FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        if result:
            current_left, current_total = result
            new_left = max(0, current_left + delta)
            if delta > 0:
                new_total = current_total + delta
                cursor.execute('''
                    UPDATE users SET package_left = ?, package_total = ? WHERE user_id = ?
                ''', (new_left, new_total, user_id))
            else:
                cursor.execute('''
                    UPDATE users SET package_left = ? WHERE user_id = ?
                ''', (new_left, user_id))
        else:
            if delta > 0:
                cursor.execute('''
                    INSERT INTO users (user_id, package_total, package_left)
                    VALUES (?, ?, ?)
                ''', (user_id, delta, delta))
        conn.commit()
        conn.close()

# ============================================================
# === ФУНКЦИИ ДЛЯ ЗАЯВОК ====================================
# ============================================================

async def create_booking_request(user_id: int, datetime_str: str) -> int:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        row = await conn.fetchrow('''
            INSERT INTO booking_requests (user_id, datetime, status)
            VALUES ($1, $2, 'pending')
            RETURNING id
        ''', user_id, datetime_str)
        await conn.close()
        return row['id']
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO booking_requests (user_id, datetime, status)
            VALUES (?, ?, 'pending')
        ''', (user_id, datetime_str))
        request_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return request_id

async def get_pending_requests() -> List[Dict]:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch('''
            SELECT br.*, u.username, u.full_name 
            FROM booking_requests br
            JOIN users u ON br.user_id = u.user_id
            WHERE br.status = 'pending'
            ORDER BY br.created_at
        ''')
        await conn.close()
        return [dict(row) for row in rows]
    else:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT br.*, u.username, u.full_name 
            FROM booking_requests br
            JOIN users u ON br.user_id = u.user_id
            WHERE br.status = 'pending'
            ORDER BY br.created_at
        ''')
        requests = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return requests

async def approve_request(request_id: int):
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        req = await conn.fetchrow("SELECT user_id, datetime FROM booking_requests WHERE id = $1", request_id)
        if req:
            user_id, datetime_str = req['user_id'], req['datetime']
            result = await conn.fetchrow("SELECT package_left FROM users WHERE user_id = $1", user_id)
            package_left = result['package_left'] if result else 0
            
            if package_left > 0:
                await conn.execute('''
                    INSERT INTO trainings (user_id, datetime, status)
                    VALUES ($1, $2, 'confirmed')
                ''', user_id, datetime_str)
                await conn.execute("UPDATE users SET package_left = package_left - 1 WHERE user_id = $1", user_id)
                training_type = "пакета"
            else:
                await conn.execute('''
                    INSERT INTO trainings (user_id, datetime, status)
                    VALUES ($1, $2, 'confirmed')
                ''', user_id, datetime_str)
                training_type = "разовое"
            
            await conn.execute("UPDATE booking_requests SET status = 'approved' WHERE id = $1", request_id)
            await conn.execute("UPDATE open_slots SET is_booked = TRUE, booked_by = $1 WHERE datetime = $2", user_id, datetime_str)
            await conn.close()
            return training_type, user_id, datetime_str
        await conn.close()
        return None, None, None
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, datetime FROM booking_requests WHERE id = ?", (request_id,))
        req = cursor.fetchone()
        if req:
            user_id, datetime_str = req
            cursor.execute("SELECT package_left FROM users WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            package_left = result[0] if result else 0
            
            if package_left > 0:
                cursor.execute('''
                    INSERT INTO trainings (user_id, datetime, status)
                    VALUES (?, ?, 'confirmed')
                ''', (user_id, datetime_str))
                cursor.execute("UPDATE users SET package_left = package_left - 1 WHERE user_id = ?", (user_id,))
                training_type = "пакета"
            else:
                cursor.execute('''
                    INSERT INTO trainings (user_id, datetime, status)
                    VALUES (?, ?, 'confirmed')
                ''', (user_id, datetime_str))
                training_type = "разовое"
            
            cursor.execute("UPDATE booking_requests SET status = 'approved' WHERE id = ?", (request_id,))
            cursor.execute("UPDATE open_slots SET is_booked = 1, booked_by = ? WHERE datetime = ?", (user_id, datetime_str))
            conn.commit()
            conn.close()
            return training_type, user_id, datetime_str
        conn.close()
        return None, None, None

async def reject_request(request_id: int, reason: str = None):
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        if reason:
            await conn.execute("UPDATE booking_requests SET status = 'rejected', reason = $1 WHERE id = $2", reason, request_id)
        else:
            await conn.execute("UPDATE booking_requests SET status = 'rejected' WHERE id = $1", request_id)
        await conn.close()
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        if reason:
            cursor.execute("UPDATE booking_requests SET status = 'rejected', reason = ? WHERE id = ?", (reason, request_id))
        else:
            cursor.execute("UPDATE booking_requests SET status = 'rejected' WHERE id = ?", (request_id,))
        conn.commit()
        conn.close()

# ============================================================
# === ФУНКЦИИ ДЛЯ ТРЕНИРОВОК ================================
# ============================================================

async def get_user_trainings(user_id: int, status: str = None) -> List[Dict]:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        if status:
            rows = await conn.fetch('''
                SELECT * FROM trainings 
                WHERE user_id = $1 AND status = $2 AND datetime >= NOW()
                ORDER BY datetime
            ''', user_id, status)
        else:
            rows = await conn.fetch('''
                SELECT * FROM trainings 
                WHERE user_id = $1 AND datetime >= NOW()
                ORDER BY datetime
            ''', user_id)
        await conn.close()
        return [dict(row) for row in rows]
    else:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if status:
            cursor.execute('''
                SELECT * FROM trainings 
                WHERE user_id = ? AND status = ? AND datetime >= datetime('now')
                ORDER BY datetime
            ''', (user_id, status))
        else:
            cursor.execute('''
                SELECT * FROM trainings 
                WHERE user_id = ? AND datetime >= datetime('now')
                ORDER BY datetime
            ''', (user_id,))
        trainings = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return trainings

async def get_all_trainings_by_date(date_str: str) -> List[Dict]:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch('''
            SELECT t.*, u.username, u.full_name 
            FROM trainings t
            JOIN users u ON t.user_id = u.user_id
            WHERE DATE(t.datetime) = $1 AND t.status = 'confirmed'
            ORDER BY t.datetime
        ''', date_str)
        await conn.close()
        return [dict(row) for row in rows]
    else:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT t.*, u.username, u.full_name 
            FROM trainings t
            JOIN users u ON t.user_id = u.user_id
            WHERE DATE(t.datetime) = ? AND t.status = 'confirmed'
            ORDER BY t.datetime
        ''', (date_str,))
        trainings = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return trainings

async def cancel_training_by_trainer(training_id: int, reason: str):
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        training = await conn.fetchrow('''
            SELECT t.user_id, t.datetime, u.package_total 
            FROM trainings t
            JOIN users u ON t.user_id = u.user_id
            WHERE t.id = $1 AND t.status = 'confirmed'
        ''', training_id)
        if training:
            user_id, datetime_str, package_total = training['user_id'], training['datetime'], training['package_total']
            await conn.execute('''
                UPDATE trainings SET status = 'cancelled_by_trainer', reason = $1 WHERE id = $2
            ''', reason, training_id)
            if package_total > 0:
                await conn.execute("UPDATE users SET package_left = package_left + 1 WHERE user_id = $1", user_id)
                refund_type = "пакет"
            else:
                refund_type = "разовое (без возврата)"
            await conn.close()
            return True, refund_type, user_id, datetime_str
        await conn.close()
        return False, None, None, None
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT t.user_id, t.datetime, u.package_total 
            FROM trainings t
            JOIN users u ON t.user_id = u.user_id
            WHERE t.id = ? AND t.status = 'confirmed'
        ''', (training_id,))
        training = cursor.fetchone()
        if training:
            user_id, datetime_str, package_total = training
            cursor.execute('''
                UPDATE trainings SET status = 'cancelled_by_trainer', reason = ? WHERE id = ?
            ''', (reason, training_id))
            if package_total > 0:
                cursor.execute("UPDATE users SET package_left = package_left + 1 WHERE user_id = ?", (user_id,))
                refund_type = "пакет"
            else:
                refund_type = "разовое (без возврата)"
            conn.commit()
            conn.close()
            return True, refund_type, user_id, datetime_str
        conn.close()
        return False, None, None, None

async def cancel_trainings_bulk(date_str: str = None, time_start: str = None, time_end: str = None, reason: str = "") -> int:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        query = "SELECT id, user_id FROM trainings WHERE status = 'confirmed'"
        params = []
        if date_str:
            query += " AND DATE(datetime) = $" + str(len(params) + 1)
            params.append(date_str)
            if time_start and time_end:
                query += " AND TIME(datetime) >= $" + str(len(params) + 1) + " AND TIME(datetime) <= $" + str(len(params) + 2)
                params.append(time_start)
                params.append(time_end)
        
        rows = await conn.fetch(query, *params)
        if not rows:
            await conn.close()
            return 0
        
        for row in rows:
            await conn.execute("UPDATE trainings SET status = 'cancelled_by_trainer', reason = $1 WHERE id = $2", reason, row['id'])
            await conn.execute("UPDATE users SET package_left = package_left + 1 WHERE user_id = $1", row['user_id'])
        
        await conn.close()
        return len(rows)
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        query = "SELECT id, user_id FROM trainings WHERE status = 'confirmed'"
        params = []
        if date_str:
            query += " AND DATE(datetime) = ?"
            params.append(date_str)
            if time_start and time_end:
                query += " AND TIME(datetime) >= ? AND TIME(datetime) <= ?"
                params.append(time_start)
                params.append(time_end)
        
        cursor.execute(query, params)
        trainings = cursor.fetchall()
        if not trainings:
            conn.close()
            return 0
        
        for training_id, user_id in trainings:
            cursor.execute("UPDATE trainings SET status = 'cancelled_by_trainer', reason = ? WHERE id = ?", (reason, training_id))
            cursor.execute("UPDATE users SET package_left = package_left + 1 WHERE user_id = ?", (user_id,))
        
        conn.commit()
        conn.close()
        return len(trainings)

# ============================================================
# === ФУНКЦИИ ДЛЯ ОТКРЫТЫХ СЛОТОВ ===========================
# ============================================================

async def clear_weekly_slots(week_start: str):
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        result = await conn.execute("DELETE FROM open_slots WHERE week_start = $1", week_start)
        await conn.close()
        print(f"DEBUG: Удалено слотов: {result.split()[-1] if result else 0}")
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM open_slots WHERE week_start = ?", (week_start,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        print(f"DEBUG: Удалено слотов: {deleted}")

async def add_open_slot(datetime_str: str, week_start: str):
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute('''
            INSERT INTO open_slots (datetime, week_start, is_booked)
            VALUES ($1, $2, FALSE)
            ON CONFLICT (datetime) DO NOTHING
        ''', datetime_str, week_start)
        await conn.close()
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO open_slots (datetime, week_start, is_booked)
            VALUES (?, ?, 0)
        ''', (datetime_str, week_start))
        conn.commit()
        conn.close()

async def get_free_slots(week_start: str) -> List[str]:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch('''
            SELECT datetime FROM open_slots 
            WHERE week_start = $1 AND is_booked = FALSE
            ORDER BY datetime
        ''', week_start)
        await conn.close()
        slots = [row['datetime'] for row in rows]
        print(f"DEBUG: get_free_slots для {week_start} -> {len(slots)} слотов")
        return slots
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT datetime FROM open_slots 
            WHERE week_start = ? AND is_booked = 0
            ORDER BY datetime
        ''', (week_start,))
        slots = [row[0] for row in cursor.fetchall()]
        conn.close()
        print(f"DEBUG: get_free_slots для {week_start} -> {len(slots)} слотов")
        return slots

async def get_all_slots_for_week(week_start: str) -> List[str]:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch('SELECT datetime FROM open_slots WHERE week_start = $1 ORDER BY datetime', week_start)
        await conn.close()
        return [row['datetime'] for row in rows]
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('SELECT datetime FROM open_slots WHERE week_start = ? ORDER BY datetime', (week_start,))
        slots = [row[0] for row in cursor.fetchall()]
        conn.close()
        return slots

async def sync_slots_with_trainings(week_start: str):
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        week_end = (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
        rows = await conn.fetch('''
            SELECT datetime, user_id FROM trainings 
            WHERE DATE(datetime) BETWEEN $1 AND $2 AND status = 'confirmed'
        ''', week_start, week_end)
        for row in rows:
            await conn.execute('''
                UPDATE open_slots SET is_booked = TRUE, booked_by = $1 
                WHERE datetime = $2 AND week_start = $3
            ''', row['user_id'], row['datetime'], week_start)
        await conn.close()
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        week_end = (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
        cursor.execute('''
            SELECT datetime, user_id FROM trainings 
            WHERE DATE(datetime) BETWEEN ? AND ? AND status = 'confirmed'
        ''', (week_start, week_end))
        trainings = cursor.fetchall()
        for datetime_str, user_id in trainings:
            cursor.execute('''
                UPDATE open_slots SET is_booked = 1, booked_by = ? 
                WHERE datetime = ? AND week_start = ?
            ''', (user_id, datetime_str, week_start))
        conn.commit()
        conn.close()

async def close_week_slots(week_start: str) -> int:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        result = await conn.execute("DELETE FROM open_slots WHERE week_start = $1", week_start)
        await conn.close()
        return int(result.split()[-1]) if result else 0
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM open_slots WHERE week_start = ?", (week_start,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted

async def get_week_status(week_start: str) -> dict:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        count = await conn.fetchval("SELECT COUNT(*) FROM open_slots WHERE week_start = $1", week_start)
        await conn.close()
        return {"has_slots": count > 0, "slots_count": count}
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM open_slots WHERE week_start = ?", (week_start,))
        count = cursor.fetchone()[0]
        conn.close()
        return {"has_slots": count > 0, "slots_count": count}

# ============================================================
# === ФУНКЦИИ ДЛЯ ПОСТОЯННЫХ ЗАПИСЕЙ ========================
# ============================================================

async def add_recurring_booking(user_id: int, weekday: int, time_str: str, start_date: str, end_date: str = None) -> int:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        row = await conn.fetchrow('''
            INSERT INTO recurring_bookings (user_id, weekday, time, start_date, end_date, is_active)
            VALUES ($1, $2, $3, $4, $5, TRUE)
            RETURNING id
        ''', user_id, weekday, time_str, start_date, end_date)
        await conn.close()
        return row['id']
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO recurring_bookings (user_id, weekday, time, start_date, end_date, is_active)
            VALUES (?, ?, ?, ?, ?, 1)
        ''', (user_id, weekday, time_str, start_date, end_date))
        booking_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return booking_id

async def get_recurring_bookings(user_id: int = None) -> List[Dict]:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        if user_id:
            rows = await conn.fetch('''
                SELECT rb.*, u.username, u.full_name 
                FROM recurring_bookings rb
                JOIN users u ON rb.user_id = u.user_id
                WHERE rb.is_active = TRUE AND rb.user_id = $1
                ORDER BY rb.weekday, rb.time
            ''', user_id)
        else:
            rows = await conn.fetch('''
                SELECT rb.*, u.username, u.full_name 
                FROM recurring_bookings rb
                JOIN users u ON rb.user_id = u.user_id
                WHERE rb.is_active = TRUE
                ORDER BY rb.weekday, rb.time
            ''')
        await conn.close()
        return [dict(row) for row in rows]
    else:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if user_id:
            cursor.execute('''
                SELECT rb.*, u.username, u.full_name 
                FROM recurring_bookings rb
                JOIN users u ON rb.user_id = u.user_id
                WHERE rb.is_active = 1 AND rb.user_id = ?
                ORDER BY rb.weekday, rb.time
            ''', (user_id,))
        else:
            cursor.execute('''
                SELECT rb.*, u.username, u.full_name 
                FROM recurring_bookings rb
                JOIN users u ON rb.user_id = u.user_id
                WHERE rb.is_active = 1
                ORDER BY rb.weekday, rb.time
            ''')
        bookings = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return bookings

async def get_recurring_bookings_for_week(week_start: str, week_end: str) -> List[Dict]:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch('''
            SELECT rb.*, u.username, u.full_name 
            FROM recurring_bookings rb
            JOIN users u ON rb.user_id = u.user_id
            WHERE rb.is_active = TRUE
            AND (rb.end_date IS NULL OR rb.end_date >= $1)
            AND rb.start_date <= $2
            ORDER BY rb.weekday, rb.time
        ''', week_start, week_end)
        await conn.close()
        return [dict(row) for row in rows]
    else:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT rb.*, u.username, u.full_name 
            FROM recurring_bookings rb
            JOIN users u ON rb.user_id = u.user_id
            WHERE rb.is_active = 1
            AND (rb.end_date IS NULL OR rb.end_date >= ?)
            AND rb.start_date <= ?
            ORDER BY rb.weekday, rb.time
        ''', (week_start, week_end))
        bookings = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return bookings

async def add_temporary_cancellation(recurring_id: int, cancel_date: str) -> bool:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            await conn.execute('''
                INSERT INTO recurring_cancellations (recurring_id, cancel_date)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
            ''', recurring_id, cancel_date)
            await conn.close()
            return True
        except:
            await conn.close()
            return False
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO recurring_cancellations (recurring_id, cancel_date)
            VALUES (?, ?)
        ''', (recurring_id, cancel_date))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0

async def deactivate_recurring_booking(booking_id: int) -> bool:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        result = await conn.execute("UPDATE recurring_bookings SET is_active = FALSE WHERE id = $1", booking_id)
        await conn.close()
        return result != "UPDATE 0"
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("UPDATE recurring_bookings SET is_active = 0 WHERE id = ?", (booking_id,))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0

async def update_recurring_booking(booking_id: int, new_weekday: int, new_time: str) -> bool:
    if USE_POSTGRES:
        conn = await asyncpg.connect(DATABASE_URL)
        result = await conn.execute('''
            UPDATE recurring_bookings 
            SET weekday = $1, time = $2 
            WHERE id = $3

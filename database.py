import os
import asyncpg
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Получаем URL из переменных окружения
DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_NAME = "trainings.db"  # для совместимости со старым кодом

async def get_connection():
    """Получить соединение с Supabase/PostgreSQL"""
    return await asyncpg.connect(DATABASE_URL)

async def init_db():
    """Создание всех таблиц в PostgreSQL/Supabase"""
    conn = await get_connection()
    
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
    
    # Таблица временных отмен
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS recurring_cancellations (
            id SERIAL PRIMARY KEY,
            recurring_id INTEGER REFERENCES recurring_bookings(id),
            cancel_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Индексы
    await conn.execute('CREATE INDEX IF NOT EXISTS idx_trainings_user_id ON trainings(user_id)')
    await conn.execute('CREATE INDEX IF NOT EXISTS idx_trainings_datetime ON trainings(datetime)')
    await conn.execute('CREATE INDEX IF NOT EXISTS idx_open_slots_week_start ON open_slots(week_start)')
    await conn.execute('CREATE INDEX IF NOT EXISTS idx_recurring_bookings_user_id ON recurring_bookings(user_id)')
    
    await conn.close()
    print("✅ PostgreSQL: таблицы созданы/проверены")

# ============================================================
# === ФУНКЦИИ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ =============================
# ============================================================

async def get_user(user_id: int) -> Optional[Dict]:
    conn = await get_connection()
    row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    await conn.close()
    return dict(row) if row else None

async def get_user_by_username(username: str) -> Optional[Dict]:
    clean_username = username.lstrip('@').lower()
    conn = await get_connection()
    row = await conn.fetchrow("SELECT * FROM users WHERE LOWER(username) = $1", clean_username)
    await conn.close()
    return dict(row) if row else None

async def get_all_users() -> List[Dict]:
    conn = await get_connection()
    rows = await conn.fetch("SELECT user_id, username, full_name, package_left, package_total FROM users ORDER BY full_name")
    await conn.close()
    return [dict(row) for row in rows]

async def add_user(user_id: int, username: str, full_name: str):
    conn = await get_connection()
    await conn.execute('''
        INSERT INTO users (user_id, username, full_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id) DO NOTHING
    ''', user_id, username, full_name)
    await conn.close()

async def update_package(user_id: int, delta: int):
    conn = await get_connection()
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

# ============================================================
# === ФУНКЦИИ ДЛЯ ЗАЯВОК ====================================
# ============================================================

async def create_booking_request(user_id: int, datetime_str: str) -> int:
    conn = await get_connection()
    row = await conn.fetchrow('''
        INSERT INTO booking_requests (user_id, datetime, status)
        VALUES ($1, $2, 'pending')
        RETURNING id
    ''', user_id, datetime_str)
    await conn.close()
    return row['id']

async def get_pending_requests() -> List[Dict]:
    conn = await get_connection()
    rows = await conn.fetch('''
        SELECT br.*, u.username, u.full_name 
        FROM booking_requests br
        JOIN users u ON br.user_id = u.user_id
        WHERE br.status = 'pending'
        ORDER BY br.created_at
    ''')
    await conn.close()
    return [dict(row) for row in rows]

async def approve_request(request_id: int):
    conn = await get_connection()
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

async def reject_request(request_id: int, reason: str = None):
    conn = await get_connection()
    if reason:
        await conn.execute("UPDATE booking_requests SET status = 'rejected', reason = $1 WHERE id = $2", reason, request_id)
    else:
        await conn.execute("UPDATE booking_requests SET status = 'rejected' WHERE id = $1", request_id)
    await conn.close()

# ============================================================
# === ФУНКЦИИ ДЛЯ ТРЕНИРОВОК ================================
# ============================================================

async def get_user_trainings(user_id: int, status: str = None) -> List[Dict]:
    conn = await get_connection()
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

async def get_all_trainings_by_date(date_str: str) -> List[Dict]:
    conn = await get_connection()
    rows = await conn.fetch('''
        SELECT t.*, u.username, u.full_name 
        FROM trainings t
        JOIN users u ON t.user_id = u.user_id
        WHERE DATE(t.datetime) = $1 AND t.status = 'confirmed'
        ORDER BY t.datetime
    ''', date_str)
    await conn.close()
    return [dict(row) for row in rows]

async def cancel_training_by_trainer(training_id: int, reason: str):
    conn = await get_connection()
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

async def cancel_trainings_bulk(date_str: str = None, time_start: str = None, time_end: str = None, reason: str = "") -> int:
    conn = await get_connection()
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

# ============================================================
# === ФУНКЦИИ ДЛЯ ОТКРЫТЫХ СЛОТОВ ===========================
# ============================================================

async def clear_weekly_slots(week_start: str):
    conn = await get_connection()
    await conn.execute("DELETE FROM open_slots WHERE week_start = $1", week_start)
    await conn.close()

async def add_open_slot(datetime_str: str, week_start: str):
    conn = await get_connection()
    await conn.execute('''
        INSERT INTO open_slots (datetime, week_start, is_booked)
        VALUES ($1, $2, FALSE)
        ON CONFLICT (datetime) DO NOTHING
    ''', datetime_str, week_start)
    await conn.close()

async def get_free_slots(week_start: str) -> List[str]:
    conn = await get_connection()
    rows = await conn.fetch('''
        SELECT datetime FROM open_slots 
        WHERE week_start = $1 AND is_booked = FALSE
        ORDER BY datetime
    ''', week_start)
    await conn.close()
    return [row['datetime'] for row in rows]

async def get_all_slots_for_week(week_start: str) -> List[str]:
    conn = await get_connection()
    rows = await conn.fetch('SELECT datetime FROM open_slots WHERE week_start = $1 ORDER BY datetime', week_start)
    await conn.close()
    return [row['datetime'] for row in rows]

async def sync_slots_with_trainings(week_start: str):
    conn = await get_connection()
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

async def close_week_slots(week_start: str) -> int:
    conn = await get_connection()
    result = await conn.execute("DELETE FROM open_slots WHERE week_start = $1", week_start)
    await conn.close()
    try:
        return int(result.split()[-1])
    except:
        return 0

async def get_week_status(week_start: str) -> dict:
    conn = await get_connection()
    count = await conn.fetchval("SELECT COUNT(*) FROM open_slots WHERE week_start = $1", week_start)
    await conn.close()
    return {"has_slots": count > 0, "slots_count": count}

# ============================================================
# === ФУНКЦИИ ДЛЯ ПОСТОЯННЫХ ЗАПИСЕЙ ========================
# ============================================================

async def add_recurring_booking(user_id: int, weekday: int, time_str: str, start_date: str, end_date: str = None) -> int:
    conn = await get_connection()
    row = await conn.fetchrow('''
        INSERT INTO recurring_bookings (user_id, weekday, time, start_date, end_date, is_active)
        VALUES ($1, $2, $3, $4, $5, TRUE)
        RETURNING id
    ''', user_id, weekday, time_str, start_date, end_date)
    await conn.close()
    return row['id']

async def get_recurring_bookings(user_id: int = None) -> List[Dict]:
    conn = await get_connection()
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

async def get_recurring_bookings_for_week(week_start: str, week_end: str) -> List[Dict]:
    conn = await get_connection()
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

async def add_temporary_cancellation(recurring_id: int, cancel_date: str) -> bool:
    conn = await get_connection()
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

async def deactivate_recurring_booking(booking_id: int) -> bool:
    conn = await get_connection()
    result = await conn.execute("UPDATE recurring_bookings SET is_active = FALSE WHERE id = $1", booking_id)
    await conn.close()
    return result != "UPDATE 0"

async def update_recurring_booking(booking_id: int, new_weekday: int, new_time: str) -> bool:
    conn = await get_connection()
    result = await conn.execute('''
        UPDATE recurring_bookings 
        SET weekday = $1, time = $2 
        WHERE id = $3 AND is_active = TRUE
    ''', new_weekday, new_time, booking_id)
    await conn.close()
    return result != "UPDATE 0"

async def update_recurring_end_date(booking_id: int, end_date: str) -> bool:
    conn = await get_connection()
    result = await conn.execute("UPDATE recurring_bookings SET end_date = $1 WHERE id = $2", end_date, booking_id)
    await conn.close()
    return result != "UPDATE 0"

async def get_recurring_bookings_by_user(user_id: int) -> List[Dict]:
    conn = await get_connection()
    rows = await conn.fetch('''
        SELECT * FROM recurring_bookings 
        WHERE user_id = $1 AND is_active = TRUE
        ORDER BY weekday, time
    ''', user_id)
    await conn.close()
    return [dict(row) for row in rows]

# ============================================================
# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===============================
# ============================================================

def is_date_passed(date_str: str) -> bool:
    """Проверить, прошла ли дата"""
    today = datetime.now().date()
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    return date_obj < today

async def cleanup_old_data():
    """Очищает старые тренировки (старше 7 дней)"""
    try:
        conn = await get_connection()
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        await conn.execute('''
            DELETE FROM trainings 
            WHERE datetime < $1 AND status IN ('completed', 'cancelled', 'cancelled_by_trainer')
        ''', week_ago)
        
        week_ago_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        await conn.execute('''
            DELETE FROM recurring_cancellations 
            WHERE cancel_date < $1
        ''', week_ago_date)
        
        await conn.close()
        print("🧹 Очистка БД выполнена")
    except Exception as e:
        print(f"Ошибка очистки БД: {e}")

import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional

DB_NAME = "trainings.db"

def init_db():
    """Создание всех таблиц"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Таблица пользователей (клиентов)
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
    
    # Таблица открытых окон (для записи)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS open_slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start DATE,
            datetime TEXT UNIQUE,
            is_booked BOOLEAN DEFAULT 0,
            booked_by INTEGER DEFAULT NULL
        )
    ''')
    
    # Таблица постоянных записей (рекуррентных тренировок)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS recurring_bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            weekday INTEGER,
            time TEXT,
            start_date DATE,
            end_date DATE,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')
    
    # Таблица временных отмен постоянных записей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS recurring_cancellations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recurring_id INTEGER,
            cancel_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (recurring_id) REFERENCES recurring_bookings (id)
        )
    ''')
    
    conn.commit()
    conn.close()

# === Функции для пользователей ===
def get_user(user_id: int) -> Optional[Dict]:
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = cursor.fetchone()
    conn.close()
    return dict(user) if user else None

def get_user_by_username(username: str) -> Optional[Dict]:
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    clean_username = username.lstrip('@').lower()
    cursor.execute("SELECT * FROM users WHERE LOWER(username) = ?", (clean_username,))
    user = cursor.fetchone()
    conn.close()
    return dict(user) if user else None

def get_all_users() -> List[Dict]:
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, username, full_name, package_left, package_total FROM users ORDER BY full_name")
    users = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return users

def add_user(user_id: int, username: str, full_name: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO users (user_id, username, full_name)
        VALUES (?, ?, ?)
    ''', (user_id, username, full_name))
    conn.commit()
    conn.close()

def update_package(user_id: int, delta: int):
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

# === Функции для заявок ===
def create_booking_request(user_id: int, datetime_str: str) -> int:
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

def get_pending_requests() -> List[Dict]:
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

def approve_request(request_id: int):
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

def reject_request(request_id: int, reason: str = None):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    if reason:
        cursor.execute("UPDATE booking_requests SET status = 'rejected', reason = ? WHERE id = ?", (reason, request_id))
    else:
        cursor.execute("UPDATE booking_requests SET status = 'rejected' WHERE id = ?", (request_id,))
    conn.commit()
    conn.close()

# === Функции для тренировок ===
def get_user_trainings(user_id: int, status: str = None) -> List[Dict]:
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

def get_free_slots(week_start: str) -> List[str]:
    """Получить все свободные слоты на неделю"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Получаем все слоты, которые не помечены как занятые
    cursor.execute('''
        SELECT datetime FROM open_slots 
        WHERE week_start = ? AND is_booked = 0
        ORDER BY datetime
    ''', (week_start,))
    slots = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    # Отладка - выводим все даты, для которых есть слоты
    if slots:
        dates = set(s.split()[0] for s in slots)
        print(f"DEBUG: get_free_slots для {week_start} -> {len(slots)} слотов на даты: {sorted(dates)}")
    else:
        print(f"DEBUG: get_free_slots для {week_start} -> НЕТ СЛОТОВ!")
    
    return slots
def get_all_trainings_by_date(date_str: str) -> List[Dict]:
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

def cancel_training_by_trainer(training_id: int, reason: str):
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

def cancel_trainings_bulk(date_str: str = None, time_start: str = None, time_end: str = None, reason: str = "") -> int:
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

# === Функции для открытых слотов ===
def clear_weekly_slots(week_start: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM open_slots WHERE week_start = ?", (week_start,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"DEBUG: Удалено старых слотов для недели {week_start}: {deleted}")

def add_open_slot(datetime_str: str, week_start: str):
    """Добавить свободный слот для записи"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO open_slots (datetime, week_start, is_booked)
            VALUES (?, ?, 0)
        ''', (datetime_str, week_start))
        conn.commit()
    except Exception as e:
        print(f"ERROR add_open_slot: {e}")
    finally:
        conn.close()

def get_free_slots(week_start: str) -> List[str]:
    """Получить все свободные слоты на неделю"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT datetime FROM open_slots 
        WHERE week_start = ? AND is_booked = 0
        ORDER BY datetime
    ''', (week_start,))
    slots = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    # Убираем фильтрацию по trainings, так как она уже учтена в is_booked
    # Если слот помечен как is_booked=1, он не попадет в выборку
    
    return slots

def get_week_slots_with_status(week_start: str) -> Dict:
    """Получить все слоты на неделю с их статусом"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Получаем открытые слоты
    cursor.execute('''
        SELECT datetime, is_booked, booked_by FROM open_slots 
        WHERE week_start = ?
        ORDER BY datetime
    ''', (week_start,))
    slots = cursor.fetchall()
    
    # Получаем подтверждённые тренировки
    week_end = (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
    cursor.execute('''
        SELECT datetime, user_id FROM trainings 
        WHERE status = 'confirmed' 
        AND DATE(datetime) BETWEEN ? AND ?
    ''', (week_start, week_end))
    trainings = {row[0]: row[1] for row in cursor.fetchall()}
    
    conn.close()
    
    result = {}
    for slot_dt, is_booked, booked_by in slots:
        if slot_dt in trainings:
            result[slot_dt] = {'status': 'booked', 'user_id': trainings[slot_dt]}
        elif is_booked:
            result[slot_dt] = {'status': 'booked', 'user_id': booked_by}
        else:
            result[slot_dt] = {'status': 'free', 'user_id': None}
    
    return result

def sync_slots_with_trainings(week_start: str):
    """Синхронизировать open_slots с существующими тренировками"""
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

def close_week_slots(week_start: str) -> int:
    """Закрыть запись на неделю (удалить все слоты)"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM open_slots WHERE week_start = ?", (week_start,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted

def get_week_status(week_start: str) -> dict:
    """Проверить, открыта ли запись на неделю"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM open_slots WHERE week_start = ?", (week_start,))
    count = cursor.fetchone()[0]
    conn.close()
    return {"has_slots": count > 0, "slots_count": count}

def is_date_passed(date_str: str) -> bool:
    """Проверить, прошла ли дата"""
    today = datetime.now().date()
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    return date_obj < today

def get_available_weeks_for_booking() -> List[Dict]:
    """Получить доступные недели для записи (текущая и следующая)"""
    today = datetime.now()
    weeks = []
    
    # Текущая неделя (только будущие дни)
    current_week_start = today - timedelta(days=today.weekday())
    current_week_end = current_week_start + timedelta(days=6)
    
    # Следующая неделя
    next_week_start = current_week_start + timedelta(days=7)
    next_week_end = next_week_start + timedelta(days=6)
    
    weeks.append({
        'type': 'current',
        'label': 'Текущая неделя',
        'start': current_week_start.strftime("%Y-%m-%d"),
        'end': current_week_end.strftime("%Y-%m-%d"),
        'can_book': True  # Можно записываться только на будущие дни
    })
    
    weeks.append({
        'type': 'next',
        'label': 'Следующая неделя',
        'start': next_week_start.strftime("%Y-%m-%d"),
        'end': next_week_end.strftime("%Y-%m-%d"),
        'can_book': True
    })
    
    return weeks

# === Функции для постоянных записей ===
def add_recurring_booking(user_id: int, weekday: int, time_str: str, start_date: str, end_date: str = None) -> int:
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

def get_recurring_bookings(user_id: int = None) -> List[Dict]:
    """Получить все активные постоянные записи"""
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

def get_recurring_bookings_for_week(week_start: str, week_end: str) -> List[Dict]:
    """Получить постоянные записи, действующие в указанную неделю"""
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
    
    # Отладочный вывод
    print(f"DEBUG: get_recurring_bookings_for_week({week_start}, {week_end}) -> {len(bookings)} записей")
    for b in bookings:
        print(f"  - {b['full_name']}: день {b['weekday']} время {b['time']} (до {b['end_date']})")
    
    return bookings

def get_recurring_cancellations(recurring_id: int, week_start: str, week_end: str) -> List[str]:
    """Получить даты отмен для постоянной записи"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT cancel_date FROM recurring_cancellations 
        WHERE recurring_id = ? AND cancel_date BETWEEN ? AND ?
    ''', (recurring_id, week_start, week_end))
    cancellations = [row[0] for row in cursor.fetchall()]
    conn.close()
    return cancellations

def add_temporary_cancellation(recurring_id: int, cancel_date: str) -> bool:
    """Добавить временную отмену постоянной записи"""
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

def deactivate_recurring_booking(booking_id: int) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("UPDATE recurring_bookings SET is_active = 0 WHERE id = ?", (booking_id,))
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    return affected > 0

def update_recurring_booking(booking_id: int, new_weekday: int, new_time: str) -> bool:
    """Обновить постоянную запись (перенести)"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE recurring_bookings 
        SET weekday = ?, time = ? 
        WHERE id = ? AND is_active = 1
    ''', (new_weekday, new_time, booking_id))
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"DEBUG: Обновлена запись {booking_id}: день={new_weekday}, время={new_time}, affected={affected}")
    return affected > 0

def update_recurring_end_date(booking_id: int, end_date: str) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("UPDATE recurring_bookings SET end_date = ? WHERE id = ?", (end_date, booking_id))
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    return affected > 0

def update_recurring_booking(booking_id: int, new_weekday: int, new_time: str) -> bool:
    """Обновить постоянную запись (перенести)"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE recurring_bookings 
        SET weekday = ?, time = ? 
        WHERE id = ? AND is_active = 1
    ''', (new_weekday, new_time, booking_id))
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"DEBUG: Обновлена запись {booking_id}: день={new_weekday}, время={new_time}, affected={affected}")
    return affected > 0

def get_recurring_bookings_by_user(user_id: int) -> List[Dict]:
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM recurring_bookings 
        WHERE user_id = ? AND is_active = 1
        ORDER BY weekday, time
    ''', (user_id,))
    bookings = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return bookings
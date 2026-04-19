import asyncio
import re
import sqlite3
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN, TRAINER_ID, WORK_HOURS_START, WORK_HOURS_END
from database import *
from keyboards import *

# Инициализация
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Состояния для FSM
class BookingStates(StatesGroup):
    waiting_for_date = State()

class TrainerStates(StatesGroup):
    waiting_for_slots = State()
    waiting_for_overwrite_confirm = State()
    waiting_for_cancel_date = State()
    waiting_for_cancel_reason = State()
    waiting_for_mass_day = State()
    waiting_for_mass_time_start = State()
    waiting_for_mass_time_end = State()
    waiting_for_mass_reason = State()
    waiting_for_package_user = State()
    waiting_for_broadcast = State()
    waiting_for_recurring_end_date = State()
    waiting_for_recurring_end_update = State()
    selecting_slots_day = State()
    editing_slots = State()
    waiting_for_recurring_action = State()
    waiting_for_recurring_cancel_type = State()
    waiting_for_recurring_skip_date = State()

def get_next_week_start():
    """Возвращает (next_monday, week_start_str, week_end_str)"""
    today = datetime.now()
    days_until_next_monday = (7 - today.weekday()) % 7
    if days_until_next_monday == 0:
        days_until_next_monday = 7
    next_monday = today + timedelta(days=days_until_next_monday)
    week_start = next_monday.strftime("%Y-%m-%d")
    week_end = (next_monday + timedelta(days=6)).strftime("%Y-%m-%d")
    return next_monday, week_start, week_end

def get_current_week_start():
    """Возвращает (current_monday, week_start_str, week_end_str) для текущей недели"""
    today = datetime.now()
    current_monday = today - timedelta(days=today.weekday())
    week_start = current_monday.strftime("%Y-%m-%d")
    week_end = (current_monday + timedelta(days=6)).strftime("%Y-%m-%d")
    return current_monday, week_start, week_end

def get_week_days_with_status(week_start: str, week_type: str = "next"):
    """Получить дни недели с информацией о том, прошли они или нет"""
    today = datetime.now()
    week_start_date = datetime.strptime(week_start, "%Y-%m-%d")
    days = []
    
    day_names = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    
    for i in range(7):
        current_date = week_start_date + timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        is_passed = current_date.date() < today.date()
        
        days.append({
            'date': date_str,
            'display': f"{day_names[i]} {current_date.strftime('%d.%m')}",
            'is_passed': is_passed,
            'day_index': i
        })
    
    return days

def format_schedule_text(trainings_by_day: dict, week_label: str) -> str:
    """Форматирует расписание для тренера (со всеми именами)"""
    days_order = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    
    text = f"🏋️‍♀️ *Расписание на {week_label}*\n\n"
    
    for day_name in days_order:
        day_data = trainings_by_day.get(day_name, {})
        
        locked_count = sum(1 for name in day_data.values() if name)
        
        if locked_count > 0:
            text += f"📌 *{day_name}* (🔒 {locked_count} занятий)\n"
        else:
            text += f"📌 *{day_name}*\n"
        
        for hour in range(9, 22):
            time_key = f"{hour:02d}:00"
            if hour == 21:
                time_key = "21:00"
            
            name = day_data.get(time_key, "")
            if name:
                text += f"{time_key} {name}\n"
            else:
                text += f"{time_key}\n"
        
        text += "\n"
    
    return text

def format_schedule_for_client(week_start: datetime, week_label: str, client_user_id: int, recurring_bookings: dict, can_book: bool = True) -> str:
    """Форматирует расписание для клиента"""
    days_order = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    
    if can_book:
        text = f"🏋️‍♀️ *Расписание на {week_label}*\n\n"
        text += "🔒 = занято\n"
        text += "✅ = ваша запись\n"
        text += "⏰ = свободно\n\n"
    else:
        text = f"🏋️‍♀️ *Расписание на {week_label}*\n\n"
        text += "🔍 *ТОЛЬКО ПРОСМОТР* (запись закрыта)\n\n"
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    for i, day_name in enumerate(days_order):
        current_date = week_start + timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        
        cursor.execute('''
            SELECT datetime, user_id 
            FROM trainings 
            WHERE DATE(datetime) = ? AND status = 'confirmed'
        ''', (date_str,))
        
        regular_trainings = {}
        for row in cursor.fetchall():
            dt = datetime.strptime(row[0], "%Y-%m-%d %H:%M")
            time_key = dt.strftime("%H:%M")
            regular_trainings[time_key] = row[1]
        
        locked_count = 0
        for hour in range(9, 22):
            time_key = f"{hour:02d}:00"
            if hour == 21:
                time_key = "21:00"
            
            recurring_key = f"{i}_{time_key}"
            recurring_user_id = recurring_bookings.get(recurring_key)
            
            if recurring_user_id or time_key in regular_trainings:
                locked_count += 1
        
        if locked_count > 0:
            text += f"📌 *{day_name}* (🔒 {locked_count} занятий)\n"
        else:
            text += f"📌 *{day_name}*\n"
        
        for hour in range(9, 22):
            time_key = f"{hour:02d}:00"
            if hour == 21:
                time_key = "21:00"
            
            recurring_key = f"{i}_{time_key}"
            recurring_user_id = recurring_bookings.get(recurring_key)
            
            if recurring_user_id:
                if recurring_user_id == client_user_id:
                    text += f"{time_key} *ВЫ* 🔒\n"
                else:
                    text += f"{time_key} 🔒\n"
            elif time_key in regular_trainings:
                if regular_trainings[time_key] == client_user_id:
                    text += f"{time_key} *ВЫ*\n"
                else:
                    text += f"{time_key} 🔒\n"
            else:
                text += f"{time_key}\n"
        
        text += "\n"
    
    conn.close()
    
    if can_book:
        text += "\n📝 *Как записаться:* Используйте кнопку ✍️ Записаться в главном меню"
    
    return text

# === ОСНОВНЫЕ КОМАНДЫ ===

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    await add_user(user_id, message.from_user.username, message.from_user.full_name)
    
    is_trainer = (user_id == TRAINER_ID)
    await message.answer(
        f"🏋️ Добро пожаловать в фитнес-бот, {message.from_user.full_name}!\n\n"
        f"Здесь вы можете:\n"
        f"✅ Записываться на тренировки\n"
        f"✅ Отслеживать свой пакет\n"
        f"✅ Переносить или отменять тренировки\n\n"
        f"Используйте меню для навигации 👇",
        reply_markup=main_menu_keyboard(is_trainer)
    )

@dp.message(Command("my_schedule"))
@dp.message(lambda message: message.text == "📅 Мои тренировки")
async def my_schedule(message: types.Message):
    user_id = message.from_user.id
    trainings = await get_user_trainings(user_id, 'confirmed')
    
    if not trainings:
        await message.answer("📭 У вас нет предстоящих тренировок.\nЗаписаться: /book")
        return
    
    text = "📋 *Ваши предстоящие тренировки:*\n\n"
    for t in trainings:
        dt = datetime.strptime(t['datetime'], "%Y-%m-%d %H:%M")
        text += f"📅 {dt.strftime('%d.%m.%Y %H:%M')}\n"
    
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("book"))
@dp.message(lambda message: message.text == "✍️ Записаться")
async def book_training(message: types.Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    
    if not user:
        await message.answer("❌ Пожалуйста, начните с команды /start")
        return
    
    await message.answer(
        "🗓️ *Запись на тренировку*\n\nВыберите неделю:",
        parse_mode="Markdown",
        reply_markup=booking_week_keyboard()
    )

@dp.message(Command("my_package"))
@dp.message(lambda message: message.text == "📦 Мой пакет")
async def my_package(message: types.Message):
    user = await get_user(message.from_user.id)
    if user:
        package_left = user['package_left']
        package_total = user['package_total']
        
        if package_total > 0:
            await message.answer(
                f"📦 *Ваши тренировки*\n\nОсталось занятий: *{package_left}* из {package_total}",
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                f"🎫 *Разовые занятия*\n\nУ вас нет активного пакета.\nДля покупки пакета обратитесь к тренеру.",
                parse_mode="Markdown"
            )
    else:
        await message.answer("❌ Пользователь не найден.")

@dp.message(Command("history"))
@dp.message(lambda message: message.text == "📊 Моя статистика")
async def my_history(message: types.Message):
    user_id = message.from_user.id
    last_week = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT status, COUNT(*) FROM trainings 
        WHERE user_id = ? AND DATE(datetime) >= ?
        GROUP BY status
    ''', (user_id, last_week))
    stats = cursor.fetchall()
    conn.close()
    
    stat_dict = dict(stats)
    text = f"📊 *Ваша статистика за неделю*\n\n"
    text += f"✅ Проведено: {stat_dict.get('completed', 0)}\n"
    text += f"📝 Запланировано: {stat_dict.get('confirmed', 0)}\n"
    text += f"❌ Отменено: {stat_dict.get('cancelled', 0) + stat_dict.get('cancelled_by_trainer', 0)}\n"
    
    await message.answer(text, parse_mode="Markdown")

# === ЗАПИСЬ НА ТРЕНИРОВКУ ===

@dp.callback_query(lambda c: c.data.startswith("book_week_"))
async def select_week_for_booking(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    week_type = callback.data.split("_")[2]
    today = datetime.now()
    
    if week_type == "current":
        week_start = today - timedelta(days=today.weekday())
        week_label = "текущую"
    else:
        days_until_monday = (7 - today.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        week_start = today + timedelta(days=days_until_monday)
        week_label = "следующую"
    
    week_start_str = week_start.strftime("%Y-%m-%d")
    free_slots = await get_free_slots(week_start_str)
    
    if not free_slots:
        await callback.message.edit_text(
            f"❌ *Запись на {week_label} неделю ещё не открыта!*\n\n"
            f"Тренер должен открыть запись через панель тренера.",
            parse_mode="Markdown"
        )
        await callback.answer()
        return
    
    days_with_slots = {}
    for slot in free_slots:
        date_str = slot.split()[0]
        if date_str not in days_with_slots:
            days_with_slots[date_str] = []
        days_with_slots[date_str].append(slot)
    
    days_list = get_week_days_with_status(week_start_str, week_type)
    for day in days_list:
        date_str = day['date']
        slots = days_with_slots.get(date_str, [])
        day['display'] = f"{day['display']} ({len(slots)} окон)"
    
    await state.update_data(week_slots=days_with_slots, week_type=week_type, week_start=week_start_str)
    
    await callback.message.edit_text(
        f"🗓️ *Выберите день* на {week_label} неделю (с {week_start.strftime('%d.%m')}):\n\n"
        f"🔒 - день уже прошёл, запись невозможна",
        parse_mode="Markdown",
        reply_markup=day_selection_keyboard(days_list, week_type)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("select_day_"))
async def select_time_for_booking(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    date_str = parts[2]
    week_type = parts[3] if len(parts) > 3 else "next"
    
    if is_date_passed(date_str):
        await callback.message.edit_text("❌ Нельзя записаться на прошедшую дату.")
        await callback.answer()
        return
    
    data = await state.get_data()
    week_slots = data.get('week_slots', {})
    slots = week_slots.get(date_str, [])
    
    if not slots:
        await callback.message.edit_text("❌ На этот день нет свободных окон.")
        await callback.answer()
        return
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT datetime FROM trainings WHERE DATE(datetime) = ? AND status = "confirmed"', (date_str,))
    booked = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    available_slots = [s for s in slots if s not in booked]
    
    if not available_slots:
        await callback.message.edit_text("❌ На этот день все слоты уже заняты.")
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"🗓️ *{date_str}*\n\nВыберите время:",
        parse_mode="Markdown",
        reply_markup=time_slots_keyboard(available_slots, date_str, week_type)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("time_"))
async def process_booking_time(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    parts = callback.data.split("_")
    
    if len(parts) < 4:
        await callback.answer("Ошибка!")
        return
    
    date_str = parts[1]
    datetime_str = parts[2]
    week_type = parts[3]
    
    if is_date_passed(date_str):
        await callback.message.edit_text("❌ Нельзя записаться на прошедшую дату!")
        await callback.answer()
        return
    
    existing = await get_user_trainings(user_id, 'confirmed')
    for t in existing:
        if t['datetime'] == datetime_str:
            await callback.message.edit_text("❌ Вы уже записаны на это время!")
            await callback.answer()
            return
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('SELECT is_booked FROM open_slots WHERE datetime = ?', (datetime_str,))
    slot = cursor.fetchone()
    
    if not slot or slot[0] == 1:
        await callback.message.edit_text("❌ Это время уже занято другим клиентом!")
        await callback.answer()
        conn.close()
        return
    
    cursor.execute('SELECT id FROM trainings WHERE datetime = ? AND status = "confirmed"', (datetime_str,))
    if cursor.fetchone():
        await callback.message.edit_text("❌ Это время уже занято!")
        await callback.answer()
        conn.close()
        return
    
    conn.close()
    
    await create_booking_request(user_id, datetime_str)
    user = await get_user(user_id)
    package_info = f"пакет: {user['package_left']} занятий" if user['package_total'] > 0 else "разовые занятия"
    
    await bot.send_message(
        TRAINER_ID,
        f"📝 *НОВАЯ ЗАЯВКА*\n\n"
        f"Клиент: {user['full_name']} (@{callback.from_user.username})\n"
        f"Время: {datetime_str}\n"
        f"Статус: {package_info}",
        parse_mode="Markdown"
    )
    
    await callback.message.edit_text(
        f"✅ *Заявка отправлена!*\n\n📅 {datetime_str}\n\nОжидайте подтверждения тренера.",
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_days")
async def back_to_days(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    week_slots = data.get('week_slots', {})
    week_type = data.get('week_type', 'next')
    week_start = data.get('week_start')
    
    days_list = get_week_days_with_status(week_start, week_type)
    for day in days_list:
        date_str = day['date']
        slots = week_slots.get(date_str, [])
        day['display'] = f"{day['display']} ({len(slots)} окон)"
    
    await callback.message.edit_text(
        "🗓️ *Выберите день:*",
        parse_mode="Markdown",
        reply_markup=day_selection_keyboard(days_list, week_type)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_booking")
async def cancel_booking(callback: types.CallbackQuery):
    await callback.message.edit_text("❌ Запись отменена.")
    await callback.answer()

# === РАСПИСАНИЕ ===

@dp.message(Command("schedule"))
@dp.message(lambda message: message.text == "📅 Расписание")
async def show_client_schedule(message: types.Message):
    user_id = message.from_user.id
    is_trainer = (user_id == TRAINER_ID)
    
    if is_trainer:
        await message.answer(
            "📅 *Просмотр расписания*\n\nВыберите неделю:",
            parse_mode="Markdown",
            reply_markup=schedule_week_keyboard()
        )
    else:
        await message.answer(
            "📅 *Просмотр расписания*\n\n"
            "• Текущая неделя — только просмотр\n"
            "• Следующая неделя — доступна запись\n\n"
            "Выберите неделю:",
            parse_mode="Markdown",
            reply_markup=client_schedule_week_keyboard()
        )

@dp.callback_query(lambda c: c.data.startswith("client_week_"))
async def show_client_week_schedule(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    week_type = callback.data.split("_")[2]
    today = datetime.now()
    
    if week_type == "current":
        week_start = today - timedelta(days=today.weekday())
        week_label = "текущую неделю"
        can_book = False
    else:
        week_start = today + timedelta(days=(7 - today.weekday()))
        week_label = "следующую неделю"
        week_start_str = week_start.strftime("%Y-%m-%d")
        week_status = await get_week_status(week_start_str)
        can_book = week_status["has_slots"]
    
    week_end = week_start + timedelta(days=6)
    recurring_bookings = await get_recurring_bookings_for_week(week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d"))
    
    recurring_dict = {}
    for rb in recurring_bookings:
        for i in range(7):
            current_date = week_start + timedelta(days=i)
            if current_date.weekday() == rb['weekday']:
                if rb['end_date'] is None or rb['end_date'] >= current_date.strftime("%Y-%m-%d"):
                    if rb['start_date'] <= current_date.strftime("%Y-%m-%d"):
                        recurring_dict[f"{i}_{rb['time']}"] = rb['user_id']
    
    text = format_schedule_for_client(week_start, week_label, user_id, recurring_dict, can_book)
    
    if not can_book and week_type == "next":
        text += "\n\n⚠️ *Запись на эту неделю закрыта тренером*\n"
        text += "Вы можете только просматривать расписание."
    
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()

# === ПАНЕЛЬ ТРЕНЕРА ===

@dp.message(Command("admin_panel"))
@dp.message(lambda message: message.text == "👨‍💼 Панель тренера")
async def admin_panel(message: types.Message):
    if message.from_user.id != TRAINER_ID:
        await message.answer("⛔ У вас нет доступа.")
        return
    
    await message.answer("👨‍💼 *Панель управления тренера*\n\nВыберите действие:", 
                        parse_mode="Markdown", 
                        reply_markup=admin_panel_keyboard())

@dp.callback_query(lambda c: c.data == "admin_schedule")
async def admin_schedule_menu(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    await callback.message.edit_text(
        "📅 *Просмотр расписания*\n\nВыберите неделю:",
        parse_mode="Markdown",
        reply_markup=schedule_week_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("week_"))
async def show_week_schedule(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    week_type = callback.data.split("_")[1]
    today = datetime.now()
    
    if week_type == "current":
        week_start = today - timedelta(days=today.weekday())
        week_label = "текущую неделю"
    else:
        week_start = today + timedelta(days=(7 - today.weekday()))
        week_label = "следующую неделю"
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    trainings_by_day = {}
    days_full = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    
    for i in range(7):
        current_date = week_start + timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        day_name = days_full[i]
        
        cursor.execute('''
            SELECT datetime, u.full_name 
            FROM trainings t
            JOIN users u ON t.user_id = u.user_id
            WHERE DATE(t.datetime) = ? AND t.status = 'confirmed'
            ORDER BY t.datetime
        ''', (date_str,))
        
        day_trainings = {}
        for row in cursor.fetchall():
            dt = datetime.strptime(row[0], "%Y-%m-%d %H:%M")
            time_key = dt.strftime("%H:%M")
            day_trainings[time_key] = row[1]
        
        trainings_by_day[day_name] = day_trainings
    
    conn.close()
    text = format_schedule_text(trainings_by_day, week_label)
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_requests")
async def show_requests(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    requests = await get_pending_requests()
    if not requests:
        await callback.message.edit_text("📭 Нет новых заявок.")
        return
    
    await callback.message.edit_text(
        "📋 *Неподтверждённые заявки:*",
        parse_mode="Markdown",
        reply_markup=pending_requests_keyboard(requests)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("approve_"))
async def approve_booking(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    request_id = int(callback.data.split("_")[1])
    
    training_type, user_id, datetime_str = await approve_request(request_id)
    user = await get_user(user_id)
    package_left = user['package_left'] if user else 0
    package_total = user['package_total'] if user else 0
    
    if training_type == "пакета":
        message_text = f"✅ *Тренировка подтверждена!*\n\n📅 {datetime_str}\n\n🏋️ Занятие списано из вашего пакета.\n📦 *Остаток: {package_left} из {package_total}*"
    else:
        message_text = f"✅ *Тренировка подтверждена!*\n\n📅 {datetime_str}\n\n🎫 Разовое занятие."
    
    try:
        await bot.send_message(user_id, message_text, parse_mode="Markdown")
    except Exception as e:
        print(f"Не удалось уведомить пользователя: {e}")
    
    await callback.message.edit_text(f"✅ Заявка подтверждена! ({training_type})")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("reject_"))
async def reject_booking(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    request_id = int(callback.data.split("_")[1])
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT br.user_id, br.datetime, u.full_name, u.username 
        FROM booking_requests br
        JOIN users u ON br.user_id = u.user_id
        WHERE br.id = ?
    ''', (request_id,))
    req = cursor.fetchone()
    conn.close()
    
    if not req:
        await callback.message.edit_text("❌ Заявка не найдена.")
        await callback.answer()
        return
    
    user_id, datetime_str, full_name, username = req
    await reject_request(request_id)
    
    try:
        await bot.send_message(
            user_id,
            f"❌ *Тренировка отклонена*\n\n"
            f"📅 {datetime_str}\n\n"
            f"Попробуйте записаться на другое время: /book",
            parse_mode="Markdown"
        )
    except Exception as e:
        print(f"Не удалось уведомить пользователя: {e}")
    
    await callback.message.edit_text(
        f"❌ *Заявка отклонена!*\n\n"
        f"👤 Клиент: {full_name} (@{username or 'нет юзернейма'})\n"
        f"📅 Время: {datetime_str}\n\n"
        f"Клиент уведомлён об отклонении.",
        parse_mode="Markdown"
    )
    await callback.answer()

# === ОТКРЫТИЕ ЗАПИСИ ===

@dp.callback_query(lambda c: c.data == "admin_slots")
async def open_slots_menu(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Текущая неделя", callback_data="open_slots_week_current")],
        [InlineKeyboardButton(text="📅 Следующая неделя", callback_data="open_slots_week_next")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_admin")]
    ])
    
    await callback.message.edit_text(
        "📅 *Выберите неделю для открытия записи*\n\n"
        "⚠️ Внимание: при открытии записи на текущую неделю\n"
        "будут доступны только дни, которые ещё не прошли.",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("open_slots_week_"))
async def open_slots_week_selection(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    week_type = callback.data.split("_")[3]
    today = datetime.now()
    
    if week_type == "current":
        week_start_date, week_start, week_end = get_current_week_start()
        week_label = "текущую"
    else:
        week_start_date, week_start, week_end = get_next_week_start()
        week_label = "следующую"
    
    week_status = await get_week_status(week_start)
    
    if week_status["has_slots"]:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, перезаписать", callback_data=f"confirm_overwrite_{week_type}")],
            [InlineKeyboardButton(text="❌ Нет, оставить как есть", callback_data="cancel_overwrite")],
            [InlineKeyboardButton(text="🔒 Закрыть запись", callback_data=f"admin_close_slots_{week_type}")]
        ])
        
        await callback.message.edit_text(
            f"⚠️ ЗАПИСЬ УЖЕ ОТКРЫТА!\n\n"
            f"📅 Неделя: {week_start} - {week_end}\n"
            f"📊 Сейчас открыто: {week_status['slots_count']} слотов\n\n"
            f"• Перезаписать — удалить старые слоты и создать новые\n"
            f"• Закрыть запись — удалить все слоты\n\n"
            f"Что хотите сделать?",
            reply_markup=keyboard
        )
        await callback.answer()
        return
    
    await state.update_data(
        busy_slots=[],
        week_start=week_start,
        week_end=week_end,
        week_start_date=week_start_date,
        week_type=week_type
    )
    
    await callback.message.edit_text(
        f"📅 ОТКРЫТИЕ ЗАПИСИ НА {week_label.upper()} НЕДЕЛЮ\n\n"
        f"🗓️ Неделя: {week_start} - {week_end}\n\n"
        f"Сейчас вы отметите занятые окна (когда вы НЕ можете тренировать).\n"
        f"Остальное время станет доступно для записи клиентам.\n\n"
        f"👇 Выберите день недели:",
        reply_markup=open_slots_weekday_keyboard()
    )
    await state.set_state(TrainerStates.selecting_slots_day)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("open_slots_") and not c.data.startswith("open_slots_back") and not c.data.startswith("open_slots_finish") and not c.data.startswith("open_slots_week"))
async def select_day_for_slots(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    day_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
    day_key = callback.data.split("_")[2]
    day_index = day_map.get(day_key, 0)
    
    data = await state.get_data()
    week_start_date = data.get('week_start_date')
    week_type = data.get('week_type', 'next')
    
    current_date = week_start_date + timedelta(days=day_index)
    day_name = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'][day_index]
    date_str = current_date.strftime("%d.%m")
    
    is_passed = False
    if week_type == 'current':
        is_passed = current_date.date() < datetime.now().date()
    
    busy_slots = data.get('busy_slots', [])
    day_busy = [t.split()[1] for t in busy_slots if t.startswith(current_date.strftime("%Y-%m-%d"))]
    
    await state.update_data(current_day=day_name, current_date=current_date.strftime("%Y-%m-%d"))
    
    if is_passed:
        await callback.message.edit_text(
            f"🔒 *{day_name} {date_str}*\n\n"
            f"Этот день уже прошёл. Редактирование невозможно.\n\n"
            f"Выберите другой день:",
            parse_mode="Markdown",
            reply_markup=open_slots_weekday_keyboard()
        )
    else:
        await callback.message.edit_text(
            f"📅 *{day_name} {date_str}*\n\n"
            f"Нажмите на время, чтобы отметить его как ЗАНЯТОЕ (🔒).\n"
            f"Свободное время (⏰) будет доступно клиентам.\n\n"
            f"Текущие занятые окна: {', '.join(day_busy) if day_busy else 'нет'}",
            parse_mode="Markdown",
            reply_markup=open_slots_time_keyboard(day_name, date_str, day_busy, is_passed)
        )
    
    await state.set_state(TrainerStates.editing_slots)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("toggle_time_"))
async def toggle_time_slot(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    parts = callback.data.split("_")
    day_name = parts[2]
    time_key = parts[3]
    
    data = await state.get_data()
    current_date = data.get('current_date')
    busy_slots = data.get('busy_slots', [])
    
    datetime_str = f"{current_date} {time_key}"
    
    if datetime_str in busy_slots:
        busy_slots.remove(datetime_str)
        status = "свободно"
    else:
        busy_slots.append(datetime_str)
        status = "занято"
    
    await state.update_data(busy_slots=busy_slots)
    
    date_obj = datetime.strptime(current_date, "%Y-%m-%d")
    date_str = date_obj.strftime("%d.%m")
    day_busy = [t.split()[1] for t in busy_slots if t.startswith(current_date)]
    
    await callback.message.edit_text(
        f"📅 *{day_name} {date_str}*\n\n"
        f"✅ Время {time_key} отмечено как {status}\n"
        f"Текущие занятые окна: {', '.join(day_busy) if day_busy else 'нет'}",
        parse_mode="Markdown",
        reply_markup=open_slots_time_keyboard(day_name, date_str, day_busy, False)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "open_slots_back_days")
async def back_to_days_selection(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    data = await state.get_data()
    week_start = data.get('week_start')
    week_end = data.get('week_end')
    
    await callback.message.edit_text(
        f"📅 *Открытие записи на неделю*\n\n"
        f"🗓️ Неделя: {week_start} - {week_end}\n\n"
        f"👇 *Выберите день недели:*",
        parse_mode="Markdown",
        reply_markup=open_slots_weekday_keyboard()
    )
    await state.set_state(TrainerStates.selecting_slots_day)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "open_slots_finish")
async def finish_opening_slots(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    data = await state.get_data()
    busy_slots = data.get('busy_slots', [])
    week_start = data.get('week_start')
    week_start_date = data.get('week_start_date')
    week_type = data.get('week_type', 'next')
    
    total_slots = 0
    days_info = []
    today = datetime.now().date()
    
    for day in range(7):
        current_date = week_start_date + timedelta(days=day)
        day_name = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'][day]
        day_slots = 0
        
        if week_type == 'current' and current_date.date() < today:
            days_info.append(f"{day_name}: (день прошёл, слоты не созданы)")
            continue
        
        for hour in range(WORK_HOURS_START, WORK_HOURS_END):
            datetime_str = current_date.strftime(f"%Y-%m-%d {hour:02d}:00")
            if datetime_str not in busy_slots:
                day_slots += 1
                total_slots += 1
        
        days_info.append(f"{day_name}: {day_slots} свободных окон")
    
    await callback.message.edit_text(
        f"📅 *Подтверждение открытия записи*\n\n"
        f"🗓️ Неделя: {week_start}\n"
        f"🔒 Занятых окон: {len(busy_slots)}\n"
        f"⏰ Свободных окон: {total_slots}\n\n"
        f"📋 *По дням:*\n" + "\n".join(days_info) + "\n\n"
        f"Открыть запись для клиентов?",
        parse_mode="Markdown",
        reply_markup=open_slots_confirm_keyboard(week_start)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_open_"))
async def confirm_open_slots(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    data = await state.get_data()
    busy_slots = data.get('busy_slots', [])
    week_start = data.get('week_start')
    week_start_date = data.get('week_start_date')
    week_type = data.get('week_type', 'next')
    
    await clear_weekly_slots(week_start)
    
    created = 0
    days_created = []
    day_names = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    today = datetime.now().date()
    
    for day in range(7):
        current_date = week_start_date + timedelta(days=day)
        day_name = day_names[day]
        day_slots = 0
        
        if week_type == 'current' and current_date.date() < today:
            days_created.append(f"{day_name}: (день прошёл, слоты не созданы)")
            continue
        
        for hour in range(WORK_HOURS_START, WORK_HOURS_END):
            datetime_str = current_date.strftime(f"%Y-%m-%d {hour:02d}:00")
            if datetime_str not in busy_slots:
                await add_open_slot(datetime_str, week_start)
                created += 1
                day_slots += 1
        
        days_created.append(f"{day_name}: {day_slots} слотов")
    
    await sync_slots_with_trainings(week_start)
    
    await callback.message.edit_text(
        f"✅ *Запись успешно открыта!*\n\n"
        f"🗓️ Неделя: {week_start}\n"
        f"⏰ Создано {created} свободных окон\n"
        f"🔒 Занятых окон: {len(busy_slots)}\n\n"
        f"📋 *По дням:*\n" + "\n".join(days_created),
        parse_mode="Markdown"
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_overwrite_"))
async def confirm_overwrite(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    week_type = callback.data.split("_")[2]
    today = datetime.now()
    
    if week_type == "current":
        week_start_date, week_start, week_end = get_current_week_start()
    else:
        week_start_date, week_start, week_end = get_next_week_start()
    
    await close_week_slots(week_start)
    
    await callback.message.answer("🗑️ Старые слоты удалены. Теперь откройте запись заново.")
    
    await state.update_data(
        busy_slots=[],
        week_start=week_start,
        week_end=week_end,
        week_start_date=week_start_date,
        week_type=week_type
    )
    
    await callback.message.edit_text(
        f"📅 *Открытие записи на {week_type.upper()} неделю*\n\n"
        f"🗓️ Неделя: {week_start} - {week_end}\n\n"
        f"👇 *Выберите день недели:*",
        parse_mode="Markdown",
        reply_markup=open_slots_weekday_keyboard()
    )
    await state.set_state(TrainerStates.selecting_slots_day)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_overwrite")
async def cancel_overwrite(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    await callback.message.edit_text("✅ Оставил существующее расписание без изменений.")
    await state.clear()
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("admin_close_slots_"))
async def close_slots_menu(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    week_type = callback.data.split("_")[3]
    
    if week_type == "current":
        _, week_start, week_end = get_current_week_start()
    else:
        _, week_start, week_end = get_next_week_start()
    
    week_status = await get_week_status(week_start)
    
    if not week_status["has_slots"]:
        await callback.message.edit_text(
            f"❌ ЗАПИСЬ УЖЕ ЗАКРЫТА!\n\n"
            f"📅 Неделя: {week_start} - {week_end}\n\n"
            f"Сейчас нет открытых слотов для записи.",
        )
        await callback.answer()
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, закрыть запись", callback_data=f"confirm_close_slots_{week_type}")],
        [InlineKeyboardButton(text="❌ Нет, оставить", callback_data="cancel_close_slots")]
    ])
    
    await callback.message.edit_text(
        f"⚠️ ЗАКРЫТИЕ ЗАПИСИ\n\n"
        f"📅 Неделя: {week_start} - {week_end}\n"
        f"📊 Сейчас открыто: {week_status['slots_count']} слотов\n\n"
        f"После закрытия клиенты НЕ СМОГУТ записаться на эту неделю.\n"
        f"Существующие записи клиентов останутся.\n\n"
        f"Закрыть запись?",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_close_slots_"))
async def confirm_close_slots(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    week_type = callback.data.split("_")[3]
    
    if week_type == "current":
        _, week_start, week_end = get_current_week_start()
    else:
        _, week_start, week_end = get_next_week_start()
    
    deleted = await close_week_slots(week_start)
    
    await callback.message.edit_text(
        f"✅ ЗАПИСЬ ЗАКРЫТА!\n\n"
        f"📅 Неделя: {week_start} - {week_end}\n"
        f"🗑️ Удалено слотов: {deleted}\n\n"
        f"Клиенты больше не могут записаться на эту неделю.\n"
        f"Существующие записи клиентов сохранены."
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_close_slots")
async def cancel_close_slots(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    await callback.message.edit_text(f"✅ Запись осталась открытой.")
    await callback.answer()

# === ОТМЕНА ТРЕНИРОВОК ===

@dp.callback_query(lambda c: c.data == "admin_cancel")
async def cancel_training_menu(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    today = datetime.now().strftime("%Y-%m-%d")
    trainings = await get_all_trainings_by_date(today)
    
    if not trainings:
        await callback.message.edit_text("📭 Сегодня нет тренировок.")
        return
    
    text = "📋 *Тренировки на сегодня:*\n\n"
    for t in trainings:
        dt = datetime.strptime(t['datetime'], "%Y-%m-%d %H:%M")
        text += f"ID:{t['id']} - {t['full_name']} - {dt.strftime('%H:%M')}\n"
    
    text += "\nОтправьте: /cancel_training ID ПРИЧИНА"
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()

@dp.message(Command("cancel_training"))
async def cancel_single_training(message: types.Message):
    if message.from_user.id != TRAINER_ID:
        return
    
    parts = message.text.split(" ", 2)
    if len(parts) < 2:
        await message.answer("❌ Использование: /cancel_training ID [причина]")
        return
    
    training_id = int(parts[1])
    reason = parts[2] if len(parts) > 2 else "Не указана"
    
    result, refund_type, user_id, datetime_str = await cancel_training_by_trainer(training_id, reason)
    
    if result:
        try:
            await bot.send_message(user_id, f"⚠️ *Тренировка отменена*\n\n{datetime_str}\nПричина: {reason}", parse_mode="Markdown")
        except Exception as e:
            print(f"Не удалось уведомить пользователя: {e}")
        await message.answer(f"✅ Тренировка отменена. ({refund_type})")
    else:
        await message.answer("❌ Тренировка не найдена.")

@dp.callback_query(lambda c: c.data == "admin_mass_cancel")
async def mass_cancel_menu(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    await callback.message.edit_text(
        "🗓️ *Массовая отмена*\n\nВыберите тип:",
        parse_mode="Markdown",
        reply_markup=mass_cancel_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "mass_day")
async def mass_cancel_day(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    await callback.message.edit_text("📅 Введите дату (ГГГГ-ММ-ДД):")
    await state.set_state(TrainerStates.waiting_for_mass_day)
    await callback.answer()

@dp.message(TrainerStates.waiting_for_mass_day)
async def process_mass_day(message: types.Message, state: FSMContext):
    date_str = message.text.strip()
    if not re.match(r"\d{4}-\d{2}-\d{2}", date_str):
        await message.answer("❌ Неверный формат.")
        return
    await state.update_data(mass_date=date_str)
    await message.answer("📝 Введите причину:")
    await state.set_state(TrainerStates.waiting_for_mass_reason)

@dp.callback_query(lambda c: c.data == "mass_time")
async def mass_cancel_time(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    await callback.message.edit_text("📅 Введите дату (ГГГГ-ММ-ДД):")
    await state.set_state(TrainerStates.waiting_for_mass_time_start)
    await callback.answer()

@dp.message(TrainerStates.waiting_for_mass_time_start)
async def process_mass_time_date(message: types.Message, state: FSMContext):
    date_str = message.text.strip()
    if not re.match(r"\d{4}-\d{2}-\d{2}", date_str):
        await message.answer("❌ Неверный формат")
        return
    await state.update_data(mass_date=date_str)
    await message.answer("⏰ Введите НАЧАЛО (ЧЧ:ММ):")
    await state.set_state(TrainerStates.waiting_for_mass_time_end)

@dp.message(TrainerStates.waiting_for_mass_time_end)
async def process_mass_time_range(message: types.Message, state: FSMContext):
    time_str = message.text.strip()
    if not re.match(r"\d{2}:\d{2}", time_str):
        await message.answer("❌ Неверный формат")
        return
    await state.update_data(mass_time_start=time_str)
    await message.answer("⏰ Введите КОНЕЦ (ЧЧ:ММ):")
    await state.set_state(TrainerStates.waiting_for_mass_reason)

@dp.message(TrainerStates.waiting_for_mass_reason)
async def process_mass_reason(message: types.Message, state: FSMContext):
    reason = message.text.strip()
    data = await state.get_data()
    date_str = data.get('mass_date')
    time_start = data.get('mass_time_start')
    time_end = data.get('mass_time_end')
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    if time_start and time_end:
        cursor.execute('''SELECT user_id, datetime FROM trainings 
            WHERE DATE(datetime) = ? AND TIME(datetime) >= ? AND TIME(datetime) <= ?
            AND status = 'confirmed' ''', (date_str, time_start, time_end))
    else:
        cursor.execute('''SELECT user_id, datetime FROM trainings 
            WHERE DATE(datetime) = ? AND status = 'confirmed' ''', (date_str,))
    
    affected = cursor.fetchall()
    conn.close()
    
    count = await cancel_trainings_bulk(date_str, time_start, time_end, reason)
    
    if count > 0:
        await message.answer(f"✅ Отменено {count} тренировок.")
        for user_id, dt in affected:
            try:
                await bot.send_message(user_id, f"⚠️ *Тренировка отменена*\n\n{dt}\nПричина: {reason}", parse_mode="Markdown")
            except Exception as e:
                print(f"Не удалось уведомить пользователя: {e}")
    else:
        await message.answer("❌ Нет тренировок для отмены.")
    
    await state.clear()

# === УПРАВЛЕНИЕ ПАКЕТАМИ ===

@dp.callback_query(lambda c: c.data == "admin_add_package")
async def add_package_menu(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    users = await get_all_users()
    if users:
        user_list = "📋 *Клиенты:*\n"
        for u in users:
            user_list += f"• {u['full_name']} - @{u['username'] or 'нет'} (пакет: {u['package_left']}/{u['package_total']})\n"
        user_list += "\n"
    else:
        user_list = "📭 Нет клиентов.\n\n"
    
    await callback.message.edit_text(
        f"{user_list}➕ *Добавить пакет*\n\nВведите: @username количество\nПример: `@john 10`",
        parse_mode="Markdown"
    )
    await state.set_state(TrainerStates.waiting_for_package_user)
    await callback.answer()

@dp.message(TrainerStates.waiting_for_package_user)
async def process_add_package(message: types.Message, state: FSMContext):
    if message.from_user.id != TRAINER_ID:
        return
    
    try:
        parts = message.text.strip().split()
        if len(parts) < 2:
            await message.answer("❌ Формат: @username количество")
            return
        
        username = parts[0].lstrip('@')
        amount = int(parts[1])
        user = await get_user_by_username(username)
        
        if not user:
            await message.answer(f"❌ Пользователь @{username} не найден")
            return
        
        old_package = user['package_left']
        await update_package(user['user_id'], amount)
        
        await message.answer(f"✅ {user['full_name']}: +{amount} занятий (было {old_package}, стало {old_package + amount})")
        try:
            await bot.send_message(user['user_id'], f"🎉 +{amount} занятий!\nТеперь у вас: {old_package + amount}", parse_mode="Markdown")
        except Exception as e:
            print(f"Не удалось уведомить пользователя: {e}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
    
    await state.clear()

# === ОТЧЁТЫ ===

@dp.callback_query(lambda c: c.data == "admin_report")
async def weekly_report(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    last_week = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT status, COUNT(*) FROM trainings WHERE DATE(datetime) >= ? GROUP BY status", (last_week,))
    stats = cursor.fetchall()
    
    next_week_end = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d")
    cursor.execute('''SELECT datetime, u.full_name FROM trainings t
        JOIN users u ON t.user_id = u.user_id
        WHERE DATE(datetime) BETWEEN date('now') AND ? AND t.status = 'confirmed'
        ORDER BY datetime''', (next_week_end,))
    upcoming = cursor.fetchall()
    conn.close()
    
    stat_dict = dict(stats)
    text = f"📊 *Отчёт за неделю*\n\n✅ Проведено: {stat_dict.get('completed', 0)}\n📝 Запланировано: {stat_dict.get('confirmed', 0)}\n❌ Отменено: {stat_dict.get('cancelled', 0)}\n⚠️ Отменено тренером: {stat_dict.get('cancelled_by_trainer', 0)}\n\n📅 *Расписание:*\n"
    
    if upcoming:
        for u in upcoming[:15]:
            dt = datetime.strptime(u[0], "%Y-%m-%d %H:%M")
            text += f"• {dt.strftime('%d.%m %H:%M')} - {u[1]}\n"
    else:
        text += "Нет тренировок"
    
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_today")
async def today_schedule(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    today = datetime.now().strftime("%Y-%m-%d")
    trainings = await get_all_trainings_by_date(today)
    
    if not trainings:
        await callback.message.edit_text(f"📭 На сегодня ({today}) нет тренировок.")
        return
    
    text = f"📌 *Расписание на сегодня*\n\n"
    for t in trainings:
        dt = datetime.strptime(t['datetime'], "%Y-%m-%d %H:%M")
        text += f"⏰ {dt.strftime('%H:%M')} - {t['full_name']}\n"
    
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()

# === РАССЫЛКА ===

@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def broadcast_menu(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    await callback.message.edit_text("📢 *Рассылка*\n\nВведите сообщение для всех клиентов:", parse_mode="Markdown")
    await state.set_state(TrainerStates.waiting_for_broadcast)
    await callback.answer()

@dp.message(TrainerStates.waiting_for_broadcast)
async def process_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id != TRAINER_ID:
        return
    
    users = await get_all_users()
    sent = 0
    failed = 0
    
    await message.answer(f"📢 Рассылка {len(users)} пользователям...")
    
    for user in users:
        try:
            await bot.send_message(user['user_id'], f"📢 *Сообщение от тренера*\n\n{message.text}", parse_mode="Markdown")
            sent += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    
    await message.answer(f"✅ Отправлено: {sent}\n❌ Не доставлено: {failed}")
    await state.clear()

@dp.callback_query(lambda c: c.data == "back_admin")
async def back_to_admin(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    await callback.message.edit_text("👨‍💼 *Панель тренера*", parse_mode="Markdown", reply_markup=admin_panel_keyboard())
    await callback.answer()

# === ПОСТОЯННЫЕ ЗАПИСИ ===

@dp.callback_query(lambda c: c.data == "admin_recurring")
async def recurring_menu(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    await callback.message.edit_text(
        "🔄 *Управление постоянными записями*\n\n"
        "Постоянные записи — это клиенты, которые занимаются в одно и то же время КАЖДУЮ неделю.\n\n"
        "Они автоматически отображаются в расписании как 🔒 и недоступны для записи другим.\n\n"
        "Выберите действие:",
        parse_mode="Markdown",
        reply_markup=recurring_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "recurring_add")
async def recurring_add_user_select(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    users = await get_all_users()
    if not users:
        await callback.message.edit_text("📭 Нет зарегистрированных клиентов.")
        return
    
    await callback.message.edit_text(
        "👤 *Выберите клиента* для постоянной записи:",
        parse_mode="Markdown",
        reply_markup=recurring_user_select_keyboard(users)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("recurring_user_"))
async def recurring_add_day_select(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    user_id = int(callback.data.split("_")[2])
    await state.update_data(recurring_user_id=user_id)
    
    await callback.message.edit_text(
        "📅 *Выберите день недели*:",
        parse_mode="Markdown",
        reply_markup=recurring_weekday_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("recurring_day_"))
async def recurring_add_time_select(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    weekday = int(callback.data.split("_")[2])
    await state.update_data(recurring_weekday=weekday)
    
    await callback.message.edit_text(
        "⏰ *Выберите время*:",
        parse_mode="Markdown",
        reply_markup=recurring_time_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("recurring_time_"))
async def recurring_add_duration(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    time_str = callback.data.split("_")[2]
    await state.update_data(recurring_time=time_str)
    
    data = await state.get_data()
    user_id = data.get('recurring_user_id')
    
    if not user_id:
        await callback.message.edit_text("❌ Ошибка: пользователь не найден! Пожалуйста, начните добавление заново.", parse_mode="Markdown")
        await callback.answer()
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Бессрочно", callback_data="recurring_duration_forever")],
        [InlineKeyboardButton(text="📅 Ввести свою дату", callback_data="recurring_duration_custom")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="recurring_back_time")]
    ])
    
    await callback.message.edit_text(
        "📅 *Как долго действует постоянная запись?*\n\n"
        "• Бессрочно — запись действует постоянно\n"
        "• Своя дата — укажите конкретную дату окончания",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("recurring_duration_"))
async def recurring_add_save(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    duration_type = callback.data.split("_")[2]
    data = await state.get_data()
    
    user_id = data.get('recurring_user_id')
    
    if not user_id:
        await callback.message.edit_text("❌ Ошибка: пользователь не найден! Пожалуйста, начните добавление заново.", parse_mode="Markdown")
        await callback.answer()
        return
    
    weekday = data.get('recurring_weekday')
    time_str = data.get('recurring_time')
    start_date = datetime.now().strftime("%Y-%m-%d")
    
    if duration_type == "forever":
        end_date = None
        duration_text = "бессрочно"
        await add_recurring_booking(user_id, weekday, time_str, start_date, end_date)
        user = await get_user(user_id)
        if user:
            days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
            
            await callback.message.edit_text(
                f"✅ *Постоянная запись добавлена!*\n\n"
                f"👤 Клиент: {user['full_name']}\n"
                f"📅 День: {days[weekday]}\n"
                f"⏰ Время: {time_str}\n"
                f"📅 Действует: {duration_text}",
                parse_mode="Markdown"
            )
            
            try:
                await bot.send_message(
                    user_id,
                    f"🔄 *Вам добавлена постоянная запись!*\n\n"
                    f"📅 День: {days[weekday]}\n"
                    f"⏰ Время: {time_str}\n"
                    f"📅 Действует: {duration_text}",
                    parse_mode="Markdown"
                )
            except Exception as e:
                print(f"Не удалось уведомить пользователя: {e}")
        else:
            await callback.message.edit_text("❌ Пользователь не найден!", parse_mode="Markdown")
        
        await state.clear()
        await callback.answer()
        
    else:
        await callback.message.edit_text(
            "📅 *Введите дату окончания*\n\n"
            "Формат: ГГГГ-ММ-ДД\n"
            "Пример: 2026-12-31\n\n"
            "Или /cancel для отмены",
            parse_mode="Markdown"
        )
        await state.set_state(TrainerStates.waiting_for_recurring_end_date)
        await callback.answer()

@dp.message(TrainerStates.waiting_for_recurring_end_date)
async def process_recurring_end_date(message: types.Message, state: FSMContext):
    if message.from_user.id != TRAINER_ID:
        return
    
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Добавление отменено.")
        return
    
    end_date = message.text.strip()
    if not re.match(r"\d{4}-\d{2}-\d{2}", end_date):
        await message.answer("❌ Неверный формат. Используйте ГГГГ-ММ-ДД")
        return
    
    data = await state.get_data()
    user_id = data.get('recurring_user_id')
    weekday = data.get('recurring_weekday')
    time_str = data.get('recurring_time')
    start_date = datetime.now().strftime("%Y-%m-%d")
    
    await add_recurring_booking(user_id, weekday, time_str, start_date, end_date)
    user = await get_user(user_id)
    if user:
        days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
        
        await message.answer(
            f"✅ *Постоянная запись добавлена!*\n\n"
            f"👤 Клиент: {user['full_name']}\n"
            f"📅 День: {days[weekday]}\n"
            f"⏰ Время: {time_str}\n"
            f"📅 Действует до: {end_date}",
            parse_mode="Markdown"
        )
        
        try:
            await bot.send_message(
                user_id,
                f"🔄 *Вам добавлена постоянная запись!*\n\n"
                f"📅 День: {days[weekday]}\n"
                f"⏰ Время: {time_str}\n"
                f"📅 Действует до: {end_date}",
                parse_mode="Markdown"
            )
        except Exception as e:
            print(f"Не удалось уведомить пользователя: {e}")
    else:
        await message.answer("❌ Пользователь не найден!", parse_mode="Markdown")
    
    await state.clear()

@dp.callback_query(lambda c: c.data == "recurring_list")
async def recurring_list(callback: types.CallbackQuery):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    bookings = await get_recurring_bookings()
    if not bookings:
        await callback.message.edit_text("📭 Нет активных постоянных записей.")
        return
    
    await callback.message.edit_text(
        "🔄 *Список постоянных записей*\n\nНажмите на запись для управления:",
        parse_mode="Markdown",
        reply_markup=recurring_list_keyboard(bookings)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("recurring_select_"))
async def recurring_select_action(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    booking_id = int(callback.data.split("_")[2])
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT rb.*, u.full_name, u.user_id 
        FROM recurring_bookings rb
        JOIN users u ON rb.user_id = u.user_id
        WHERE rb.id = ?
    ''', (booking_id,))
    booking = cursor.fetchone()
    conn.close()
    
    if not booking:
        await callback.message.edit_text("❌ Запись не найдена.")
        await callback.answer()
        return
    
    days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    
    await state.update_data(
        selected_booking_id=booking_id,
        selected_booking_user_id=booking[1],
        selected_booking_weekday=booking[2],
        selected_booking_time=booking[3],
        selected_booking_full_name=booking[7]
    )
    
    await callback.message.edit_text(
        f"🔄 *Управление постоянной записью*\n\n"
        f"👤 Клиент: {booking[7]}\n"
        f"📅 День: {days[booking[2]]}\n"
        f"⏰ Время: {booking[3]}\n\n"
        f"Что хотите сделать?",
        parse_mode="Markdown",
        reply_markup=recurring_action_keyboard(booking_id)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("recurring_cancel_once_"))
async def recurring_cancel_once(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    booking_id = int(callback.data.split("_")[3])
    await state.update_data(cancel_booking_id=booking_id)
    
    await callback.message.edit_text(
        "❓ *Разовая отмена*\n\n"
        "Какую тренировку пропустить?\n\n"
        "Выберите вариант:",
        parse_mode="Markdown",
        reply_markup=recurring_skip_week_keyboard(booking_id)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("recurring_skip_next_week_"))
async def recurring_skip_next_week(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    booking_id = int(callback.data.split("_")[4])
    data = await state.get_data()
    
    next_monday, week_start, week_end = get_next_week_start()
    weekday = data.get('selected_booking_weekday')
    time_str = data.get('selected_booking_time')
    user_id = data.get('selected_booking_user_id')
    full_name = data.get('selected_booking_full_name')
    
    days_until_target = (weekday - next_monday.weekday()) % 7
    target_date = next_monday + timedelta(days=days_until_target)
    
    await add_temporary_cancellation(booking_id, target_date.strftime("%Y-%m-%d"))
    
    days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    
    await callback.message.edit_text(
        f"✅ *Разовая отмена добавлена!*\n\n"
        f"👤 Клиент: {full_name}\n"
        f"📅 Пропущенная тренировка: {days[weekday]} {time_str}\n"
        f"🗓️ Дата: {target_date.strftime('%d.%m.%Y')}",
        parse_mode="Markdown"
    )
    
    try:
        await bot.send_message(
            user_id,
            f"📅 *Изменение в постоянной записи*\n\n"
            f"Тренер отменил вашу тренировку на {target_date.strftime('%d.%m')} в {time_str}",
            parse_mode="Markdown"
        )
    except Exception as e:
        print(f"Не удалось уведомить пользователя: {e}")
    
    await state.clear()
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("recurring_skip_custom_"))
async def recurring_skip_custom_date(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    booking_id = int(callback.data.split("_")[3])
    await state.update_data(cancel_booking_id=booking_id)
    
    await callback.message.edit_text(
        "📅 *Введите дату для отмены*\n\n"
        "Формат: ГГГГ-ММ-ДД\n"
        "Пример: 2026-04-25\n\n"
        "Или /cancel для отмены",
        parse_mode="Markdown"
    )
    await state.set_state(TrainerStates.waiting_for_recurring_skip_date)
    await callback.answer()

@dp.message(TrainerStates.waiting_for_recurring_skip_date)
async def process_recurring_skip_date(message: types.Message, state: FSMContext):
    if message.from_user.id != TRAINER_ID:
        return
    
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено.")
        return
    
    date_str = message.text.strip()
    if not re.match(r"\d{4}-\d{2}-\d{2}", date_str):
        await message.answer("❌ Неверный формат. Используйте ГГГГ-ММ-ДД")
        return
    
    data = await state.get_data()
    booking_id = data.get('cancel_booking_id')
    weekday = data.get('selected_booking_weekday')
    time_str = data.get('selected_booking_time')
    user_id = data.get('selected_booking_user_id')
    full_name = data.get('selected_booking_full_name')
    
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    if date_obj.weekday() != weekday:
        days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
        await message.answer(
            f"❌ {date_str} - это {days[date_obj.weekday()]}, "
            f"а постоянная запись на {days[weekday]}.\n\n"
            f"Пожалуйста, выберите правильный день недели."
        )
        return
    
    await add_temporary_cancellation(booking_id, date_str)
    
    days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    
    await message.answer(
        f"✅ *Разовая отмена добавлена!*\n\n"
        f"👤 Клиент: {full_name}\n"
        f"📅 Отменена тренировка: {days[weekday]} {time_str}\n"
        f"🗓️ Дата: {date_obj.strftime('%d.%m.%Y')}",
        parse_mode="Markdown"
    )
    
    try:
        await bot.send_message(
            user_id,
            f"📅 *Изменение в постоянной записи*\n\n"
            f"Тренер отменил вашу тренировку на {date_obj.strftime('%d.%m.%Y')} в {time_str}",
            parse_mode="Markdown"
        )
    except Exception as e:
        print(f"Не удалось уведомить пользователя: {e}")
    
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("recurring_cancel_forever_"))
async def recurring_cancel_forever(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    booking_id = int(callback.data.split("_")[3])
    data = await state.get_data()
    
    user_id = data.get('selected_booking_user_id')
    full_name = data.get('selected_booking_full_name')
    weekday = data.get('selected_booking_weekday')
    time_str = data.get('selected_booking_time')
    
    await deactivate_recurring_booking(booking_id)
    
    days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    
    await callback.message.edit_text(
        f"❌ *Постоянная запись отменена насовсем!*\n\n"
        f"👤 Клиент: {full_name}\n"
        f"📅 День: {days[weekday]}\n"
        f"⏰ Время: {time_str}",
        parse_mode="Markdown"
    )
    
    try:
        await bot.send_message(
            user_id,
            f"❌ *Ваша постоянная запись отменена тренером!*\n\n"
            f"📅 День: {days[weekday]}\n"
            f"⏰ Время: {time_str}",
            parse_mode="Markdown"
        )
    except Exception as e:
        print(f"Не удалось уведомить пользователя: {e}")
    
    await state.clear()
    await callback.answer()

@dp.callback_query(lambda c: c.data == "recurring_back")
async def recurring_back(callback: types.CallbackQuery):
    await recurring_menu(callback)

@dp.callback_query(lambda c: c.data == "recurring_back_day")
async def recurring_back_day(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = data.get('recurring_user_id')
    if user_id:
        await callback.message.edit_text(
            "📅 *Выберите день недели*:",
            parse_mode="Markdown",
            reply_markup=recurring_weekday_keyboard()
        )
    else:
        await callback.message.edit_text("❌ Ошибка: сессия потеряна. Начните добавление заново.", parse_mode="Markdown")
        await recurring_add_user_select(callback)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "recurring_back_time")
async def recurring_back_time(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    weekday = data.get('recurring_weekday')
    user_id = data.get('recurring_user_id')
    
    if user_id and weekday is not None:
        await callback.message.edit_text(
            "⏰ *Выберите время*:",
            parse_mode="Markdown",
            reply_markup=recurring_time_keyboard()
        )
    elif user_id:
        await recurring_add_day_select(callback, state)
    else:
        await callback.message.edit_text("❌ Ошибка: сессия потеряна. Начните добавление заново.", parse_mode="Markdown")
        await recurring_add_user_select(callback)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "recurring_back_menu")
async def recurring_back_menu(callback: types.CallbackQuery):
    await recurring_menu(callback)

@dp.callback_query(lambda c: c.data.startswith("recurring_back_action_"))
async def recurring_back_to_action(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != TRAINER_ID:
        await callback.answer("Нет доступа")
        return
    
    booking_id = int(callback.data.split("_")[3])
    data = await state.get_data()
    
    days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    
    await callback.message.edit_text(
        f"🔄 *Управление постоянной записью*\n\n"
        f"👤 Клиент: {data.get('selected_booking_full_name')}\n"
        f"📅 День: {days[data.get('selected_booking_weekday')]}\n"
        f"⏰ Время: {data.get('selected_booking_time')}\n\n"
        f"Что хотите сделать?",
        parse_mode="Markdown",
        reply_markup=recurring_action_keyboard(booking_id)
    )
    await callback.answer()

# === ОТЛАДОЧНЫЕ КОМАНДЫ ===

@dp.message(Command("check_slots"))
async def check_slots(message: types.Message):
    if message.from_user.id != TRAINER_ID:
        return
    
    next_monday, week_start, week_end = get_next_week_start()
    free_slots = await get_free_slots(week_start)
    
    if free_slots:
        slots_by_day = {}
        for slot in free_slots:
            date = slot.split()[0]
            if date not in slots_by_day:
                slots_by_day[date] = []
            slots_by_day[date].append(slot)
        
        text = f"📋 *Слоты на неделю {week_start} - {week_end}:*\n\n"
        for date, slots in slots_by_day.items():
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            day_names = ['ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']
            day_name = day_names[date_obj.weekday()]
            text += f"📅 {day_name} {date}: {len(slots)} слотов\n"
        
        text += f"\n📊 Всего: {len(free_slots)} слотов"
        await message.answer(text, parse_mode="Markdown")
    else:
        await message.answer(
            f"❌ *Нет свободных слотов на неделю {week_start}!*\n\n"
            f"Откройте запись через панель тренера → 📅 Открыть запись",
            parse_mode="Markdown"
        )

@dp.message(Command("debug_slots"))
async def debug_slots(message: types.Message):
    if message.from_user.id != TRAINER_ID:
        return
    
    current_monday, current_start, current_end = get_current_week_start()
    current_slots = await get_all_slots_for_week(current_start)
    
    next_monday, next_start, next_end = get_next_week_start()
    next_slots = await get_all_slots_for_week(next_start)
    
    text = "📊 *ОТЛАДКА СЛОТОВ*\n\n"
    
    text += f"📅 *Текущая неделя:* {current_start} - {current_end}\n"
    text += f"📊 Слотов в БД: {len(current_slots)}\n"
    if current_slots:
        dates = set(s.split()[0] for s in current_slots)
        text += f"📅 Даты: {', '.join(sorted(dates))}\n"
    else:
        text += "❌ НЕТ СЛОТОВ!\n"
    
    text += f"\n📅 *Следующая неделя:* {next_start} - {next_end}\n"
    text += f"📊 Слотов в БД: {len(next_slots)}\n"
    if next_slots:
        dates = set(s.split()[0] for s in next_slots)
        text += f"📅 Даты: {', '.join(sorted(dates))}\n"
    else:
        text += "❌ НЕТ СЛОТОВ!\n"
    
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("check_db"))
async def check_db(message: types.Message):
    if message.from_user.id != TRAINER_ID:
        return
    
    try:
        conn = await get_connection()
        await conn.fetchval("SELECT 1")
        await conn.close()
        await message.answer(
            "✅ *База данных подключена успешно!*\n\n"
            f"📊 Тип БД: PostgreSQL (Supabase)\n"
            f"🔗 Статус: Работает",
            parse_mode="Markdown"
        )
    except Exception as e:
        await message.answer(
            f"❌ *Ошибка подключения к БД!*\n\n"
            f"```\n{str(e)}\n```",
            parse_mode="Markdown"
        )

# === ЗАПУСК БОТА ===

async def main():
    """Главная функция запуска бота в режиме Polling"""
    await init_db()
    await cleanup_old_data()
    
    print("=" * 50)
    print("🤖 БОТ ЗАПУЩЕН!")
    print(f"📅 Дата и время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    print(f"👨‍💼 Тренер ID: {TRAINER_ID}")
    print(f"💾 База данных: PostgreSQL (Supabase)")
    print(f"📡 Режим работы: Polling (long polling)")
    print("=" * 50)
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

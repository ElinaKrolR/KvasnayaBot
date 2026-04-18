from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

def main_menu_keyboard(is_trainer: bool = False):
    """Главное меню"""
    if is_trainer:
        buttons = [
            [KeyboardButton(text="📅 Мои тренировки"), KeyboardButton(text="✍️ Записаться")],
            [KeyboardButton(text="📦 Мой пакет"), KeyboardButton(text="📊 Моя статистика")],
            [KeyboardButton(text="📅 Расписание"), KeyboardButton(text="👨‍💼 Панель тренера")]
        ]
    else:
        buttons = [
            [KeyboardButton(text="📅 Мои тренировки"), KeyboardButton(text="✍️ Записаться")],
            [KeyboardButton(text="📦 Мой пакет"), KeyboardButton(text="📊 Моя статистика")],
            [KeyboardButton(text="📅 Расписание")]
        ]
    
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def booking_week_keyboard():
    """Клавиатура для выбора недели записи (текущая и следующая)"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Текущая неделя", callback_data="book_week_current")],
        [InlineKeyboardButton(text="📅 Следующая неделя", callback_data="book_week_next")],
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="cancel_booking")]
    ])
    return keyboard

def day_selection_keyboard(days: list, week_type: str):
    """Клавиатура для выбора дня с учетом блокировки прошедших дней"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for day in days:
        # Если день уже прошел - добавляем метку 🔒
        display_text = day['display']
        if day.get('is_passed', False):
            display_text = f"🔒 {display_text} (прошел)"
            callback_data = "day_passed"
        else:
            callback_data = f"select_day_{day['date']}_{week_type}"
        
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=display_text, callback_data=callback_data)
        ])
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад к неделям", callback_data="back_to_weeks")
    ])
    return keyboard

def time_slots_keyboard(slots: list, selected_date: str, week_type: str):
    """Клавиатура для выбора времени"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    row = []
    for i, slot in enumerate(slots):
        time_part = slot.split()[1] if ' ' in slot else slot
        row.append(InlineKeyboardButton(text=time_part, callback_data=f"time_{selected_date}_{slot}_{week_type}"))
        if len(row) == 2 or i == len(slots) - 1:
            keyboard.inline_keyboard.append(row)
            row = []
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад к дням", callback_data=f"back_to_days_{week_type}")
    ])
    return keyboard

def pending_requests_keyboard(requests: list):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    for req in requests:
        date_time = req['datetime']
        user_name = req['full_name']
        dt = date_time.split()
        display_time = f"{dt[0][5:]} {dt[1]}"
        
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"📝 {user_name} - {display_time}", callback_data=f"info_{req['id']}")
        ])
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text="✅", callback_data=f"approve_{req['id']}"),
            InlineKeyboardButton(text="❌", callback_data=f"reject_{req['id']}")
        ])
    
    return keyboard

def admin_panel_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Заявки", callback_data="admin_requests")],
        [InlineKeyboardButton(text="📅 Открыть запись", callback_data="admin_slots")],
        [InlineKeyboardButton(text="📅 Расписание", callback_data="admin_schedule")],
        [InlineKeyboardButton(text="🔄 Постоянные записи", callback_data="admin_recurring")],
        [InlineKeyboardButton(text="❌ Отменить тренировку", callback_data="admin_cancel")],
        [InlineKeyboardButton(text="🗓️ Массовая отмена", callback_data="admin_mass_cancel")],
        [InlineKeyboardButton(text="📊 Отчёт за неделю", callback_data="admin_report")],
        [InlineKeyboardButton(text="📌 Расписание на сегодня", callback_data="admin_today")],
        [InlineKeyboardButton(text="➕ Добавить пакет", callback_data="admin_add_package")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")]
    ])
    return keyboard

def schedule_week_keyboard():
    """Клавиатура для тренера (все недели)"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Текущая неделя", callback_data="week_current")],
        [InlineKeyboardButton(text="📅 Следующая неделя", callback_data="week_next")]
    ])
    return keyboard

def client_schedule_week_keyboard():
    """Клавиатура для клиента (текущая и следующая неделя)"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Текущая неделя (только просмотр)", callback_data="client_week_current")],
        [InlineKeyboardButton(text="📅 Следующая неделя (доступна запись)", callback_data="client_week_next")]
    ])
    return keyboard

def open_slots_weekday_keyboard():
    """Клавиатура для выбора дня недели при открытии записи"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Понедельник", callback_data="open_slots_mon")],
        [InlineKeyboardButton(text="Вторник", callback_data="open_slots_tue")],
        [InlineKeyboardButton(text="Среда", callback_data="open_slots_wed")],
        [InlineKeyboardButton(text="Четверг", callback_data="open_slots_thu")],
        [InlineKeyboardButton(text="Пятница", callback_data="open_slots_fri")],
        [InlineKeyboardButton(text="Суббота", callback_data="open_slots_sat")],
        [InlineKeyboardButton(text="Воскресенье", callback_data="open_slots_sun")],
        [InlineKeyboardButton(text="✅ Завершить и открыть запись", callback_data="open_slots_finish")]
    ])
    return keyboard

def open_slots_time_keyboard(day_name: str, day_date: str, busy_times: list, is_passed: bool = False):
    """Клавиатура для выбора времени занятых окон"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    # Добавляем заголовок
    if is_passed:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"🔒 {day_name} {day_date} (ДЕНЬ ПРОШЕЛ)", callback_data="ignore")
        ])
    else:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"📅 {day_name} {day_date}", callback_data="ignore")
        ])
    
    # Часы с 9 до 21
    row = []
    for hour in range(9, 22):
        time_key = f"{hour:02d}:00"
        if hour == 21:
            time_key = "21:00"
        
        if is_passed:
            button_text = f"🔒 {time_key}"
            callback = "ignore"
        elif time_key in busy_times:
            button_text = f"🔒 {time_key}"
            callback = f"toggle_time_{day_name}_{time_key}"
        else:
            button_text = f"⏰ {time_key}"
            callback = f"toggle_time_{day_name}_{time_key}"
        
        row.append(InlineKeyboardButton(text=button_text, callback_data=callback))
        
        if len(row) == 3:
            keyboard.inline_keyboard.append(row)
            row = []
    
    if row:
        keyboard.inline_keyboard.append(row)
    
    if not is_passed:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text="🔙 Назад к дням", callback_data="open_slots_back_days")
        ])
    
    return keyboard

def open_slots_confirm_keyboard(week_start: str):
    """Клавиатура подтверждения открытия записи"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, открыть запись", callback_data=f"confirm_open_{week_start}")],
        [InlineKeyboardButton(text="❌ Нет, вернуться к редактированию", callback_data="open_slots_back_days")],
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="back_admin")]
    ])
    return keyboard

def mass_cancel_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Отменить весь день", callback_data="mass_day")],
        [InlineKeyboardButton(text="⏰ Отменить интервал времени", callback_data="mass_time")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_admin")]
    ])
    return keyboard

def recurring_menu_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить постоянную запись", callback_data="recurring_add")],
        [InlineKeyboardButton(text="📋 Список постоянных записей", callback_data="recurring_list")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_admin")]
    ])
    return keyboard

def recurring_weekday_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Понедельник", callback_data="recurring_day_0")],
        [InlineKeyboardButton(text="Вторник", callback_data="recurring_day_1")],
        [InlineKeyboardButton(text="Среда", callback_data="recurring_day_2")],
        [InlineKeyboardButton(text="Четверг", callback_data="recurring_day_3")],
        [InlineKeyboardButton(text="Пятница", callback_data="recurring_day_4")],
        [InlineKeyboardButton(text="Суббота", callback_data="recurring_day_5")],
        [InlineKeyboardButton(text="Воскресенье", callback_data="recurring_day_6")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="recurring_back")]
    ])
    return keyboard

def recurring_time_keyboard(hours: list = None):
    if hours is None:
        hours = [f"{h:02d}:00" for h in range(9, 22)]
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    row = []
    for hour in hours:
        row.append(InlineKeyboardButton(text=hour, callback_data=f"recurring_time_{hour}"))
        if len(row) == 3:
            keyboard.inline_keyboard.append(row)
            row = []
    if row:
        keyboard.inline_keyboard.append(row)
    
    # Добавляем кнопку "Назад" с правильным callback
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="recurring_back_time")
    ])
    return keyboard

def recurring_list_keyboard(bookings: list):
    days = ['ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    for b in bookings:
        day_name = days[b['weekday']]
        client_name = b.get('full_name', f"Клиент ID:{b['user_id']}")
        end_date_text = f"до {b['end_date']}" if b.get('end_date') else "бессрочно"
        
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"🗓️ {day_name} {b['time']} - {client_name} ({end_date_text})", 
                callback_data=f"recurring_select_{b['id']}"
            )
        ])
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="recurring_back_menu")
    ])
    return keyboard

def recurring_action_keyboard(booking_id: int):
    """Клавиатура для действий с постоянной записью"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить (разово)", callback_data=f"recurring_cancel_once_{booking_id}")],
        [InlineKeyboardButton(text="⚠️ Отменить насовсем", callback_data=f"recurring_cancel_forever_{booking_id}")],
        [InlineKeyboardButton(text="🔙 Назад к списку", callback_data="recurring_list")]
    ])
    return keyboard
def recurring_skip_week_keyboard(booking_id: int):
    """Клавиатура для выбора - пропустить только следующую неделю или выбрать конкретную дату"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Только следующая неделя", callback_data=f"recurring_skip_next_week_{booking_id}")],
        [InlineKeyboardButton(text="📅 Выбрать конкретную дату", callback_data=f"recurring_skip_custom_{booking_id}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=f"recurring_back_action_{booking_id}")]
    ])
    return keyboard

def recurring_user_select_keyboard(users: list):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for user in users:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"{user['full_name']} (@{user['username']}) - пакет: {user['package_left']}", callback_data=f"recurring_user_{user['user_id']}")
        ])
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="recurring_back_menu")
    ])
    return keyboard

def recurring_time_keyboard_with_status(hours: list, weekday: int):
    """Клавиатура для выбора времени с отображением занятости"""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    row = []
    for hour in hours:
        if hour.startswith("🔒"):
            # Занятое время - кнопка неактивна (но мы все равно делаем ее, просто с другим callback)
            display_text = hour
            callback_data = f"time_busy_{weekday}_{hour.split()[1]}"
        else:
            display_text = hour
            callback_data = f"recurring_time_select_{weekday}_{hour}"
        
        row.append(InlineKeyboardButton(text=display_text, callback_data=callback_data))
        if len(row) == 3:
            keyboard.inline_keyboard.append(row)
            row = []
    if row:
        keyboard.inline_keyboard.append(row)
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад к дням", callback_data="recurring_back_day")
    ])
    return keyboard
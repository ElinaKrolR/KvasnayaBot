import os
BOT_TOKEN = os.environ.get("BOT_TOKEN")
TRAINER_ID = int(os.environ.get("TRAINER_ID", 1073737882))
# ... остальные настройки

# Рабочие часы тренера
WORK_HOURS_START =9     # 9:00
WORK_HOURS_END = 21     # 21:00

# Длительность тренировки в минутах
TRAINING_DURATION = 60

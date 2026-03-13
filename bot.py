import json
import logging
import sqlite3
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
import traceback
import random
import uuid
import os
import threading
import atexit
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import asyncio

DB_INITIALIZED = False

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Токен бота
TOKEN = '8685226609:AAEA8vHoWELRP7_KwXfV9Qbpq1usLUfdJ6o'

# ID администратора
ADMIN_ID = 860845946
ADMIN2_ID = 745565559 
ADMINS = [ADMIN_ID, ADMIN2_ID]  

# Функция проверки админа
def is_admin(user_id):
    return user_id in ADMINS

# URL для калькулятора дохода
CALCULATOR_URL = "https://eda.yandex.ru/partner/perf/samara/?utm_medium=cpc&utm_source=yandex-hr&utm_campaign=%5BEDA%5DMX_Courier_RU-ALL-1M_Brand_search_NU%7C73792274&utm_term=49415175552%7C---autotargeting&utm_content=k50id%7C0100000049415175552_49415175552%7Ccid%7C73792274%7Cgid%7C5378729251%7Caid%7C15662855932%7Cadp%7Cno%7Cpos%7Cpremium1%7Csrc%7Csearch_none%7Cdvc%7Cdesktop%7Cmain&etext=2202.H1-umiWOxa1IhaqocPaUS69zT9wHAZdkgZEGqorPY5rJ_ebzkat1FDn2yZO3bEqDYssRPcp0IyJXzD9sTJXJ7293dG14ZXB1Z2VrdW1hemM.0d27564e0c3a01c61971ab0f3d5b481a3ae88ee1&yclid=14506292526793097215"

# ========== ИЗОБРАЖЕНИЯ ==========
IMAGES = {
    'test_required': 'https://i.ibb.co/yFQr7VHx/photo-2026-03-03-19-24-40-1.jpg',
    'main_menu': 'https://i.ibb.co/5hQCccsB/photo-2026-03-03-19-24-49-1.jpg'
}

# ========== ПОСТОЯННОЕ ХРАНЕНИЕ ДАННЫХ ==========
DB_PATH = 'bot_database.db'
BACKUP_FILE = 'backup.json'

def get_db():
    """Возвращает соединение с БД"""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

def init_database():
    """Инициализирует таблицы в БД"""
    global DB_INITIALIZED
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute("PRAGMA foreign_keys = ON")
        
        # Таблица пользователей
        c.execute('''CREATE TABLE IF NOT EXISTS users
                     (user_id INTEGER PRIMARY KEY,
                      username TEXT,
                      first_name TEXT,
                      last_name TEXT,
                      registration_date TEXT,
                      balance REAL DEFAULT 0,
                      test_passed INTEGER DEFAULT 0,
                      test_attempts INTEGER DEFAULT 0,
                      last_test_attempt TEXT)''')
        
        # Таблица заявок на вывод
        c.execute('''CREATE TABLE IF NOT EXISTS withdrawals
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER,
                      amount REAL,
                      payment_method TEXT,
                      payment_details TEXT,
                      status TEXT DEFAULT 'pending',
                      request_date TEXT,
                      completed_date TEXT,
                      reject_reason TEXT,
                      FOREIGN KEY (user_id) REFERENCES users(user_id))''')
        
        # Таблица тикетов поддержки
        c.execute('''CREATE TABLE IF NOT EXISTS support_tickets
                     (ticket_id TEXT PRIMARY KEY,
                      user_id INTEGER,
                      username TEXT,
                      first_name TEXT,
                      message TEXT,
                      status TEXT DEFAULT 'open',
                      created_at TEXT,
                      answered_at TEXT,
                      admin_reply TEXT,
                      FOREIGN KEY (user_id) REFERENCES users(user_id))''')
        
        # Таблица курьеров
        c.execute('''CREATE TABLE IF NOT EXISTS couriers
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      recruiter_id INTEGER,
                      full_name TEXT,
                      city TEXT,
                      status TEXT DEFAULT 'pending',
                      balance REAL DEFAULT 0,
                      registered_at TEXT,
                      confirmed_at TEXT,
                      sheet_row INTEGER,
                      orders_completed INTEGER DEFAULT 0,
                      reject_reason TEXT,
                      invited_at TEXT,
                      FOREIGN KEY (recruiter_id) REFERENCES users(user_id))''')
        
        conn.commit()
        
        # Добавляем новые поля
        try:
            c.execute("SELECT orders_completed FROM couriers LIMIT 1")
        except sqlite3.OperationalError:
            c.execute("ALTER TABLE couriers ADD COLUMN orders_completed INTEGER DEFAULT 0")
            conn.commit()
            logger.info("✅ Добавлено поле orders_completed")
        
        try:
            c.execute("SELECT reject_reason FROM couriers LIMIT 1")
        except sqlite3.OperationalError:
            c.execute("ALTER TABLE couriers ADD COLUMN reject_reason TEXT")
            conn.commit()
            logger.info("✅ Добавлено поле reject_reason")
        
        try:
            c.execute("SELECT invited_at FROM couriers LIMIT 1")
        except sqlite3.OperationalError:
            c.execute("ALTER TABLE couriers ADD COLUMN invited_at TEXT")
            conn.commit()
            logger.info("✅ Добавлено поле invited_at")
        
        logger.info("✅ Таблицы созданы/проверены")
        
        # Загружаем данные из Google Sheets
        try:
            load_from_google_sheets()
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки из Google Sheets: {e}")
        
        # Пересчитываем балансы
        try:
            logger.info("🔄 Пересчитываем балансы...")
            recalc_all_balances()
        except Exception as e:
            logger.error(f"❌ Ошибка при пересчете балансов: {e}")
        
        DB_INITIALIZED = True
        logger.info("✅ База данных инициализирована")
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        logger.error(traceback.format_exc())
        return False

# ========== ФУНКЦИИ ДЛЯ ПЕРЕСЧЕТА БАЛАНСОВ ==========
def recalc_all_balances():
    """Пересчитывает балансы всех пользователей"""
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute("SELECT user_id FROM users")
        users = c.fetchall()
        
        count = 0
        for user in users:
            user_id = user[0]
            
            c.execute("SELECT SUM(balance) FROM couriers WHERE recruiter_id = ?", (user_id,))
            couriers_sum = c.fetchone()[0] or 0
            
            c.execute("SELECT SUM(amount) FROM withdrawals WHERE user_id = ? AND status = 'completed'", (user_id,))
            withdrawals_sum = c.fetchone()[0] or 0
            
            real_balance = couriers_sum - withdrawals_sum
            
            c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            current = c.fetchone()
            current_balance = current[0] if current else 0
            
            if current_balance != real_balance:
                c.execute("UPDATE users SET balance = ? WHERE user_id = ?", (real_balance, user_id))
                logger.info(f"💰 Пользователь {user_id}: {current_balance} -> {real_balance}")
                count += 1
        
        conn.commit()
        logger.info(f"✅ Пересчитаны балансы для {count} пользователей")
        return count
        
    except Exception as e:
        logger.error(f"❌ Ошибка при пересчете балансов: {e}")
        return 0

def get_user_balance(user_id):
    """Получает реальный баланс пользователя"""
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute("SELECT SUM(balance) FROM couriers WHERE recruiter_id = ?", (user_id,))
        couriers_sum = c.fetchone()[0] or 0
        
        c.execute("SELECT SUM(amount) FROM withdrawals WHERE user_id = ? AND status = 'completed'", (user_id,))
        withdrawals_sum = c.fetchone()[0] or 0
        
        real_balance = couriers_sum - withdrawals_sum
        
        c.execute("UPDATE users SET balance = ? WHERE user_id = ?", (real_balance, user_id))
        conn.commit()
        
        return real_balance
        
    except Exception as e:
        logger.error(f"Ошибка в get_user_balance: {e}")
        return 0

# ========== ФУНКЦИИ ПРОВЕРКИ ПОЛЬЗОВАТЕЛЕЙ ==========
def is_registered(user_id):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        return result is not None
    except Exception as e:
        logger.error(f"Ошибка в is_registered: {e}")
        return False

def register_user(user_id, username, first_name, last_name):
    try:
        conn = get_db()
        c = conn.cursor()
        registration_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""INSERT OR IGNORE INTO users 
                     (user_id, username, first_name, last_name, registration_date, balance, test_passed) 
                     VALUES (?, ?, ?, ?, ?, 0, 0)""",
                  (user_id, username, first_name, last_name, registration_date))
        conn.commit()
        logger.info(f"✅ Пользователь {user_id} зарегистрирован")
    except Exception as e:
        logger.error(f"Ошибка в register_user: {e}")

def update_test_status(user_id, passed):
    """Обновляет статус теста пользователя"""
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not c.fetchone():
            logger.error(f"❌ Пользователь {user_id} не найден")
            return False
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""UPDATE users 
                     SET test_passed = ?, last_test_attempt = ? 
                     WHERE user_id = ?""",
                  (1 if passed else 0, current_time, user_id))
        conn.commit()
        
        c.execute("SELECT test_passed FROM users WHERE user_id = ?", (user_id,))
        new_value = c.fetchone()[0]
        
        logger.info(f"✅ Обновлен test_passed для {user_id} на {new_value}")
        return new_value == 1
        
    except Exception as e:
        logger.error(f"Ошибка в update_test_status: {e}")
        return False

def can_take_test(user_id):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT test_passed, last_test_attempt FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        
        if not result:
            return True, 0
        
        test_passed, last_attempt_str = result
        
        if test_passed == 1:
            return True, 0
        
        if last_attempt_str:
            last_attempt = datetime.strptime(last_attempt_str, "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            time_diff = now - last_attempt
            
            if time_diff < timedelta(minutes=5):
                remaining = 5 - int(time_diff.total_seconds() / 60)
                return False, remaining
        
        return True, 0
    except Exception as e:
        logger.error(f"Ошибка в can_take_test: {e}")
        return True, 0

# ========== ФУНКЦИИ ДЛЯ ВЫВОДА ==========
def create_withdrawal_request(user_id, amount, method, details):
    conn = get_db()
    c = conn.cursor()
    try:
        real_balance = get_user_balance(user_id)
        
        if amount > real_balance:
            logger.error(f"❌ Недостаточно средств: баланс {real_balance}, запрос {amount}")
            return None
        
        request_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute('''INSERT INTO withdrawals 
                     (user_id, amount, payment_method, payment_details, request_date, status) 
                     VALUES (?, ?, ?, ?, ?, 'pending')''',
                  (user_id, amount, method, details, request_date))
        request_id = c.lastrowid
        conn.commit()
        
        logger.info(f"💰 Заявка {request_id} создана")
        
        c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (user_id,))
        user = c.fetchone()
        username = user[0] if user else None
        first_name = user[1] if user else "Пользователь"
        
        def add_to_sheet_thread():
            try:
                add_withdrawal_to_sheet(user_id, username, first_name, amount, method, details, request_id)
            except Exception as e:
                logger.error(f"Ошибка в потоке добавления в Google Sheets: {e}")
        
        thread = threading.Thread(target=add_to_sheet_thread)
        thread.start()
        
        return request_id
    except Exception as e:
        logger.error(f"Ошибка в create_withdrawal_request: {e}")
        return None

def get_pending_withdrawals():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''SELECT w.id, w.user_id, u.first_name, u.username, 
                            w.amount, w.payment_method, w.payment_details, w.request_date
                     FROM withdrawals w
                     JOIN users u ON w.user_id = u.user_id
                     WHERE w.status = 'pending'
                     ORDER BY w.request_date ASC''')
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка в get_pending_withdrawals: {e}")
        return []

def get_all_withdrawals(limit=50):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''SELECT w.id, w.user_id, u.first_name, u.username, 
                            w.amount, w.payment_method, w.payment_details, w.status, w.request_date, w.completed_date, w.reject_reason
                     FROM withdrawals w
                     JOIN users u ON w.user_id = u.user_id
                     ORDER BY w.request_date DESC
                     LIMIT ?''', (limit,))
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка в get_all_withdrawals: {e}")
        return []

def get_withdrawal_by_id(request_id):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''SELECT w.id, w.user_id, u.first_name, u.username, 
                            w.amount, w.payment_method, w.payment_details, w.status, w.request_date
                     FROM withdrawals w
                     JOIN users u ON w.user_id = u.user_id
                     WHERE w.id = ?''', (request_id,))
        return c.fetchone()
    except Exception as e:
        logger.error(f"Ошибка в get_withdrawal_by_id: {e}")
        return None

async def confirm_withdrawal(request_id, context):
    """Подтверждает вывод средств"""
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute('''SELECT user_id, amount FROM withdrawals WHERE id = ? AND status = 'pending' ''', (request_id,))
        withdrawal = c.fetchone()
        
        if not withdrawal:
            logger.error(f"❌ Заявка {request_id} не найдена")
            return False, None, None
        
        user_id, amount = withdrawal
        
        completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE withdrawals SET status = 'completed', completed_date = ? WHERE id = ?", 
                  (completed_date, request_id))
        
        conn.commit()
        
        new_balance = get_user_balance(user_id)
        
        logger.info(f"✅ Подтвержден вывод {amount} для пользователя {user_id}")
        
        def update_sheet_thread():
            try:
                update_withdrawal_status_in_sheet(request_id, user_id, amount, "✅ Подтвержден", 
                                                 datetime.now().strftime("%d.%m.%Y %H:%M"))
            except Exception as e:
                logger.error(f"Ошибка обновления статуса в Google Sheets: {e}")
        
        thread = threading.Thread(target=update_sheet_thread)
        thread.start()
        
        return True, user_id, amount
        
    except Exception as e:
        logger.error(f"Ошибка в confirm_withdrawal: {e}")
        return False, None, None

async def reject_withdrawal(request_id, reason, context):
    """Отклоняет вывод средств"""
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute('''SELECT user_id, amount FROM withdrawals WHERE id = ? AND status = 'pending' ''', (request_id,))
        withdrawal = c.fetchone()
        
        if not withdrawal:
            logger.error(f"❌ Заявка {request_id} не найдена")
            return False, None, None
        
        user_id, amount = withdrawal
        
        completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE withdrawals SET status = 'rejected', completed_date = ?, reject_reason = ? WHERE id = ?", 
                  (completed_date, reason, request_id))
        
        conn.commit()
        
        current_balance = get_user_balance(user_id)
        
        logger.info(f"❌ Отклонен вывод {amount} для пользователя {user_id}. Причина: {reason}")
        
        def update_sheet_thread():
            try:
                update_withdrawal_status_in_sheet(request_id, user_id, amount, "❌ Отклонен", 
                                                 datetime.now().strftime("%d.%m.%Y %H:%M"))
            except Exception as e:
                logger.error(f"Ошибка обновления статуса в Google Sheets: {e}")
        
        thread = threading.Thread(target=update_sheet_thread)
        thread.start()
        
        return True, user_id, amount
        
    except Exception as e:
        logger.error(f"Ошибка в reject_withdrawal: {e}")
        return False, None, None

# ========== ФУНКЦИИ ПОДДЕРЖКИ ==========
def create_support_ticket(user_id, username, first_name, message):
    conn = get_db()
    c = conn.cursor()
    try:
        ticket_id = str(uuid.uuid4())[:8]
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute('''INSERT INTO support_tickets 
                     (ticket_id, user_id, username, first_name, message, created_at) 
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (ticket_id, user_id, username, first_name, message, created_at))
        conn.commit()
        return ticket_id
    except Exception as e:
        logger.error(f"Ошибка в create_support_ticket: {e}")
        return None

def get_open_tickets():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''SELECT ticket_id, user_id, username, first_name, message, created_at 
                     FROM support_tickets 
                     WHERE status = 'open' 
                     ORDER BY created_at ASC''')
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка в get_open_tickets: {e}")
        return []

def get_all_tickets(limit=50):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''SELECT ticket_id, user_id, username, first_name, message, status, created_at, answered_at, admin_reply
                     FROM support_tickets 
                     ORDER BY created_at DESC
                     LIMIT ?''', (limit,))
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка в get_all_tickets: {e}")
        return []

def get_ticket(ticket_id):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''SELECT * FROM support_tickets WHERE ticket_id = ?''', (ticket_id,))
        return c.fetchone()
    except Exception as e:
        logger.error(f"Ошибка в get_ticket: {e}")
        return None

def is_ticket_open(ticket_id):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT status FROM support_tickets WHERE ticket_id = ?", (ticket_id,))
        result = c.fetchone()
        return result and result[0] == 'open'
    except Exception as e:
        logger.error(f"Ошибка в is_ticket_open: {e}")
        return False

def close_ticket(ticket_id, admin_reply):
    conn = get_db()
    c = conn.cursor()
    try:
        answered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute('''UPDATE support_tickets 
                     SET status = 'closed', answered_at = ?, admin_reply = ? 
                     WHERE ticket_id = ?''',
                  (answered_at, admin_reply, ticket_id))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Ошибка в close_ticket: {e}")
        return False

# ========== ФУНКЦИИ ДЛЯ КУРЬЕРОВ ==========
def add_courier(recruiter_id, recruiter_username, recruiter_name, full_name, city):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM users WHERE user_id = ?", (recruiter_id,))
        user = c.fetchone()
        if not user:
            registration_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute("""
                INSERT INTO users 
                (user_id, username, first_name, last_name, registration_date, balance, test_passed) 
                VALUES (?, ?, ?, ?, ?, 0, 0)
            """, (recruiter_id, recruiter_username, recruiter_name, "", registration_date))
            conn.commit()
            logger.info(f"✅ Пользователь {recruiter_id} создан")
        
        c.execute('''SELECT id FROM couriers 
                     WHERE recruiter_id = ? AND full_name = ? AND city = ? AND status = 'confirmed' ''',
                  (recruiter_id, full_name, city))
        exists = c.fetchone()
        
        if exists:
            logger.info(f"⚠️ Курьер уже существует с id={exists[0]}")
            return False, "Курьер с такими данными уже подтвержден"
        
        registered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute('''INSERT INTO couriers 
                     (recruiter_id, full_name, city, status, registered_at)
                     VALUES (?, ?, ?, ?, ?)''',
                  (recruiter_id, full_name, city, 'pending', registered_at))
        conn.commit()
        
        courier_id = c.lastrowid
        logger.info(f"✅ Курьер добавлен с ID: {courier_id}")
        
        get_user_balance(recruiter_id)
        
        def add_to_sheet_thread():
            try:
                success, row_number = add_courier_to_google_sheet(
                    recruiter_name, recruiter_username, full_name, city
                )
                if success and row_number:
                    conn2 = get_db()
                    c2 = conn2.cursor()
                    c2.execute('''UPDATE couriers SET sheet_row = ? WHERE id = ?''', (row_number, courier_id))
                    conn2.commit()
                    conn2.close()
                    logger.info(f"✅ Курьер {full_name} добавлен в Google Sheets, строка: {row_number}")
            except Exception as e:
                logger.error(f"Ошибка в потоке добавления в Google Sheets: {e}")
        
        thread = threading.Thread(target=add_to_sheet_thread)
        thread.start()
        
        return True, "Заявка на курьера отправлена на проверку! ✅"
        
    except Exception as e:
        logger.error(f"❌ Ошибка в add_courier: {e}")
        traceback.print_exc()
        return False, f"Ошибка: {str(e)}"

def get_recruiter_couriers(recruiter_id):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''SELECT full_name, city, status, registered_at, confirmed_at, balance, orders_completed 
                     FROM couriers 
                     WHERE recruiter_id = ? 
                     ORDER BY registered_at DESC''', (recruiter_id,))
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка в get_recruiter_couriers: {e}")
        return []

# ========== GOOGLE SHEETS ИНТЕГРАЦИЯ ==========
def get_google_sheet():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 
                 'https://www.googleapis.com/auth/drive',
                 'https://www.googleapis.com/auth/spreadsheets']
        
        creds_json = os.environ.get('GOOGLE_CREDS_JSON')
        
        if creds_json:
            creds_dict = json.loads(creds_json)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        
        client = gspread.authorize(creds)
        
        sheet_id = os.environ.get('GOOGLE_SHEET_ID', '')
        if not sheet_id:
            logger.error("GOOGLE_SHEET_ID не задан")
            return None
            
        sheet = client.open_by_key(sheet_id).sheet1
        logger.info("✅ Подключение к Google Sheets установлено")
        return sheet
    except Exception as e:
        logger.error(f"Ошибка подключения к Google Sheets: {e}")
        return None

def get_withdrawals_sheet():
    try:
        sheet = get_google_sheet()
        if not sheet:
            return None
        
        spreadsheet = sheet.spreadsheet
        
        try:
            withdrawals_sheet = spreadsheet.worksheet("Выводы")
            logger.info("✅ Подключение к листу 'Выводы' установлено")
            return withdrawals_sheet
        except gspread.WorksheetNotFound:
            logger.info("📝 Лист 'Выводы' не найден, создаем новый...")
            withdrawals_sheet = spreadsheet.add_worksheet(title="Выводы", rows=1000, cols=9)
            headers = ["Дата", "User ID", "Username", "Имя", "Сумма", "Способ", "Реквизиты", "Статус", "Дата подтверждения"]
            withdrawals_sheet.append_row(headers, value_input_option='USER_ENTERED')
            logger.info("✅ Лист 'Выводы' создан")
            return withdrawals_sheet
            
    except Exception as e:
        logger.error(f"Ошибка подключения к листу 'Выводы': {e}")
        return None

def add_withdrawal_to_sheet(user_id, username, first_name, amount, method, details, request_id):
    try:
        withdrawals_sheet = get_withdrawals_sheet()
        if not withdrawals_sheet:
            return False
        
        row = [
            datetime.now().strftime("%d.%m.%Y %H:%M"),
            str(user_id),
            f"@{username}" if username else "-",
            first_name,
            amount,
            method,
            details,
            "⏳ Ожидает",
            "-"
        ]
        
        withdrawals_sheet.append_row(row, value_input_option='USER_ENTERED')
        logger.info(f"✅ Заявка #{request_id} добавлена в Google Sheets")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка добавления заявки в Google Sheets: {e}")
        return False

def update_withdrawal_status_in_sheet(request_id, user_id, amount, status_text, completed_date=None):
    try:
        withdrawals_sheet = get_withdrawals_sheet()
        if not withdrawals_sheet:
            return False
        
        all_values = withdrawals_sheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            logger.warning("⚠️ В таблице нет данных")
            return False
        
        headers = all_values[0]
        
        user_id_col = None
        amount_col = None
        status_col = None
        date_col = None
        
        for i, header in enumerate(headers):
            if header == "User ID":
                user_id_col = i + 1
            elif header == "Сумма":
                amount_col = i + 1
            elif header == "Статус":
                status_col = i + 1
            elif header == "Дата подтверждения":
                date_col = i + 1
        
        if not user_id_col or not amount_col or not status_col:
            logger.error(f"❌ Не найдены нужные столбцы")
            return False
        
        found_row = None
        for row_idx, row in enumerate(all_values[1:], start=2):
            if len(row) < max(user_id_col, amount_col, status_col):
                continue
                
            row_user_id = str(row[user_id_col - 1]).strip() if user_id_col - 1 < len(row) else ""
            row_amount = str(row[amount_col - 1]).strip() if amount_col - 1 < len(row) else ""
            row_status = str(row[status_col - 1]).strip() if status_col - 1 < len(row) else ""
            
            row_user_id_clean = row_user_id.replace(' ', '').replace('@', '').replace('user_', '')
            str_user_id = str(user_id).strip()
            
            row_amount_clean = row_amount.replace(' ', '').replace(',', '.').replace('руб', '').strip()
            str_amount = str(amount).strip()
            
            if (row_user_id_clean == str_user_id or row_user_id == str_user_id) and \
               (row_amount_clean == str_amount or row_amount == str_amount) and \
               (row_status == "⏳ Ожидает" or "ожидает" in row_status.lower()):
                
                found_row = row_idx
                break
        
        if not found_row:
            logger.warning(f"⚠️ Точное совпадение не найдено")
            return False
        
        withdrawals_sheet.update_cell(found_row, status_col, status_text)
        
        if date_col and completed_date:
            withdrawals_sheet.update_cell(found_row, date_col, completed_date)
        
        logger.info(f"✅ Статус заявки #{request_id} обновлен в Google Sheets")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка обновления статуса в Google Sheets: {e}")
        return False

def add_courier_to_google_sheet(recruiter_name, recruiter_username, full_name, city):
    try:
        sheet = get_google_sheet()
        if not sheet:
            return None, None
        
        row = [
            datetime.now().strftime("%d.%m.%Y %H:%M"),
            recruiter_name,
            f"@{recruiter_username}" if recruiter_username else "-",
            full_name,
            city,
            "⏳ Ожидает",
            0,
            0,
            0,
            0,
            "",
            ""
        ]
        
        sheet.append_row(row, value_input_option='USER_ENTERED')
        
        time.sleep(2)
        all_records = sheet.get_all_records()
        row_number = len(all_records) + 1
        
        logger.info(f"✅ Курьер {full_name} добавлен в Google Sheets (строка {row_number})")
        return True, row_number
    except Exception as e:
        logger.error(f"Ошибка добавления в Google Sheets: {e}")
        return None, None

def notify_recruiter_about_status_change(recruiter_id, full_name, city, new_status, reject_reason=""):
    try:
        def send_notification():
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute("SELECT user_id FROM users WHERE user_id = ?", (recruiter_id,))
                if not c.fetchone():
                    return
                
                if new_status == 'confirmed':
                    message = (
                        f"✅ *Курьер подтвержден!*\n\n"
                        f"👤 *Имя:* {full_name}\n"
                        f"🏙 *Город:* {city}\n\n"
                        f"Курьер успешно прошел проверку!"
                    )
                elif new_status == 'rejected':
                    message = (
                        f"❌ *Курьер отклонен*\n\n"
                        f"👤 *Имя:* {full_name}\n"
                        f"🏙 *Город:* {city}\n"
                    )
                    if reject_reason:
                        message += f"📝 *Причина:* {reject_reason}\n\n"
                    message += f"Попробуйте записать другого кандидата."
                elif new_status == 'invited':
                    message = (
                        f"🏢 *Курьер приглашен в хаб!*\n\n"
                        f"👤 *Имя:* {full_name}\n"
                        f"🏙 *Город:* {city}\n\n"
                        f"Курьер приглашен для прохождения обучения."
                    )
                else:
                    return
                
                import asyncio
                from telegram import Bot
                
                bot = Bot(token=TOKEN)
                asyncio.run(bot.send_message(
                    chat_id=recruiter_id,
                    text=message,
                    parse_mode='Markdown'
                ))
                
                logger.info(f"📨 Уведомление о статусе '{new_status}' отправлено рекрутеру {recruiter_id}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка при отправке уведомления: {e}")
        
        thread = threading.Thread(target=send_notification)
        thread.start()
        
    except Exception as e:
        logger.error(f"❌ Ошибка в notify_recruiter_about_status_change: {e}")

def update_courier_status_in_sheet(sheet_row, status_text):
    try:
        sheet = get_google_sheet()
        if not sheet:
            return False
        
        sheet.update_cell(sheet_row, 6, status_text)
        logger.info(f"✅ Обновлен статус в Google Sheets: строка {sheet_row}")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка обновления статуса в Google Sheets: {e}")
        return False

def sync_deleted_couriers():
    try:
        sheet = get_google_sheet()
        if not sheet:
            return
        
        records = sheet.get_all_records()
        sheet_couriers = set()
        for record in records:
            full_name = record.get('ФИО клиента', '').strip()
            city = record.get('Город', '').strip()
            if full_name and city:
                sheet_couriers.add((full_name, city))
        
        logger.info(f"📊 В Google Sheets найдено {len(sheet_couriers)} курьеров")
        
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT id, full_name, city FROM couriers")
        db_couriers = c.fetchall()
        
        deleted_count = 0
        for courier in db_couriers:
            courier_id, full_name, city = courier
            if (full_name, city) not in sheet_couriers:
                c.execute("DELETE FROM couriers WHERE id = ?", (courier_id,))
                deleted_count += 1
                logger.info(f"🗑️ Удален курьер из БД: {full_name} ({city})")
        
        if deleted_count > 0:
            conn.commit()
            recalc_all_balances()
        
    except Exception as e:
        logger.error(f"❌ Ошибка при синхронизации удалений: {e}")

def check_pending_couriers():
    try:
        sheet = get_google_sheet()
        if not sheet:
            return
        
        records = sheet.get_all_records()
        logger.info(f"🔍 Проверяем {len(records)} записей в Google Sheets")
        
        conn = get_db()
        c = conn.cursor()
        
        updated_count = 0
        new_count = 0
        balance_updated_count = 0
        orders_updated_count = 0
        deleted_count = 0
        
        existing_couriers = set()
        
        for i, record in enumerate(records):
            try:
                full_name = record.get('ФИО клиента', '').strip()
                city = record.get('Город', '').strip()
                status_text = record.get('СТАТУС', '').strip()
                
                if not full_name or not city:
                    continue
                
                existing_couriers.add((full_name, city))
                
                recruiter_username = record.get('Username рекрутера', '').replace('@', '').strip()
                
                balance_raw = record.get('Баланс', '0')
                accepted_raw = record.get('ПРИНЯТО', '0')
                rejected_raw = record.get('ОТКЛОНЕНО', '0')
                invited_raw = record.get('Приглашен в хаб', '0')
                orders_raw = record.get('Выполнено заказов', '0')
                reject_reason_raw = record.get('Причина отказа', '')
                
                accepted = str(accepted_raw).strip()
                rejected = str(rejected_raw).strip()
                invited = str(invited_raw).strip()
                
                try:
                    balance_str = str(balance_raw).strip().replace(' ', '').replace(',', '.')
                    balance = float(balance_str) if balance_str and balance_str != '0' else 0.0
                except:
                    balance = 0.0
                
                try:
                    orders_str = str(orders_raw).strip().replace(' ', '')
                    orders_completed = int(float(orders_str)) if orders_str and orders_str != '0' else 0
                except:
                    orders_completed = 0
                
                reject_reason = str(reject_reason_raw).strip() if reject_reason_raw else ""
                
                sheet_row = i + 2
                
                if invited in ['1', 'true', 'True', 'TRUE', 'yes', 'Yes', 'YES', '✅', '☑']:
                    new_status = 'invited'
                    status_text_map = "🏢 Приглашен в хаб"
                elif accepted in ['1', 'true', 'True', 'TRUE', 'yes', 'Yes', 'YES', '✅', '☑']:
                    new_status = 'confirmed'
                    status_text_map = "✅ Подтвержден"
                elif rejected in ['1', 'true', 'True', 'TRUE', 'yes', 'Yes', 'YES', '❌']:
                    new_status = 'rejected'
                    status_text_map = "❌ Отклонен"
                else:
                    if 'Подтвержден' in status_text or '✅' in status_text:
                        new_status = 'confirmed'
                        status_text_map = "✅ Подтвержден"
                    elif 'Отклонен' in status_text or '❌' in status_text:
                        new_status = 'rejected'
                        status_text_map = "❌ Отклонен"
                    elif 'Приглашен' in status_text or '🏢' in status_text or 'хаб' in status_text.lower():
                        new_status = 'invited'
                        status_text_map = "🏢 Приглашен в хаб"
                    else:
                        new_status = 'pending'
                        status_text_map = "⏳ Ожидает"
                
                recruiter_id = None
                if recruiter_username:
                    c.execute("SELECT user_id FROM users WHERE username = ?", (recruiter_username,))
                    user = c.fetchone()
                    if user:
                        recruiter_id = user[0]
                
                if not recruiter_id:
                    for admin_id in ADMINS:
                        if recruiter_username in ['unknownsorcerer', 'costa'] or str(admin_id) in recruiter_username:
                            recruiter_id = admin_id
                            break
                
                if not recruiter_id:
                    logger.warning(f"⚠️ Рекрутер не найден для курьера {full_name}, строка пропущена")
                    continue
                
                c.execute("SELECT user_id FROM users WHERE user_id = ?", (recruiter_id,))
                if not c.fetchone():
                    logger.info(f"👤 Создаем пользователя-рекрутера с ID {recruiter_id}")
                    registration_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    c.execute("""
                        INSERT INTO users 
                        (user_id, username, first_name, last_name, registration_date, balance, test_passed) 
                        VALUES (?, ?, ?, ?, ?, 0, 0)
                    """, (recruiter_id, recruiter_username or f"user_{recruiter_id}", "Рекрутер", "", registration_date))
                    conn.commit()
                
                c.execute('''SELECT id, status, sheet_row, balance, orders_completed, reject_reason, invited_at 
                           FROM couriers WHERE full_name = ? AND city = ? ORDER BY id DESC LIMIT 1''',
                          (full_name, city))
                existing = c.fetchone()
                
                if existing:
                    (existing_id, existing_status, existing_sheet_row, 
                     existing_balance, existing_orders, existing_reject_reason, existing_invited_at) = existing
                    
                    if existing_status != new_status:
                        confirmed_at = None
                        invited_at = None
                        
                        if new_status == 'confirmed':
                            confirmed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        elif new_status == 'invited':
                            invited_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        c.execute('''UPDATE couriers 
                                     SET status = ?, confirmed_at = ?, invited_at = ? 
                                     WHERE id = ?''',
                                  (new_status, confirmed_at, invited_at, existing_id))
                        updated_count += 1
                        logger.info(f"🔄 Статус изменен: {full_name} -> {new_status}")
                        
                        notify_recruiter_about_status_change(recruiter_id, full_name, city, new_status, reject_reason)
                        
                        if existing_sheet_row:
                            update_courier_status_in_sheet(existing_sheet_row, status_text_map)
                    
                    if existing_balance != balance:
                        c.execute("UPDATE couriers SET balance = ? WHERE id = ?", (balance, existing_id))
                        balance_updated_count += 1
                    
                    if existing_orders != orders_completed:
                        c.execute("UPDATE couriers SET orders_completed = ? WHERE id = ?", (orders_completed, existing_id))
                        orders_updated_count += 1
                    
                    if reject_reason and (not existing_reject_reason or existing_reject_reason != reject_reason):
                        c.execute("UPDATE couriers SET reject_reason = ? WHERE id = ?", (reject_reason, existing_id))
                    
                    if existing_sheet_row != sheet_row:
                        c.execute("UPDATE couriers SET sheet_row = ? WHERE id = ?", (sheet_row, existing_id))
                
                else:
                    registered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    confirmed_at = registered_at if new_status == 'confirmed' else None
                    invited_at = registered_at if new_status == 'invited' else None
                    
                    c.execute('''INSERT INTO couriers 
                                 (recruiter_id, full_name, city, status, balance, registered_at, 
                                  confirmed_at, invited_at, sheet_row, orders_completed, reject_reason) 
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              (recruiter_id, full_name, city, new_status, balance, registered_at, 
                               confirmed_at, invited_at, sheet_row, orders_completed, reject_reason))
                    new_count += 1
                    logger.info(f"✅ Новый курьер: {full_name} со статусом {new_status}")
                    
                    if new_status != 'pending':
                        notify_recruiter_about_status_change(recruiter_id, full_name, city, new_status, reject_reason)
                    
                    if new_status != 'pending' and sheet_row:
                        update_courier_status_in_sheet(sheet_row, status_text_map)
                
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке строки {i+2}: {e}")
                continue
        
        for admin_id in ADMINS:
            c.execute("SELECT id, full_name, city FROM couriers WHERE recruiter_id = ?", (admin_id,))
            db_couriers = c.fetchall()
            
            for courier in db_couriers:
                courier_id, full_name, city = courier
                if (full_name, city) not in existing_couriers:
                    c.execute("DELETE FROM couriers WHERE id = ?", (courier_id,))
                    deleted_count += 1
                    logger.info(f"🗑️ Удален курьер из БД: {full_name} ({city})")
        
        conn.commit()
        
        logger.info("🔄 Пересчитываем балансы...")
        recalc_all_balances()
        
        logger.info(f"📊 ИТОГИ СИНХРОНИЗАЦИИ:")
        logger.info(f"   • Обновлено статусов: {updated_count}")
        logger.info(f"   • Добавлено новых: {new_count}")
        logger.info(f"   • Обновлено балансов: {balance_updated_count}")
        logger.info(f"   • Обновлено заказов: {orders_updated_count}")
        logger.info(f"   • Удалено из БД: {deleted_count}")
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")

def load_from_google_sheets():
    try:
        sheet = get_google_sheet()
        if not sheet:
            return
        
        records = sheet.get_all_records()
        logger.info(f"📥 Загружаем {len(records)} курьеров из Google Sheets")
        
        conn = get_db()
        c = conn.cursor()
        
        new_count = 0
        updated_count = 0
        
        for i, record in enumerate(records):
            try:
                full_name = record.get('ФИО клиента', '').strip()
                city = record.get('Город', '').strip()
                status_text = record.get('СТАТУС', '').strip()
                accepted = record.get('ПРИНЯТО', '0')
                rejected = record.get('ОТКЛОНЕНО', '0')
                
                balance_raw = record.get('Баланс', '0')
                orders_raw = record.get('Выполнено заказов', '0')
                
                try:
                    balance_str = str(balance_raw).strip().replace(' ', '').replace(',', '.')
                    balance = float(balance_str) if balance_str and balance_str != '0' else 0.0
                except:
                    balance = 0.0
                
                try:
                    orders_str = str(orders_raw).strip().replace(' ', '')
                    orders_completed = int(float(orders_str)) if orders_str and orders_str != '0' else 0
                except:
                    orders_completed = 0
                
                if not full_name or not city:
                    continue
                
                status = 'pending'
                if str(accepted).strip() == '1':
                    status = 'confirmed'
                elif str(rejected).strip() == '1':
                    status = 'rejected'
                elif 'Подтвержден' in status_text or '✅' in status_text:
                    status = 'confirmed'
                
                recruiter_username = record.get('Username рекрутера', '').replace('@', '').strip()
                recruiter_id = None
                
                if recruiter_username:
                    c.execute("SELECT user_id FROM users WHERE username = ?", (recruiter_username,))
                    user = c.fetchone()
                    if user:
                        recruiter_id = user[0]
                
                if not recruiter_id:
                    for admin_id in ADMINS:
                        if recruiter_username in ['unknownsorcerer', 'costa'] or str(admin_id) in recruiter_username:
                            recruiter_id = admin_id
                            break
                
                if not recruiter_id:
                    logger.warning(f"⚠️ Рекрутер не найден для курьера {full_name}, строка пропущена")
                    continue
                
                c.execute("SELECT user_id FROM users WHERE user_id = ?", (recruiter_id,))
                if not c.fetchone():
                    logger.info(f"👤 Создаем пользователя-рекрутера с ID {recruiter_id}")
                    registration_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    c.execute("""
                        INSERT INTO users 
                        (user_id, username, first_name, last_name, registration_date, balance, test_passed) 
                        VALUES (?, ?, ?, ?, ?, 0, 0)
                    """, (recruiter_id, recruiter_username or f"user_{recruiter_id}", "Рекрутер", "", registration_date))
                    conn.commit()
                
                c.execute('''SELECT id, status, balance, orders_completed FROM couriers 
                             WHERE full_name = ? AND city = ?''', (full_name, city))
                existing = c.fetchone()
                
                if existing:
                    existing_id, existing_status, existing_balance, existing_orders = existing
                    
                    c.execute('''
                        UPDATE couriers 
                        SET status = ?, balance = ?, sheet_row = ?, recruiter_id = ?, orders_completed = ?
                        WHERE id = ?
                    ''', (status, balance, i + 2, recruiter_id, orders_completed, existing_id))
                    updated_count += 1
                    
                    if status == 'confirmed' and existing_status != 'confirmed':
                        c.execute('''
                            UPDATE couriers 
                            SET confirmed_at = ? 
                            WHERE id = ?
                        ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), existing_id))
                    
                    if existing_orders != orders_completed:
                        logger.info(f"📊 Обновлены заказы {full_name}: {existing_orders} -> {orders_completed}")
                else:
                    registered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    confirmed_at = registered_at if status == 'confirmed' else None
                    
                    c.execute('''
                        INSERT INTO couriers 
                        (recruiter_id, full_name, city, status, balance, registered_at, confirmed_at, sheet_row, orders_completed) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (recruiter_id, full_name, city, status, balance, registered_at, confirmed_at, i + 2, orders_completed))
                    new_count += 1
                    logger.info(f"✅ Добавлен новый курьер: {full_name}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке строки {i+2}: {e}")
                continue
        
        conn.commit()
        
        logger.info(f"✅ Загружено из Google Sheets: {new_count} новых, {updated_count} обновлено")
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки из Google Sheets: {e}")

def backup_database():
    """Сохраняет все данные в JSON файл"""
    global DB_INITIALIZED
    if not DB_INITIALIZED:
        logger.debug("⏳ БД еще не инициализирована, пропускаем бэкап")
        return False
    
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
        if not c.fetchone():
            logger.debug("⏳ Таблицы еще не созданы, пропускаем бэкап")
            return False
        
        users = []
        try:
            users = [dict(row) for row in c.execute("SELECT * FROM users").fetchall()]
        except Exception as e:
            logger.error(f"Ошибка при чтении users: {e}")
        
        withdrawals = []
        try:
            withdrawals = [dict(row) for row in c.execute("SELECT * FROM withdrawals").fetchall()]
        except Exception as e:
            logger.error(f"Ошибка при чтении withdrawals: {e}")
        
        support_tickets = []
        try:
            support_tickets = [dict(row) for row in c.execute("SELECT * FROM support_tickets").fetchall()]
        except Exception as e:
            logger.error(f"Ошибка при чтении support_tickets: {e}")
        
        couriers = []
        try:
            couriers = [dict(row) for row in c.execute("SELECT * FROM couriers").fetchall()]
        except Exception as e:
            logger.error(f"Ошибка при чтении couriers: {e}")
        
        backup = {
            'users': users,
            'withdrawals': withdrawals,
            'support_tickets': support_tickets,
            'couriers': couriers,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
            json.dump(backup, f, default=str, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Автосохранение: {len(users)} users, {len(couriers)} couriers")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения бэкапа: {e}")
        return False

def load_backup():
    """Загружает данные из JSON файла"""
    try:
        if not os.path.exists(BACKUP_FILE):
            logger.info("📭 Файл бэкапа не найден, начинаем с пустой БД")
            return
        
        with open(BACKUP_FILE, 'r', encoding='utf-8') as f:
            backup = json.load(f)
        
        conn = get_db()
        c = conn.cursor()
        
        c.execute("DELETE FROM users")
        c.execute("DELETE FROM withdrawals")
        c.execute("DELETE FROM support_tickets")
        c.execute("DELETE FROM couriers")
        
        for row in backup.get('users', []):
            placeholders = ','.join(['?'] * len(row))
            c.execute(f"INSERT INTO users VALUES ({placeholders})", list(row.values()))
        
        for row in backup.get('withdrawals', []):
            placeholders = ','.join(['?'] * len(row))
            c.execute(f"INSERT INTO withdrawals VALUES ({placeholders})", list(row.values()))
        
        for row in backup.get('support_tickets', []):
            placeholders = ','.join(['?'] * len(row))
            c.execute(f"INSERT INTO support_tickets VALUES ({placeholders})", list(row.values()))
        
        for row in backup.get('couriers', []):
            placeholders = ','.join(['?'] * len(row))
            c.execute(f"INSERT INTO couriers VALUES ({placeholders})", list(row.values()))
        
        conn.commit()
        logger.info(f"✅ Загружено из бэкапа: {len(backup.get('users', []))} users, {len(backup.get('couriers', []))} couriers")
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки бэкапа: {e}")

def start_auto_backup():
    """Запускает автосохранение каждые 5 минут"""
    def backup_worker():
        while True:
            time.sleep(300)
            backup_database()
    
    thread = threading.Thread(target=backup_worker, daemon=True)
    thread.start()
    logger.info("✅ Автосохранение запущено (каждые 5 минут)")

def start_sheet_monitoring():
    """Запускает мониторинг Google Sheets в фоне"""
    def monitor_worker():
        while True:
            try:
                check_pending_couriers()
                time.sleep(300)
            except Exception as e:
                logger.error(f"Ошибка в мониторинге: {e}")
                time.sleep(300)
    
    thread = threading.Thread(target=monitor_worker, daemon=True)
    thread.start()
    logger.info("✅ Мониторинг Google Sheets запущен (интервал 5 минут)")

# ========== ТЕСТОВЫЕ ВОПРОСЫ ==========
TEST_QUESTIONS = [
    {
        'question': 'Что произойдет с профилем рекрутера, если его курьер нарушает правила сервиса?',
        'options': [
            'Ничего, отвечает только курьер',
            'Профиль рекрутера может быть заблокирован',
            'Рекрутер получит штраф 1000 рублей',
            'Курьер получит предупреждение'
        ],
        'correct': 1
    },
    {
        'question': 'Как правильно писать текст вакансии, чтобы не нарушить правила?',
        'options': [
            '"Яндекс Еда ищет курьеров"',
            '"Партнер Яндекс Еды ищет курьеров"',
            '"Срочно требуются курьеры в Яндекс"',
            '"Работа в Яндекс Еде"'
        ],
        'correct': 1
    },
    {
        'question': 'Где категорически нельзя размещать вакансии?',
        'options': [
            'В тематических Telegram-каналах о работе',
            'В комментариях под постами других рекрутеров',
            'На Авито в разделе "Работа"',
            'В группах ВКонтакте о доставке'
        ],
        'correct': 1
    },
    {
        'question': 'Какие документы нужны гражданину РФ для оформления курьером (16+)?',
        'options': [
            'Только паспорт',
            'Паспорт с регистрацией, ИНН, медкнижка (если есть), согласие родителей',
            'Паспорт и СНИЛС',
            'Только ИНН'
        ],
        'correct': 1
    },
    {
        'question': 'Какие документы нужны гражданину Беларуси для оформления курьером?',
        'options': [
            'Только паспорт',
            'Паспорт, ИНН, СНИЛС, трудовой договор',
            'Только миграционная карта',
            'Паспорт и регистрация'
        ],
        'correct': 1
    },
    {
        'question': 'Что такое целевое действие (ЦД)?',
        'options': [
            'Количество заказов, которое нужно выполнить курьеру после оформления',
            'Первая доставка курьера',
            'Регистрация в приложении',
            'Выход на первый слот'
        ],
        'correct': 0
    },
    {
        'question': 'Какой максимальный норматив целевого действия (ЦД)?',
        'options': [
            '50 заказов',
            '100 заказов',
            '130 заказов',
            '200 заказов'
        ],
        'correct': 2
    },
    {
        'question': 'Сколько заказов минимально должен сделать кандидат, чтобы рекрутер мог чувствовать себя в безопасности?',
        'options': [
            'Достаточно 5 заказов',
            'Хотя бы 15 заказов',
            'Минимум 25 заказов',
            'Нужно 30 заказов'
        ],
        'correct': 2
    },
    {
        'question': 'Что получает курьер дополнительно за заказ весом от 10 до 15 кг?',
        'options': [
            'Ничего, это обычный заказ',
            'Дополнительную выплату',
            'Повышенный коэффициент',
            'Бесплатный обед в ресторане'
        ],
        'correct': 1
    },
    {
        'question': 'Что происходит, если курьер ждёт заказ в ресторане более 20 минут?',
        'options': [
            'Время ожидания не оплачивается',
            'Время ожидания оплачивается',
            'Курьер может уйти без заказа',
            'Ресторан платит штраф курьеру'
        ],
        'correct': 1
    }
]

# ========== ФУНКЦИИ ДЛЯ УДАЛЕНИЯ СООБЩЕНИЙ ==========
async def delete_previous_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет предыдущее сообщение бота и сообщение пользователя"""
    try:
        # Удаляем предыдущее сообщение бота
        if 'last_bot_message_id' in context.user_data and 'last_chat_id' in context.user_data:
            try:
                await context.bot.delete_message(
                    chat_id=context.user_data['last_chat_id'],
                    message_id=context.user_data['last_bot_message_id']
                )
            except:
                pass
        
        # Удаляем сообщение пользователя (кроме команд)
        if update.message and not update.message.text.startswith('/'):
            try:
                await context.bot.delete_message(
                    chat_id=update.message.chat_id,
                    message_id=update.message.message_id
                )
            except:
                pass
        
        # Удаляем сообщение с кнопками
        if update.callback_query and update.callback_query.message:
            try:
                await context.bot.delete_message(
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id
                )
            except:
                pass
                
    except Exception as e:
        logger.debug(f"Ошибка при удалении сообщений: {e}")

async def send_and_track(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, parse_mode=None):
    """Отправляет сообщение и сохраняет его ID"""
    await delete_previous_messages(update, context)
    
    chat_id = None
    if update.callback_query:
        chat_id = update.callback_query.message.chat_id
    elif update.message:
        chat_id = update.message.chat_id
    
    if chat_id:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        
        context.user_data['last_bot_message_id'] = message.message_id
        context.user_data['last_chat_id'] = message.chat_id
        return message
    
    return None

async def edit_and_track(query, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, parse_mode=None):
    """Редактирует сообщение и сохраняет ID"""
    await query.edit_message_text(
        text,
        reply_markup=reply_markup,
        parse_mode=parse_mode
    )
    context.user_data['last_bot_message_id'] = query.message.message_id
    context.user_data['last_chat_id'] = query.message.chat_id

async def send_menu_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, menu_type: str, caption: str, keyboard=None):
    """Отправляет изображение меню"""
    await delete_previous_messages(update, context)
    
    chat_id = None
    if update.callback_query:
        chat_id = update.callback_query.message.chat_id
    elif update.message:
        chat_id = update.message.chat_id
    
    if chat_id:
        try:
            photo_url = IMAGES.get(menu_type, IMAGES['main_menu'])
            
            message = await context.bot.send_photo(
                chat_id=chat_id,
                photo=photo_url,
                caption=caption,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            
            context.user_data['last_bot_message_id'] = message.message_id
            context.user_data['last_chat_id'] = message.chat_id
            return message
        except Exception as e:
            logger.error(f"Ошибка отправки фото: {e}")
            return await send_and_track(update, context, caption, keyboard, 'Markdown')
    
    return None

# ========== ОСНОВНЫЕ ФУНКЦИИ ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    
    logger.info(f"🚀 Запуск бота для пользователя {user_id}")
    
    if not is_registered(user_id):
        logger.info(f"📝 Регистрация нового пользователя {user_id}")
        register_user(user_id, user.username, user.first_name, user.last_name)
    
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT test_passed FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    test_passed = result[0] if result else 0
    
    logger.info(f"📊 Статус теста пользователя {user_id}: {test_passed}")
    
    # Удаляем только предыдущие сообщения бота, но НЕ ЧИСТИМ ВЕСЬ ЧАТ
    if 'last_bot_message_id' in context.user_data and 'last_chat_id' in context.user_data:
        try:
            await context.bot.delete_message(
                chat_id=context.user_data['last_chat_id'],
                message_id=context.user_data['last_bot_message_id']
            )
        except:
            pass
    
    # Удаляем сообщение с командой /start
    try:
        await context.bot.delete_message(
            chat_id=update.message.chat_id,
            message_id=update.message.message_id
        )
    except:
        pass
    
    # Отправляем новое меню
    if test_passed == 1:
        keyboard = [
            [InlineKeyboardButton("📋 Вся информация", callback_data='all_info')],
            [InlineKeyboardButton("📝 Пройти тест", callback_data='take_test')],
            [InlineKeyboardButton("💰 Вывод средств", callback_data='withdrawal')],
            [InlineKeyboardButton("👤 Личный кабинет", callback_data='personal_account')],
            [InlineKeyboardButton("💼 Ставки по городам", callback_data='rates')],
            [InlineKeyboardButton("🆘 Обратиться в поддержку", callback_data='support')]
        ]
        menu_text = "🏠 *Главное меню*\n\nВыберите нужный раздел:"
        
        await send_menu_photo(
            update, context,
            menu_type='main_menu',
            caption=menu_text,
            keyboard=InlineKeyboardMarkup(keyboard)
        )
    else:
        keyboard = [
            [InlineKeyboardButton("📋 Вся информация", callback_data='all_info')],
            [InlineKeyboardButton("📝 Пройти тест", callback_data='take_test')]
        ]
        menu_text = "📚 *Для доступа к полному функционалу необходимо пройти тест*\n\nВыберите действие:"
        
        await send_menu_photo(
            update, context,
            menu_type='test_required',
            caption=menu_text,
            keyboard=InlineKeyboardMarkup(keyboard)
        )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    if data == 'next_question':
        await next_question_callback(update, context)
        return
    
    if data.startswith('admin_reply_'):
        await admin_reply_callback(update, context)
        return
    elif data.startswith('admin_close_'):
        await admin_close_callback(update, context)
        return
    elif data.startswith('withdrawal_confirm_'):
        await admin_withdrawal_confirm(update, context)
        return
    elif data.startswith('withdrawal_reject_'):
        await admin_withdrawal_reject_start(update, context)
        return
    elif data == 'message_user':
        await admin_message_user_start(update, context)
        return
    elif data.startswith('msg_'):
        await admin_message_user_send(update, context)
        return
    
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT test_passed FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    test_passed = result[0] if result else 0
    
    protected_sections = ['withdrawal', 'personal_account', 'my_couriers', 'add_courier', 'rates']
    if test_passed == 0 and data in protected_sections:
        keyboard = [[InlineKeyboardButton("📝 Пройти тест", callback_data='take_test')]]
        text = "❌ *Доступ запрещен!*\n\nДля доступа к этому разделу необходимо пройти тест."
        
        if query.message.photo:
            await query.message.delete()
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await edit_and_track(
                query, context,
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        return
    
    if data == 'all_info':
        await show_all_info_menu(query, context)
    elif data == 'take_test':
        await start_test(query, user_id, context)
    elif data == 'withdrawal':
        await withdrawal_menu(query, user_id, context)
    elif data == 'withdrawal_history':
        await user_withdrawal_history(update, context)
    elif data == 'personal_account':
        await personal_account_menu(query, user_id, context)
    elif data == 'my_couriers':
        await show_my_couriers(query, user_id, context)
    elif data == 'add_courier':
        await add_courier_start(query, user_id, context)
    elif data == 'rates':
        await show_rates(query, context)
    elif data == 'support':
        await support_start(query, user_id, context)
    elif data.startswith('info_'):
        await show_info_section(query, context)
    elif data == 'back_to_main':
        await back_to_main(query, user_id, context)
    elif data == 'back_to_info':
        await show_all_info_menu(query, context)
    elif data.startswith('answer_'):
        await handle_test_answer(update, context)
    elif data in ['withdrawal_card', 'withdrawal_yoomoney', 'withdrawal_other']:
        await process_withdrawal_option(query, user_id, context)

async def show_rates(query, context):
    """Ставки по городам"""
    text = (
        "💼 *Ставки по городам*\n\n"
        "Ссылка на Google Таблицу:\n"
        "https://docs.google.com/spreadsheets/d/1Zg-rzfnK1ON9J7TXIGI6yaMEc-ae2v3hQ46sDhnO0n8/edit?usp=sharing\n"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back_to_main')]]
    
    if query.message.photo:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await edit_and_track(
            query, context,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

# ========== ВЫВОД СРЕДСТВ ==========
async def withdrawal_menu(query, user_id, context):
    balance = get_user_balance(user_id)
    
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*), SUM(amount) FROM withdrawals WHERE user_id = ? AND status = 'completed'", (user_id,))
    completed_count, completed_sum = c.fetchone()
    completed_count = completed_count or 0
    completed_sum = completed_sum or 0
    
    text = (
        f"💰 *Вывод средств*\n\n"
        f"💳 *Текущий баланс:* {balance} руб.\n"
        f"📊 *Всего выведено:* {completed_sum} руб. ({completed_count} заявок)\n\n"
        f"Выберите действие:"
    )
    
    keyboard = [
        [InlineKeyboardButton("💳 Создать заявку", callback_data='withdrawal_card')],
        [InlineKeyboardButton("📋 История выводов", callback_data='withdrawal_history')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_main')]
    ]
    
    if query.message.photo:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await edit_and_track(
            query, context,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def user_withdrawal_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает историю выводов пользователя"""
    user_id = update.effective_user.id
    
    conn = get_db()
    c = conn.cursor()
    
    c.execute('''SELECT id, amount, payment_method, status, request_date, completed_date, reject_reason
                 FROM withdrawals 
                 WHERE user_id = ?
                 ORDER BY request_date DESC
                 LIMIT 10''', (user_id,))
    withdrawals = c.fetchall()
    
    if not withdrawals:
        await send_and_track(
            update, context,
            "📭 У вас пока нет заявок на вывод"
        )
        return
    
    text = "📋 *История ваших выводов:*\n\n"
    
    for w in withdrawals:
        id, amount, method, status, req_date, comp_date, reason = w
        
        if status == 'completed':
            status_emoji = "✅"
            status_text = f"Подтвержден {comp_date}"
        elif status == 'rejected':
            status_emoji = "❌"
            status_text = f"Отклонен: {reason}"
        else:
            status_emoji = "⏳"
            status_text = "Ожидает обработки"
        
        text += f"{status_emoji} *Заявка #{id}*\n"
        text += f"💰 Сумма: {amount} руб.\n"
        text += f"💳 Способ: {method}\n"
        text += f"📅 Создана: {req_date}\n"
        text += f"📌 {status_text}\n"
        text += "──────────────────────\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back_to_main')]]
    await send_and_track(
        update, context,
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def process_withdrawal_option(query, user_id, context):
    method_map = {
        'withdrawal_card': 'Карта',
        'withdrawal_yoomoney': 'ЮMoney',
        'withdrawal_other': 'Другой способ'
    }
    method = method_map.get(query.data, 'Неизвестно')
    context.user_data['withdrawal_method'] = method
    
    text = (
        f"Выбран способ: *{method}*\n\n"
        f"Введите сумму и реквизиты в формате:\n"
        f"Сумма|Реквизиты\n\n"
        f"Пример: 500|1234567890123456"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Отмена", callback_data='withdrawal')]]
    
    if query.message.photo:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await edit_and_track(
            query, context,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    
    context.user_data['awaiting_withdrawal_details'] = True

async def handle_withdrawal_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text
    
    try:
        if '|' not in text:
            await send_and_track(
                update, context,
                "❌ Неверный формат. Используйте: Сумма|Реквизиты\n"
                "Пример: 500|1234567890123456"
            )
            return
        
        amount, details = text.split('|', 1)
        amount = float(amount.strip())
        details = details.strip()
        method = context.user_data.get('withdrawal_method', 'unknown')
        balance = get_user_balance(user.id)
        
        if amount < 100:
            await send_and_track(
                update, context,
                "❌ Минимальная сумма вывода: 100 руб."
            )
            return
        
        if amount > balance:
            await send_and_track(
                update, context,
                f"❌ Недостаточно средств. Ваш баланс: {balance} руб."
            )
            return
        
        request_id = create_withdrawal_request(user.id, amount, method, details)
        
        if request_id:
            await send_and_track(
                update, context,
                f"✅ *Заявка на вывод создана!*\n\n"
                f"🆔 Номер заявки: `{request_id}`\n"
                f"💰 Сумма: {amount} руб.\n"
                f"💳 Способ: {method}\n\n"
                f"Ожидайте подтверждения администратором.",
                parse_mode='Markdown'
            )
            
            keyboard = [
                [
                    InlineKeyboardButton("✅ Подтвердить", callback_data=f'withdrawal_confirm_{request_id}'),
                    InlineKeyboardButton("❌ Отказать", callback_data=f'withdrawal_reject_{request_id}')
                ]
            ]
            
            admin_message = (
                f"💰 *НОВАЯ ЗАЯВКА НА ВЫВОД*\n\n"
                f"🆔 *Заявка:* `{request_id}`\n"
                f"👤 *Пользователь:* {user.first_name} (@{user.username})\n"
                f"🆔 *User ID:* `{user.id}`\n"
                f"💰 *Сумма:* {amount} руб.\n"
                f"💳 *Способ:* {method}\n"
                f"📝 *Реквизиты:* {details}"
            )
            
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=admin_message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await send_and_track(
                update, context,
                "❌ Ошибка при создании заявки. Попробуйте позже."
            )
        
    except ValueError:
        await send_and_track(
            update, context,
            "❌ Неверный формат суммы. Введите число."
        )
    except Exception as e:
        await send_and_track(
            update, context,
            f"❌ Ошибка: {str(e)}"
        )
    
    context.user_data['awaiting_withdrawal_details'] = False

async def admin_withdrawal_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not is_admin(update.effective_user.id):
        await query.edit_message_text("❌ У вас нет прав администратора")
        return
    
    request_id = int(query.data.replace('withdrawal_confirm_', ''))
    success, user_id, amount = await confirm_withdrawal(request_id, context)
    
    if success:
        await query.edit_message_text(f"✅ Заявка {request_id} подтверждена")
        
        try:
            user_message = (
                f"✅ *Заявка на вывод подтверждена!*\n\n"
                f"🆔 Номер заявки: `{request_id}`\n"
                f"💰 Сумма: {amount} руб.\n\n"
                f"Средства поступят не раньше 10-15 числа каждого месяца"
            )
            await context.bot.send_message(
                chat_id=user_id,
                text=user_message,
                parse_mode='Markdown'
            )
            logger.info(f"📨 Уведомление о подтверждении отправлено пользователю {user_id}")
        except Exception as e:
            logger.error(f"❌ Не удалось отправить уведомление пользователю {user_id}: {e}")
    else:
        await query.edit_message_text(f"❌ Ошибка при подтверждении заявки {request_id}")

async def admin_withdrawal_reject_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not is_admin(update.effective_user.id):
        await query.edit_message_text("❌ У вас нет прав администратора")
        return
    
    request_id = int(query.data.replace('withdrawal_reject_', ''))
    context.user_data['rejecting_withdrawal'] = request_id
    
    await query.edit_message_text(
        f"📝 Введите причину отказа для заявки #{request_id}:",
        parse_mode='Markdown'
    )

async def handle_withdrawal_reject_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    
    request_id = context.user_data.get('rejecting_withdrawal')
    if not request_id:
        return
    
    reason = update.message.text
    success, user_id, amount = await reject_withdrawal(request_id, reason, context)
    
    if success:
        await update.message.reply_text(f"✅ Заявка {request_id} отклонена")
        
        try:
            user_message = (
                f"❌ *Заявка на вывод отклонена*\n\n"
                f"🆔 Номер заявки: `{request_id}`\n"
                f"💰 Сумма: {amount} руб.\n"
                f"📝 Причина: {reason}\n\n"
                f"Средства не были списаны с вашего баланса."
            )
            await context.bot.send_message(
                chat_id=user_id,
                text=user_message,
                parse_mode='Markdown'
            )
            logger.info(f"📨 Уведомление об отказе отправлено пользователю {user_id}")
        except Exception as e:
            logger.error(f"❌ Не удалось отправить уведомление пользователю {user_id}: {e}")
    else:
        await update.message.reply_text(f"❌ Ошибка при отклонении заявки {request_id}")
    
    context.user_data['rejecting_withdrawal'] = None

# ========== ЛИЧНОЕ СООБЩЕНИЕ ПОЛЬЗОВАТЕЛЮ ==========
async def admin_message_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс отправки личного сообщения пользователю"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(update.effective_user.id):
        await query.edit_message_text("❌ У вас нет прав администратора")
        return
    
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id, username, first_name FROM users ORDER BY user_id")
    users = c.fetchall()
    
    if not users:
        await query.edit_message_text("📭 Нет пользователей для отправки сообщений")
        return
    
    keyboard = []
    row = []
    for i, (user_id, username, first_name) in enumerate(users):
        display_name = first_name or f"ID:{user_id}"
        if username:
            display_name = f"@{username}"
        
        button = InlineKeyboardButton(
            display_name, 
            callback_data=f'msg_{user_id}'
        )
        row.append(button)
        
        if len(row) == 3 or i == len(users) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton("🔙 Отмена", callback_data='admin_back')])
    
    await query.edit_message_text(
        "👤 *Выберите пользователя для отправки сообщения:*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def admin_message_user_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор пользователя для отправки сообщения"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(update.effective_user.id):
        return
    
    user_id = int(query.data.replace('msg_', ''))
    context.user_data['message_target_user'] = user_id
    
    await query.edit_message_text(
        f"📝 Введите сообщение для пользователя (ID: {user_id}):",
        parse_mode='Markdown'
    )
    
    context.user_data['awaiting_admin_message'] = True

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет личное сообщение выбранному пользователю"""
    if not is_admin(update.effective_user.id):
        return
    
    target_user_id = context.user_data.get('message_target_user')
    if not target_user_id:
        return
    
    message_text = update.message.text
    
    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=f"📢 *Сообщение от администратора:*\n\n{message_text}",
            parse_mode='Markdown'
        )
        
        await update.message.reply_text(f"✅ Сообщение отправлено пользователю ID: {target_user_id}")
        logger.info(f"📨 Админ отправил сообщение пользователю {target_user_id}")
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при отправке: {e}")
        logger.error(f"❌ Ошибка отправки сообщения пользователю {target_user_id}: {e}")
    
    context.user_data['message_target_user'] = None
    context.user_data['awaiting_admin_message'] = False

# ========== ПОДДЕРЖКА ==========
async def support_start(query, user_id, context):
    text = (
        "🆘 *Поддержка*\n\n"
        "Опишите вашу проблему или вопрос. Я передам сообщение администратору.\n\n"
        "⏱ *Время ответа:* от 15 минут до 1 часа\n\n"
        "Напишите ваш вопрос одним сообщением:"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Отмена", callback_data='back_to_main')]]
    
    if query.message.photo:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await edit_and_track(
            query, context,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    
    context.user_data['awaiting_support_message'] = True

async def handle_support_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message_text = update.message.text
    
    ticket_id = create_support_ticket(
        user.id,
        user.username,
        user.first_name,
        message_text
    )
    
    await send_and_track(
        update, context,
        f"✅ Ваше обращение принято!\n\n"
        f"🆔 Номер обращения: {ticket_id}\n"
        f"⏱ Ожидаемое время ответа: от 15 минут до 1 часа\n\n"
        f"Как только администратор ответит, вы получите уведомление."
    )
    
    keyboard = [
        [InlineKeyboardButton("📨 Ответить", callback_data=f'admin_reply_{ticket_id}')],
        [InlineKeyboardButton("✅ Закрыть", callback_data=f'admin_close_{ticket_id}')]
    ]
    
    def clean_text(text):
        if not text:
            return ""
        chars_to_replace = ['_', '*', '`', '[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for char in chars_to_replace:
            text = text.replace(char, ' ')
        return text
    
    clean_first_name = clean_text(user.first_name)
    clean_username = clean_text(f"@{user.username}") if user.username else "нет username"
    clean_message = clean_text(message_text)
    
    admin_message = (
        f"🆘 *НОВОЕ ОБРАЩЕНИЕ В ПОДДЕРЖКУ*\n\n"
        f"🆔 *Тикет:* `{ticket_id}`\n"
        f"👤 *Пользователь:* {clean_first_name} ({clean_username})\n"
        f"🆔 *User ID:* `{user.id}`\n"
        f"📅 *Время:* {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"📝 *Сообщение:*\n{clean_message}"
    )
    
    for admin_id in ADMINS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=admin_message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            logger.info(f"📨 Уведомление о тикете {ticket_id} отправлено админу {admin_id}")
        except Exception as e:
            logger.error(f"❌ Не удалось отправить уведомление админу {admin_id}: {e}")
    
    context.user_data['awaiting_support_message'] = False

async def admin_reply_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not is_admin(update.effective_user.id):
        await query.edit_message_text("❌ У вас нет прав администратора")
        return
    
    ticket_id = query.data.replace('admin_reply_', '')
    
    if not is_ticket_open(ticket_id):
        await query.edit_message_text(
            f"❌ Тикет {ticket_id} уже обработан другим администратором.\n"
            f"Обновите список тикетов командой /tickets"
        )
        return
    
    context.user_data['replying_to_ticket'] = ticket_id
    admin_name = update.effective_user.first_name or f"Админ {update.effective_user.id}"
    
    for admin_id in ADMINS:
        if admin_id != update.effective_user.id:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"⚠️ *Внимание!*\n\n"
                         f"Администратор {admin_name} начал отвечать на тикет `{ticket_id}`.\n"
                         f"Пожалуйста, не отвечайте на этот тикет, чтобы избежать дублирования.",
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить админа {admin_id}: {e}")
    
    await query.edit_message_text(
        f"📝 Введите ответ для тикета `{ticket_id}`:",
        parse_mode='Markdown'
    )

async def admin_close_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not is_admin(update.effective_user.id):
        await query.edit_message_text("❌ У вас нет прав администратора")
        return
    
    ticket_id = query.data.replace('admin_close_', '')
    
    if not is_ticket_open(ticket_id):
        await query.edit_message_text(
            f"❌ Тикет {ticket_id} уже обработан другим администратором."
        )
        return
    
    ticket = get_ticket(ticket_id)
    admin_name = update.effective_user.first_name or f"Админ {update.effective_user.id}"
    
    if ticket:
        if close_ticket(ticket_id, "Тикет закрыт администратором"):
            try:
                await context.bot.send_message(
                    chat_id=ticket[1],
                    text=f"🆘 *Обращение #{ticket_id} закрыто*\n\n"
                         f"Ваш тикет был закрыт администратором {admin_name}.",
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"❌ Не удалось уведомить пользователя: {e}")
            
            for admin_id in ADMINS:
                if admin_id != update.effective_user.id:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=f"✅ Тикет `{ticket_id}` был закрыт администратором {admin_name}",
                            parse_mode='Markdown'
                        )
                    except:
                        pass
            
            await query.edit_message_text(f"✅ Тикет {ticket_id} закрыт")
        else:
            await query.edit_message_text(f"❌ Ошибка при закрытии тикета {ticket_id}")
    else:
        await query.edit_message_text("❌ Тикет не найден")

async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    
    ticket_id = context.user_data.get('replying_to_ticket')
    if not ticket_id:
        return
    
    if not is_ticket_open(ticket_id):
        await update.message.reply_text(
            f"❌ Тикет {ticket_id} уже обработан другим администратором.\n"
            f"Ваш ответ не был отправлен."
        )
        context.user_data['replying_to_ticket'] = None
        return
    
    reply_text = update.message.text
    ticket = get_ticket(ticket_id)
    admin_name = update.effective_user.first_name or f"Админ {update.effective_user.id}"
    
    if ticket:
        if close_ticket(ticket_id, reply_text):
            context.user_data['replying_to_ticket'] = None
            
            user_message = (
                f"🆘 *Ответ на обращение #{ticket_id}*\n\n"
                f"📝 *Ваш вопрос:*\n{ticket[4]}\n\n"
                f"💬 *Ответ администратора ({admin_name}):*\n{reply_text}\n\n"
                f"⏱ Время ответа: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            
            try:
                await context.bot.send_message(
                    chat_id=ticket[1],
                    text=user_message,
                    parse_mode='Markdown'
                )
                
                for admin_id in ADMINS:
                    if admin_id != update.effective_user.id:
                        try:
                            await context.bot.send_message(
                                chat_id=admin_id,
                                text=f"✅ *Тикет закрыт*\n\n"
                                     f"Администратор {admin_name} ответил на тикет `{ticket_id}`.\n\n"
                                     f"💬 *Ответ:*\n{reply_text[:200]}..." if len(reply_text) > 200 else f"💬 *Ответ:*\n{reply_text}",
                                parse_mode='Markdown'
                            )
                        except Exception as e:
                            logger.error(f"Не удалось уведомить админа {admin_id}: {e}")
                
                await update.message.reply_text(f"✅ Ответ на тикет {ticket_id} отправлен пользователю")
                
            except Exception as e:
                await update.message.reply_text(f"❌ Не удалось отправить ответ пользователю: {e}")
        else:
            await update.message.reply_text(f"❌ Ошибка при ответе на тикет {ticket_id}")
    else:
        await update.message.reply_text("❌ Тикет не найден")
    
    context.user_data['replying_to_ticket'] = None

# ========== ЛИЧНЫЙ КАБИНЕТ ==========
async def personal_account_menu(query, user_id, context):
    text = (
        "👤 *Личный кабинет*\n\n"
        "Выберите действие:\n\n"
        "📌 *Важно:* После приглашения курьера, не забудьте добавить его в список!"
    )
    
    keyboard = [
        [InlineKeyboardButton("👥 Список моих курьеров", callback_data='my_couriers')],
        [InlineKeyboardButton("📝 Записать курьера", callback_data='add_courier')],
        [InlineKeyboardButton("👀 Посмотреть лидов", url='https://partners-app.yandex.ru/team_ref/92cc13ee5ebf4e39beaf9e63107415a7?locale=ru')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_main')]
    ]
    
    if query.message.photo:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await edit_and_track(
            query, context,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def show_my_couriers(query, user_id, context):
    couriers = get_recruiter_couriers(user_id)
    total_balance = get_user_balance(user_id)
    
    if not couriers:
        text = f"📭 *У вас пока нет записанных курьеров*\n\n💰 *Общий баланс:* {total_balance} руб."
    else:
        text = f"👥 *Ваши курьеры:*\n\n💰 *Общий баланс:* {total_balance} руб.\n\n"
        for full_name, city, status, reg_date, conf_date, balance, orders_completed in couriers:
            date_obj = datetime.strptime(reg_date, "%Y-%m-%d %H:%M:%S")
            date_str = date_obj.strftime("%d.%m.%Y")
            
            if status == 'confirmed':
                status_emoji = "✅"
                conf_info = f" (подтвержден"
                if conf_date:
                    conf_date_obj = datetime.strptime(conf_date, "%Y-%m-%d %H:%M:%S")
                    conf_info += f" {conf_date_obj.strftime('%d.%m.%Y')}"
                conf_info += ")"
            elif status == 'rejected':
                status_emoji = "❌"
                conf_info = f" (отклонен)"
            else:
                status_emoji = "⏳"
                conf_info = f" (ожидает проверки)"
            
            text += f"{status_emoji} *{full_name}* — {city}\n"
            text += f"   📅 {date_str}{conf_info}\n"
            text += f"   💰 Баланс: {balance} руб.\n"
            text += f"   📊 *Выполнено заказов: {orders_completed}*\n\n"
    
    keyboard = [
        [InlineKeyboardButton("📝 Записать курьера", callback_data='add_courier')],
        [InlineKeyboardButton("🔙 Назад", callback_data='personal_account')]
    ]
    
    if query.message.photo:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await edit_and_track(
            query, context,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def add_courier_start(query, user_id, context):
    text = (
        "📝 *Запись курьера*\n\n"
        "Введите данные курьера в формате:\n"
        "`Фамилия Имя, Город`\n\n"
        "Например:\n"
        "`Иванов Иван, Москва`\n\n"
        "После отправки заявка будет отправлена на проверку администратору. ✅"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Отмена", callback_data='personal_account')]]
    
    if query.message.photo:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await edit_and_track(
            query, context,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    
    context.user_data['awaiting_courier_data'] = True

async def handle_courier_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    first_name = update.effective_user.first_name or "Пользователь"
    text = update.message.text.strip()
    
    try:
        if ',' not in text:
            await send_and_track(
                update, context,
                "❌ Неверный формат. Используйте: Фамилия Имя, Город\n"
                "Пример: Иванов Иван, Москва"
            )
            return
        
        full_name, city = text.split(',', 1)
        full_name = full_name.strip()
        city = city.strip()
        
        if not full_name or not city:
            await send_and_track(
                update, context,
                "❌ Имя и город не могут быть пустыми"
            )
            return
        
        success, message = add_courier(user_id, username, first_name, full_name, city)
        
        if success:
            await send_and_track(
                update, context,
                f"✅ *{message}*\n\n"
                f"👤 *Имя:* {full_name}\n"
                f"🏙 *Город:* {city}\n\n"
                f"Статус будет обновлен после проверки администратором.",
                parse_mode='Markdown'
            )
        else:
            await send_and_track(
                update, context,
                f"❌ {message}"
            )
        
    except Exception as e:
        await send_and_track(
            update, context,
            f"❌ Ошибка: {str(e)}"
        )
    
    context.user_data['awaiting_courier_data'] = False
    
    keyboard = [
        [InlineKeyboardButton("👥 Список курьеров", callback_data='my_couriers')],
        [InlineKeyboardButton("📝 Записать ещё", callback_data='add_courier')],
        [InlineKeyboardButton("🔙 В личный кабинет", callback_data='personal_account')]
    ]
    await send_and_track(
        update, context,
        "👤 *Личный кабинет*\n\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

# ========== ТЕСТИРОВАНИЕ ==========
async def start_test(query, user_id, context):
    logger.info(f"🎯 Начало теста для пользователя {user_id}")
    
    can_take, minutes_left = can_take_test(user_id)
    
    if not can_take:
        logger.info(f"⏳ Тест недоступен для {user_id}, осталось {minutes_left} минут")
        text = f"⏳ *Тест временно недоступен*\n\nВы уже проходили тест недавно. Следующая попытка будет доступна через *{minutes_left} минут*."
        
        if query.message.photo:
            await query.message.delete()
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(
                text=text,
                parse_mode='Markdown'
            )
        return
    
    shuffled = random.sample(TEST_QUESTIONS, len(TEST_QUESTIONS))
    context.user_data['test_answers'] = []
    context.user_data['test_current'] = 0
    context.user_data['test_questions'] = shuffled
    
    logger.info(f"✅ Тест инициализирован, вопросов: {len(shuffled)}")
    
    await show_test_question(query, context)

async def show_test_question(query, context):
    try:
        current = context.user_data.get('test_current', 0)
        questions = context.user_data.get('test_questions', [])
        
        logger.info(f"📊 Показ вопроса {current+1} из {len(questions)}")
        
        if current >= len(questions):
            logger.info("🏁 Все вопросы показаны, завершаем тест")
            await finish_test(query, context)
            return
        
        question = questions[current]
        
        keyboard = []
        for i, option in enumerate(question['options']):
            keyboard.append([InlineKeyboardButton(option, callback_data=f'answer_{i}')])
        
        keyboard.append([InlineKeyboardButton("❌ Отменить тест", callback_data='back_to_main')])
        
        text = f"📝 *Вопрос {current + 1} из {len(questions)}*\n\n{question['question']}"
        
        if query.message.photo:
            await query.message.delete()
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        
    except Exception as e:
        logger.error(f"❌ Ошибка в show_test_question: {e}")
        logger.error(traceback.format_exc())

async def handle_test_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    try:
        await query.answer()
        
        user_id = query.from_user.id
        answer_index = int(query.data.replace('answer_', ''))
        current = context.user_data.get('test_current', 0)
        questions = context.user_data.get('test_questions', [])
        answers = context.user_data.get('test_answers', [])
        
        logger.info(f"📝 Обработка ответа: пользователь {user_id}, вопрос {current+1}, ответ {answer_index}")
        
        if not questions or current >= len(questions):
            logger.error(f"❌ Ошибка теста: questions={questions}, current={current}")
            if query.message.photo:
                await query.message.delete()
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ Ошибка теста. Начните заново."
                )
            else:
                await query.edit_message_text("❌ Ошибка теста. Начните заново.")
            return
        
        question = questions[current]
        correct = question['correct']
        is_correct = (answer_index == correct)
        answers.append(is_correct)
        context.user_data['test_answers'] = answers
        
        logger.info(f"✅ Ответ {'верный' if is_correct else 'неверный'}. Правильный: {correct}")
        
        if is_correct:
            text = "✅ *Верно!*"
        else:
            correct_text = question['options'][correct]
            text = f"❌ *Неверно!*\nПравильный ответ: *{correct_text}*"
        
        context.user_data['test_current'] = current + 1
        
        if context.user_data['test_current'] >= len(questions):
            logger.info("🎯 Тест завершен, вызываем finish_test")
            await finish_test(query, context)
            return
        
        keyboard = [[InlineKeyboardButton("➡️ Далее", callback_data='next_question')]]
        
        if query.message.photo:
            await query.message.delete()
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_test_answer: {e}")
        logger.error(traceback.format_exc())
        try:
            if query.message.photo:
                await query.message.delete()
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ Произошла ошибка. Начните тест заново."
                )
            else:
                await query.edit_message_text("❌ Произошла ошибка. Начните тест заново.")
        except:
            pass

async def next_question_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    try:
        await query.answer()
        
        logger.info("➡️ Переход к следующему вопросу")
        
        if 'test_current' not in context.user_data or 'test_questions' not in context.user_data:
            logger.warning("❌ Данные теста не найдены")
            keyboard = [[InlineKeyboardButton("📝 Начать тест", callback_data='take_test')]]
            
            if query.message.photo:
                await query.message.delete()
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ Тест не найден. Начните заново.",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await query.edit_message_text(
                    text="❌ Тест не найден. Начните заново.",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return
        
        await show_test_question(query, context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в next_question_callback: {e}")
        logger.error(traceback.format_exc())
        try:
            if query.message.photo:
                await query.message.delete()
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ Произошла ошибка. Начните тест заново."
                )
            else:
                await query.edit_message_text("❌ Произошла ошибка. Начните тест заново.")
        except:
            pass

async def finish_test(query, context):
    answers = context.user_data.get('test_answers', [])
    correct_count = sum(1 for a in answers if a)
    user_id = query.from_user.id
    
    logger.info(f"🎯 Завершение теста для пользователя {user_id}, правильных ответов: {correct_count}")
    
    if correct_count >= 7:
        logger.info(f"✅ Пользователь {user_id} сдал тест")
        
        success = update_test_status(user_id, True)
        
        if not success:
            logger.error(f"❌ Ошибка при сохранении теста для {user_id}")
            update_test_status(user_id, True)
        
        context.user_data.pop('test_answers', None)
        context.user_data.pop('test_current', None)
        context.user_data.pop('test_questions', None)
        
        try:
            await query.message.delete()
        except:
            pass
        
        keyboard = [
            [InlineKeyboardButton("📋 Вся информация", callback_data='all_info')],
            [InlineKeyboardButton("📝 Пройти тест", callback_data='take_test')],
            [InlineKeyboardButton("💰 Вывод средств", callback_data='withdrawal')],
            [InlineKeyboardButton("👤 Личный кабинет", callback_data='personal_account')],
            [InlineKeyboardButton("💼 Ставки по городам", callback_data='rates')],
            [InlineKeyboardButton("🆘 Обратиться в поддержку", callback_data='support')]
        ]
        menu_text = "🏠 *Главное меню*\n\nВыберите нужный раздел:"
        
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=IMAGES['main_menu'],
            caption=menu_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
        
    elif correct_count < 3:
        logger.info(f"❌ Пользователь {user_id} провалил тест")
        update_test_status(user_id, False)
        text = (
            f"❌ *Тест не пройден*\n\n"
            f"Правильных ответов: *{correct_count} из 10*\n\n"
            f"⏳ Следующая попытка будет доступна через *5 минут*."
        )
        keyboard = [[InlineKeyboardButton("🏠 В главное меню", callback_data='back_to_main')]]
        
        context.user_data.pop('test_answers', None)
        context.user_data.pop('test_current', None)
        context.user_data.pop('test_questions', None)
        
        if query.message.photo:
            await query.message.delete()
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
    else:
        logger.info(f"⚠️ Пользователь {user_id} не сдал тест, но может попробовать снова")
        update_test_status(user_id, False)
        text = (
            f"⚠️ *Тест не пройден*\n\n"
            f"Правильных ответов: *{correct_count} из 10*\n\n"
            f"📝 Вы можете попробовать снова прямо сейчас."
        )
        keyboard = [[InlineKeyboardButton("📝 Пройти тест заново", callback_data='take_test')]]
        
        context.user_data.pop('test_answers', None)
        context.user_data.pop('test_current', None)
        context.user_data.pop('test_questions', None)
        
        if query.message.photo:
            await query.message.delete()
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )

# ========== МЕНЮ ИНФОРМАЦИИ ==========
async def show_all_info_menu(query, context):
    keyboard = [
        [InlineKeyboardButton("📌 Если курьер нарушает правила", callback_data='info_rules_violation')],
        [InlineKeyboardButton("📢 Маркировка рекламы", callback_data='info_ad_marking')],
        [InlineKeyboardButton("🚨 ВНИМАНИЕ!", callback_data='info_warning')],
        [InlineKeyboardButton("📄 Документы для оформления", callback_data='info_documents')],
        [InlineKeyboardButton("🎯 Целевое действие (ЦД)", callback_data='info_target_action')],
        [InlineKeyboardButton("💰 Когда приходят выплаты?", callback_data='info_payments')],
        [InlineKeyboardButton("💬 Как общаться с кандидатом", callback_data='info_communication')],
        [InlineKeyboardButton("📈 Мотивация и доход курьера", callback_data='info_motivation')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_main')]
    ]
    
    text = "📋 *Вся информация*\n\nВыберите интересующий вас раздел:"
    
    if query.message.photo:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await edit_and_track(
            query, context,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def show_info_section(query, context):
    section = query.data.replace('info_', '')
    
    info_texts = {
        'rules_violation': """
📌 *Если курьер нарушает правила сервиса*

⚠️ *Важно понимать!*

Вы как рекрутер несете полную ответственность за курьеров, которых привлекаете. Если курьер нарушает правила сервиса:

• Профиль рекрутера может быть заблокирован
• Вы лишаетесь доступа ко всем привлеченным курьерам
• Средства могут быть заморожены

*Помните:* вы гарантируете качество работы своих курьеров.
        """,
        'ad_marking': """
📢 *Соблюдайте требования по маркировке рекламы*

✅ *Разрешенные площадки:*
• Авито, hh.ru, Юла, SuperJob, Жердеш.ру, Бирге.ру
• Telegram-каналы о работе
• Группы ВК по трудоустройству

📝 *Как правильно писать:*
НЕЛЬЗЯ: "Яндекс Еда ищет курьеров"
МОЖНО: "Партнер Яндекс Еды ищет курьеров"

❌ *Запрещено:* спам в комментариях и личных сообщениях
        """,
        'warning': """
🚨 *ВНИМАНИЕ!*

*НЕДОПУСТИМО:*

❌ регистрация фейковых аккаунтов
❌ использование чужих данных
❌ вводящая в заблуждение информация о доходах

⚠️ *Фрод = блокировка навсегда!*

💰 *Бонусы за приглашение курьеров*

Вы можете приглашать курьеров и получать бонусы за успешных кандидатов. Однако помните:

• ❌ Попытки обмана (фейковые анкеты, накрутки) приведут к мгновенной блокировке аккаунта
• ✅ Работайте честно, привлекайте реальных курьеров
• 💰 Только качественные кандидаты приносят стабильный доход

*Будьте честны с сервисом — и сервис будет честен с вами!*
        """,
        'documents': """
📄 *Документы для оформления курьера*

🇷🇺 *Граждане РФ:*
• Паспорт с регистрацией
• ИНН
• Медкнижка (если есть)
• Согласие родителей (16+)

🌍 *Граждане ЕАЭС:*
• Паспорт
• ИНН
• СНИЛС (если есть)
• Трудовой договор
        """,
        'target_action': """
🎯 *Целевое действие (ЦД)*

*Что это?* Количество заказов после оформления

📊 *Максимум:* 130 заказов

👤 *Активный курьер:* вышел на первый слот

*Важно:* Минимум 25 заказов для безопасности рекрутера
        """,
        'payments': """
💰 *Когда приходят выплаты?*

Выплаты приходят после выполнения 5 доставленных заказов

📅 *Сроки:* не позднее 10 дней после отчётного периода

⚠️ *Внимание:* Минимум 25 заказов для безопасности рекрутера
        """,
        'communication': """
💬 *Как общаться с кандидатом*

👥 *Учитывайте возраст:*
• Молодым — неформально, на "ты"
• Старшим — деловое общение, на "вы"

📋 *Новичкам:* объясняйте пошагово

💰 *Личная выгода:* 
• Студентам — свободный режим
• Семейным — баланс работы и жизни
        """,
        'motivation': """
📈 *Мотивация и доход курьера*

✅ Фиксированная оплата
✅ Свободный режим
✅ Страхование (доставка + личное время)
✅ Юридическая поддержка (3 консультации/мес)
✅ Доплата за тяжёлые заказы (10-15 кг)
✅ Оплата ожидания (до 20 мин)
✅ Повышенные коэффициенты
✅ Чаевые
✅ Бонусы и спецпредложения

🧮 *Калькулятор дохода* внизу 👇
        """
    }
    
    text = info_texts.get(section, "Информация не найдена")
    
    if section == 'motivation':
        keyboard = [
            [InlineKeyboardButton("🧮 Открыть калькулятор дохода", url=CALCULATOR_URL)],
            [InlineKeyboardButton("🔙 Назад к разделам", callback_data='back_to_info')]
        ]
    else:
        keyboard = [[InlineKeyboardButton("🔙 Назад к разделам", callback_data='back_to_info')]]
    
    if query.message.photo:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await edit_and_track(
            query, context,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

# ========== НАВИГАЦИЯ ==========
async def back_to_main(query, user_id, context):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT test_passed FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    test_passed = result[0] if result else 0
    
    logger.info(f"🏠 Возврат в главное меню для пользователя {user_id}, test_passed = {test_passed}")
    
    await query.message.delete()
    
    if test_passed == 1:
        keyboard = [
            [InlineKeyboardButton("📋 Вся информация", callback_data='all_info')],
            [InlineKeyboardButton("📝 Пройти тест", callback_data='take_test')],
            [InlineKeyboardButton("💰 Вывод средств", callback_data='withdrawal')],
            [InlineKeyboardButton("👤 Личный кабинет", callback_data='personal_account')],
            [InlineKeyboardButton("💼 Ставки по городам", callback_data='rates')],
            [InlineKeyboardButton("🆘 Обратиться в поддержку", callback_data='support')]
        ]
        menu_text = "🏠 *Главное меню*\n\nВыберите нужный раздел:"
        
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=IMAGES['main_menu'],
            caption=menu_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        keyboard = [
            [InlineKeyboardButton("📋 Вся информация", callback_data='all_info')],
            [InlineKeyboardButton("📝 Пройти тест", callback_data='take_test')]
        ]
        menu_text = "📚 *Для доступа к полному функционалу необходимо пройти тест*\n\nВыберите действие:"
        
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=IMAGES['test_required'],
            caption=menu_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

# ========== ОБРАБОТЧИК СООБЩЕНИЙ ==========
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if context.user_data.get('awaiting_support_message'):
        await handle_support_message(update, context)
        return
    
    if context.user_data.get('replying_to_ticket'):
        await handle_admin_reply(update, context)
        return
    
    if context.user_data.get('awaiting_courier_data'):
        await handle_courier_input(update, context)
        return
    
    if context.user_data.get('awaiting_withdrawal_details'):
        await handle_withdrawal_input(update, context)
        return
    
    if context.user_data.get('rejecting_withdrawal'):
        await handle_withdrawal_reject_reason(update, context)
        return
    
    if context.user_data.get('awaiting_admin_message'):
        await handle_admin_message(update, context)
        return
    
    await send_and_track(
        update, context,
        "Используйте команду /start для навигации"
    )

# ========== АДМИН-КОМАНДЫ ==========
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Панель администратора"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора")
        return
    
    keyboard = [
        [InlineKeyboardButton("📋 Все команды", callback_data='admin_commands')],
        [InlineKeyboardButton("👤 Отправить сообщение пользователю", callback_data='message_user')],
        [InlineKeyboardButton("🔄 Синхронизация", callback_data='admin_sync')]
    ]
    
    text = (
        "🛠 *Панель администратора*\n\n"
        "📋 *Доступные команды:*\n\n"
        "🔹 `/admin` - показать это меню\n"
        "🔹 `/sync` - синхронизация с Google Sheets\n"
        "🔹 `/checkdb` - проверить состояние БД\n"
        "🔹 `/couriers` - список всех курьеров\n"
        "🔹 `/withdrawals` - список заявок на вывод\n"
        "🔹 `/tickets` - список тикетов поддержки\n"
        "🔹 `/userbalance ID` - детальный баланс пользователя\n"
        "🔹 `/fixbalance ID` - исправить баланс пользователя\n"
        "🔹 `/fixbalance all` - исправить балансы всех\n"
        "🔹 `/fixmy` - исправить курьеров без рекрутера\n"
        "🔹 `/fixusers` - исправить данные пользователей\n"
        "🔹 `/testgoogle` - тест подключения к Google Sheets\n"
        "🔹 `/broadcast` - сделать рассылку\n"
        "🔹 `/restore` - восстановить данные из бэкапа"
    )
    
    await update.message.reply_text(
        text, 
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def admin_restore(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручное восстановление из бэкапа"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора")
        return
    
    await update.message.reply_text("🔄 Восстанавливаю данные из бэкапа...")
    load_backup()
    
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users")
    users_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM couriers")
    couriers_count = c.fetchone()[0]
    
    await update.message.reply_text(
        f"✅ Восстановление завершено!\n"
        f"👥 Пользователей: {users_count}\n"
        f"👤 Курьеров: {couriers_count}"
    )

async def admin_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительная синхронизация с Google Sheets"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора")
        return
    
    status_msg = await update.message.reply_text("🔄 Начинаю синхронизацию с Google Sheets...")
    
    try:
        check_pending_couriers()
        
        conn = get_db()
        c = conn.cursor()
        
        c.execute("SELECT status, COUNT(*) FROM couriers GROUP BY status")
        stats = c.fetchall()
        
        stats_text = "\n".join([f"• {s[0]}: {s[1]}" for s in stats]) if stats else "• нет данных"
        
        await status_msg.edit_text(
            f"✅ *Синхронизация завершена!*\n\n"
            f"📊 *Текущая статистика курьеров:*\n{stats_text}",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка синхронизации: {str(e)}")

async def admin_check_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка файла БД"""
    if not is_admin(update.effective_user.id):
        return
    
    try:
        if os.path.exists(DB_PATH):
            size = os.path.getsize(DB_PATH)
            text = f"✅ Файл БД существует\n📦 Размер: {size} байт\n📍 Путь: {os.path.abspath(DB_PATH)}\n\n"
            
            conn = get_db()
            c = conn.cursor()
            
            c.execute("SELECT COUNT(*) FROM couriers")
            couriers_count = c.fetchone()[0]
            text += f"👥 Курьеров в БД: {couriers_count}\n"
            
            c.execute("SELECT COUNT(*) FROM users")
            users_count = c.fetchone()[0]
            text += f"👤 Пользователей в БД: {users_count}\n"
            
            c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'")
            pending_withdrawals = c.fetchone()[0]
            text += f"💰 Ожидающих заявок на вывод: {pending_withdrawals}\n"
            
            c.execute("SELECT COUNT(*) FROM support_tickets WHERE status='open'")
            open_tickets = c.fetchone()[0]
            text += f"🆘 Открытых тикетов: {open_tickets}\n"
            
            if couriers_count > 0:
                c.execute("SELECT full_name, city, status FROM couriers LIMIT 5")
                couriers = c.fetchall()
                text += "\n📋 Первые 5 курьеров:\n"
                for i, (name, city, status) in enumerate(couriers, 1):
                    emoji = "✅" if status == 'confirmed' else "⏳" if status == 'pending' else "❌"
                    text += f"{i}. {emoji} {name} - {city}\n"
        else:
            text = f"❌ Файл БД НЕ существует!\n📍 Путь: {os.path.abspath(DB_PATH)}"
        
        await update.message.reply_text(text)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def admin_check_couriers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка всех курьеров в БД"""
    if not is_admin(update.effective_user.id):
        return
    
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute('''
            SELECT c.id, c.full_name, c.city, c.status, c.balance, c.recruiter_id, u.username, u.first_name
            FROM couriers c
            LEFT JOIN users u ON c.recruiter_id = u.user_id
            ORDER BY c.id DESC
            LIMIT 30
        ''')
        couriers = c.fetchall()
        
        if not couriers:
            await update.message.reply_text("📭 В БД нет курьеров")
            return
        
        text = "📋 *ВСЕ КУРЬЕРЫ В БД:*\n\n"
        for courier in couriers:
            id, name, city, status, balance, recruiter_id, username, first_name = courier
            status_emoji = "✅" if status == 'confirmed' else "⏳" if status == 'pending' else "❌"
            recruiter_info = f"@{username}" if username else f"ID:{recruiter_id}"
            
            text += f"{status_emoji} *{name}* - {city}\n"
            text += f"   🆔 Курьера: {id}\n"
            text += f"   👤 Рекрутер: {recruiter_info}\n"
            text += f"   💰 Баланс: {balance}\n\n"
        
        if len(text) > 4000:
            for i in range(0, len(text), 4000):
                await update.message.reply_text(text[i:i+4000], parse_mode='Markdown')
        else:
            await update.message.reply_text(text, parse_mode='Markdown')
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def admin_withdrawals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр всех заявок на вывод"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора")
        return
    
    withdrawals = get_all_withdrawals(30)
    
    if not withdrawals:
        await update.message.reply_text("📭 Нет заявок на вывод")
        return
    
    text = "💰 *ВСЕ ЗАЯВКИ НА ВЫВОД:*\n\n"
    for w in withdrawals:
        id, user_id, first_name, username, amount, method, details, status, request_date, completed_date, reject_reason = w
        status_emoji = "✅" if status == 'completed' else "❌" if status == 'rejected' else "⏳"
        status_text = f"{status_emoji} *Заявка #{id}*\n"
        text += status_text
        text += f"👤 {first_name} (@{username})\n"
        text += f"💰 {amount} руб.\n"
        text += f"💳 {method}: {details}\n"
        text += f"📅 Создана: {request_date}\n"
        if status == 'completed':
            text += f"✅ Подтверждена: {completed_date}\n"
        elif status == 'rejected':
            text += f"❌ Отклонена: {completed_date}\n"
            text += f"📝 Причина: {reject_reason}\n"
        text += "──────────────────────\n"
    
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await update.message.reply_text(text[i:i+4000], parse_mode='Markdown')
    else:
        await update.message.reply_text(text, parse_mode='Markdown')

async def admin_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр всех тикетов поддержки"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора")
        return
    
    tickets = get_all_tickets(30)
    
    if not tickets:
        await update.message.reply_text("📭 Нет тикетов поддержки")
        return
    
    text = "🆘 *ВСЕ ТИКЕТЫ ПОДДЕРЖКИ:*\n\n"
    for ticket in tickets:
        ticket_id, user_id, username, first_name, message, status, created_at, answered_at, admin_reply = ticket
        status_emoji = "✅" if status == 'closed' else "⏳"
        status_text = f"{status_emoji} *Тикет #{ticket_id}*\n"
        text += status_text
        text += f"👤 {first_name} (@{username})\n"
        text += f"📝 {message[:100]}...\n"
        text += f"📅 Создан: {created_at}\n"
        if status == 'closed':
            text += f"✅ Закрыт: {answered_at}\n"
            if admin_reply:
                text += f"💬 Ответ: {admin_reply[:50]}...\n"
        text += "──────────────────────\n"
    
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await update.message.reply_text(text[i:i+4000], parse_mode='Markdown')
    else:
        await update.message.reply_text(text, parse_mode='Markdown')

async def admin_fix_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Исправляет баланс конкретного пользователя или всех"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора")
        return
    
    conn = get_db()
    c = conn.cursor()
    
    if context.args and context.args[0].isdigit():
        user_id = int(context.args[0])
        
        c.execute("SELECT SUM(balance) FROM couriers WHERE recruiter_id = ?", (user_id,))
        couriers_sum = c.fetchone()[0] or 0
        
        c.execute("SELECT SUM(amount) FROM withdrawals WHERE user_id = ? AND status = 'completed'", (user_id,))
        withdrawals_sum = c.fetchone()[0] or 0
        
        real_balance = couriers_sum - withdrawals_sum
        
        c.execute("UPDATE users SET balance = ? WHERE user_id = ?", (real_balance, user_id))
        conn.commit()
        
        c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (user_id,))
        user = c.fetchone()
        
        await update.message.reply_text(
            f"✅ *Баланс исправлен!*\n\n"
            f"👤 Пользователь: {user[1]} (@{user[0]})\n"
            f"💰 Сумма курьеров: {couriers_sum} руб.\n"
            f"💰 Выведено: {withdrawals_sum} руб.\n"
            f"💰 Новый баланс: {real_balance} руб.",
            parse_mode='Markdown'
        )
    
    elif context.args and context.args[0].lower() == 'all':
        status_msg = await update.message.reply_text("🔄 Пересчитываю балансы для всех пользователей...")
        
        c.execute("SELECT user_id FROM users")
        users = c.fetchall()
        
        count = 0
        for user in users:
            user_id = user[0]
            
            c.execute("SELECT SUM(balance) FROM couriers WHERE recruiter_id = ?", (user_id,))
            couriers_sum = c.fetchone()[0] or 0
            
            c.execute("SELECT SUM(amount) FROM withdrawals WHERE user_id = ? AND status = 'completed'", (user_id,))
            withdrawals_sum = c.fetchone()[0] or 0
            
            real_balance = couriers_sum - withdrawals_sum
            
            c.execute("UPDATE users SET balance = ? WHERE user_id = ?", (real_balance, user_id))
            count += 1
        
        conn.commit()
        
        await status_msg.edit_text(
            f"✅ *Балансы пересчитаны для {count} пользователей!*",
            parse_mode='Markdown'
        )
    
    else:
        admin_id = update.effective_user.id
        c.execute("SELECT SUM(balance) FROM couriers WHERE recruiter_id = ?", (admin_id,))
        couriers_sum = c.fetchone()[0] or 0
        
        c.execute("SELECT SUM(amount) FROM withdrawals WHERE user_id = ? AND status = 'completed'", (admin_id,))
        withdrawals_sum = c.fetchone()[0] or 0
        
        real_balance = couriers_sum - withdrawals_sum
        
        c.execute("SELECT full_name, city, balance FROM couriers WHERE recruiter_id = ?", (admin_id,))
        couriers = c.fetchall()
        
        c.execute("SELECT amount, status, request_date FROM withdrawals WHERE user_id = ? ORDER BY request_date DESC LIMIT 5", (admin_id,))
        withdrawals = c.fetchall()
        
        text = f"💰 *Твой текущий баланс:* {real_balance} руб.\n\n"
        text += f"📊 *Детали:*\n"
        text += f"• Сумма курьеров: {couriers_sum} руб.\n"
        text += f"• Выведено: {withdrawals_sum} руб.\n\n"
        
        text += "📋 *Твои курьеры:*\n"
        if couriers:
            for name, city, bal in couriers:
                text += f"• {name} ({city}) - {bal} руб.\n"
        else:
            text += "• Нет курьеров\n"
        
        if withdrawals:
            text += "\n📊 *Последние выводы:*\n"
            for amount, status, date in withdrawals:
                emoji = "✅" if status == 'completed' else "❌" if status == 'rejected' else "⏳"
                text += f"• {emoji} {amount} руб. - {date[:10]}\n"
        
        text += "\n*Использование:*\n"
        text += "`/fixbalance ID` - исправить баланс конкретного пользователя\n"
        text += "`/fixbalance all` - исправить балансы всех пользователей"
        
        await update.message.reply_text(text, parse_mode='Markdown')

async def admin_user_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает детальный баланс пользователя"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора")
        return
    
    if not context.args:
        await update.message.reply_text(
            "📝 Использование: `/userbalance user_id`\n"
            "Пример: `/userbalance 860845946`",
            parse_mode='Markdown'
        )
        return
    
    try:
        user_id = int(context.args[0])
        
        conn = get_db()
        c = conn.cursor()
        
        c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (user_id,))
        user = c.fetchone()
        
        if not user:
            await update.message.reply_text("❌ Пользователь не найден")
            return
        
        username, first_name = user
        
        c.execute("SELECT SUM(balance) FROM couriers WHERE recruiter_id = ?", (user_id,))
        couriers_sum = c.fetchone()[0] or 0
        
        c.execute("SELECT SUM(amount) FROM withdrawals WHERE user_id = ? AND status = 'completed'", (user_id,))
        withdrawals_sum = c.fetchone()[0] or 0
        
        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        current_balance = c.fetchone()[0] or 0
        
        real_balance = couriers_sum - withdrawals_sum
        
        c.execute("SELECT full_name, city, balance FROM couriers WHERE recruiter_id = ?", (user_id,))
        couriers = c.fetchall()
        
        c.execute("SELECT amount, status, request_date FROM withdrawals WHERE user_id = ? ORDER BY request_date DESC LIMIT 5", (user_id,))
        withdrawals = c.fetchall()
        
        text = (
            f"👤 *Пользователь:* {first_name} (@{username})\n"
            f"🆔 *ID:* {user_id}\n\n"
            f"💰 *Детальный баланс:*\n"
            f"• Сумма курьеров: {couriers_sum} руб.\n"
            f"• Выведено: {withdrawals_sum} руб.\n"
            f"• Реальный баланс: {real_balance} руб.\n"
            f"• Баланс в БД: {current_balance} руб.\n\n"
        )
        
        if couriers:
            text += "📋 *Курьеры:*\n"
            for name, city, bal in couriers:
                text += f"  • {name} ({city}) - {bal} руб.\n"
        else:
            text += "📋 *Курьеры:* нет\n"
        
        if withdrawals:
            text += "\n📊 *Последние выводы:*\n"
            for amount, status, date in withdrawals:
                emoji = "✅" if status == 'completed' else "❌" if status == 'rejected' else "⏳"
                text += f"  {emoji} {amount} руб. - {date[:10]}\n"
        
        await update.message.reply_text(text, parse_mode='Markdown')
        
    except ValueError:
        await update.message.reply_text("❌ Неверный ID пользователя")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

async def admin_fix_my_couriers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Исправляет только курьеров, созданных через бота"""
    if not is_admin(update.effective_user.id):
        return
    
    try:
        conn = get_db()
        c = conn.cursor()
        
        admin_id = update.effective_user.id
        
        c.execute('''
            UPDATE couriers 
            SET recruiter_id = ? 
            WHERE recruiter_id IS NULL 
            AND sheet_row IS NOT NULL
        ''', (admin_id,))
        
        updated = c.rowcount
        conn.commit()
        
        await update.message.reply_text(
            f"✅ Исправлено {updated} курьеров!\n"
            f"Теперь они привязаны к твоему ID ({admin_id})"
        )
        
        await admin_check_couriers(update, context)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def admin_fix_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Исправляет фейковых пользователей на реальные данные"""
    if not is_admin(update.effective_user.id):
        return
    
    try:
        conn = get_db()
        c = conn.cursor()
        
        admin_id = update.effective_user.id
        admin_username = update.effective_user.username or "admin"
        admin_first_name = update.effective_user.first_name or "Admin"
        
        c.execute('''
            UPDATE users 
            SET username = ?, first_name = ? 
            WHERE user_id = ?
        ''', (admin_username, admin_first_name, admin_id))
        
        updated = c.rowcount
        conn.commit()
        
        await update.message.reply_text(
            f"✅ Исправлен пользователь с ID {admin_id}\n"
            f"Теперь username: @{admin_username}, имя: {admin_first_name}"
        )
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def test_google(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовая команда для проверки Google Sheets"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для админа")
        return
    
    await update.message.reply_text("🔄 Тестирую подключение к Google Sheets...")
    
    try:
        creds_json = os.environ.get('GOOGLE_CREDS_JSON')
        sheet_id = os.environ.get('GOOGLE_SHEET_ID')
        
        if not creds_json:
            await update.message.reply_text("❌ GOOGLE_CREDS_JSON не найдена")
            return
        
        if not sheet_id:
            await update.message.reply_text("❌ GOOGLE_SHEET_ID не найдена")
            return
        
        await update.message.reply_text(f"✅ Переменные найдены\nID таблицы: {sheet_id}")
        
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        
        creds_dict = json.loads(creds_json)
        await update.message.reply_text("✅ JSON распарсен успешно")
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        await update.message.reply_text("✅ Credentials созданы")
        
        client = gspread.authorize(creds)
        await update.message.reply_text("✅ Авторизация в Google Sheets успешна")
        
        sheet = client.open_by_key(sheet_id).sheet1
        await update.message.reply_text("✅ Таблица открыта успешно")
        
        withdrawals_sheet = get_withdrawals_sheet()
        if withdrawals_sheet:
            await update.message.reply_text("✅ Лист 'Выводы' доступен")
        else:
            await update.message.reply_text("⚠️ Проблема с листом 'Выводы'")
        
        test_row = [
            datetime.now().strftime("%d.%m.%Y %H:%M"),
            "ТЕСТ",
            "@test",
            "Тестовый Курьер",
            "Тест-город",
            "⏳ Ожидает",
            "",
            ""
        ]
        sheet.append_row(test_row)
        await update.message.reply_text("✅ Тестовая запись успешно добавлена в таблицу!")
            
    except Exception as e:
        await update.message.reply_text(f"❌ Общая ошибка: {str(e)}")

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Рассылка сообщений всем пользователям"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас нет прав администратора")
        return
    
    if not context.args:
        await update.message.reply_text(
            "📢 *Использование:*\n"
            "`/broadcast Текст рассылки`\n\n"
            "Пример: `/broadcast Внимание! Важное объявление...`",
            parse_mode='Markdown'
        )
        return
    
    message_text = ' '.join(context.args)
    
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id FROM users")
    users = c.fetchall()
    
    if not users:
        await update.message.reply_text("📭 Нет пользователей для рассылки")
        return
    
    status_msg = await update.message.reply_text(f"🔄 Начинаю рассылку {len(users)} пользователям...")
    
    success_count = 0
    fail_count = 0
    
    for user in users:
        user_id = user[0]
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"📢 *Новое сообщение:*\n\n{message_text}",
                parse_mode='Markdown'
            )
            success_count += 1
        except Exception as e:
            fail_count += 1
            logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
        
        await asyncio.sleep(0.05)
    
    await status_msg.edit_text(
        f"✅ *Рассылка завершена!*\n\n"
        f"📊 *Статистика:*\n"
        f"• Успешно: {success_count}\n"
        f"• Ошибок: {fail_count}\n"
        f"• Всего: {len(users)}",
        parse_mode='Markdown'
    )

# ========== ЗАПУСК ==========
def main():
    # Инициализируем БД
    init_database()
    
    # Загружаем бэкап если есть
    load_backup()
    
    # Проверяем, что данные загрузились
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users")
    users_count = c.fetchone()[0]
    logger.info(f"👥 После загрузки бэкапа в БД: {users_count} пользователей")
    
    # Запускаем автосохранение и мониторинг
    start_auto_backup()
    start_sheet_monitoring()
    
    # Создаем приложение
    application = Application.builder().token(TOKEN).build()
    
    # Добавляем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CommandHandler("sync", admin_sync))
    application.add_handler(CommandHandler("checkdb", admin_check_db))
    application.add_handler(CommandHandler("couriers", admin_check_couriers))
    application.add_handler(CommandHandler("withdrawals", admin_withdrawals))
    application.add_handler(CommandHandler("tickets", admin_tickets))
    application.add_handler(CommandHandler("userbalance", admin_user_balance))
    application.add_handler(CommandHandler("fixbalance", admin_fix_balance))
    application.add_handler(CommandHandler("fixmy", admin_fix_my_couriers))
    application.add_handler(CommandHandler("fixusers", admin_fix_users))
    application.add_handler(CommandHandler("testgoogle", test_google))
    application.add_handler(CommandHandler("broadcast", admin_broadcast))
    application.add_handler(CommandHandler("restore", admin_restore))
    
    # Добавляем обработчики callback'ов
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Добавляем обработчик сообщений
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # ========== ЗАПУСК В РЕЖИМЕ POLLING ==========
    logger.info("🚀 Запускаем бот в режиме polling (ручное обновление)")
    
    # Запускаем polling
    application.run_polling()

if __name__ == '__main__':
    main()

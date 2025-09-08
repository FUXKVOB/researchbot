# bot.py
import asyncio
import os
import json
import time
import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Optional
import aiohttp
import logging
import backoff
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# Попытка импортировать reportlab для PDF
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    from reportlab.lib.units import inch
    REPORTLAB_AVAILABLE = True
    
    # Путь к файлу шрифта с поддержкой кириллицы
    FONT_PATH = "DejaVuSans.ttf"
    if os.path.exists(FONT_PATH):
        pdfmetrics.registerFont(TTFont('DejaVuSans', FONT_PATH))
        PDF_FONT_NAME = 'DejaVuSans'
    else:
        PDF_FONT_NAME = 'Helvetica'
        logging.warning("Файл шрифта DejaVuSans.ttf не найден. PDF будет генерироваться без поддержки кириллицы.")
except ImportError:
    REPORTLAB_AVAILABLE = False
    PDF_FONT_NAME = None

# Логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

DB_PATH = os.getenv('BOT_DB_PATH', 'bot_state.db')

# -----------------------
# Helpers for SQLite
# -----------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS researches (
        chat_id INTEGER PRIMARY KEY,
        topic TEXT,
        data TEXT,
        status TEXT,
        start_time REAL
    )
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS user_settings (
        chat_id INTEGER PRIMARY KEY,
        settings_json TEXT
    )
    ''')
    conn.commit()
    conn.close()

def save_research_to_db(chat_id: int, topic: str, data: dict, status: str, start_time: float):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('REPLACE INTO researches (chat_id, topic, data, status, start_time) VALUES (?,?,?,?,?)',
                (chat_id, topic, json.dumps(data, ensure_ascii=False), status, start_time))
    conn.commit()
    conn.close()

def delete_research_from_db(chat_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('DELETE FROM researches WHERE chat_id = ?', (chat_id,))
    conn.commit()
    conn.close()

def load_user_settings(chat_id: int) -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT settings_json FROM user_settings WHERE chat_id = ?', (chat_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    return {}

def save_user_settings(chat_id: int, settings: dict):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('REPLACE INTO user_settings (chat_id, settings_json) VALUES (?,?)',
                (chat_id, json.dumps(settings, ensure_ascii=False)))
    conn.commit()
    conn.close()

# -----------------------
# API clients
# -----------------------
class SerperAPI:
    """Класс для работы с Serper API"""
    def __init__(self, api_key: str, per_request_timeout: float = 15.0):
        self.api_key = api_key
        self.base_url = "https://google.serper.dev"
        self.per_request_timeout = per_request_timeout

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=3, factor=2)
    async def search(self, query: str, search_type: str = "search", num_results: int = 15) -> Dict[Any, Any]:
        headers = {
            'X-API-KEY': self.api_key,
            'Content-Type': 'application/json'
        }
        payload = {
            'q': query,
            'num': num_results,
            'hl': 'ru',
            'gl': 'ru'
        }
        try:
            timeout = aiohttp.ClientTimeout(total=self.per_request_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{self.base_url}/{search_type}", headers=headers, json=payload) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Serper API error status: {response.status}")
                        response.raise_for_status()
        except Exception as e:
            logger.error(f"Ошибка Serper search: {e}")
            raise

class MistralGenerator:
    """Класс для работы с Mistral API"""
    def __init__(self, api_key: str, per_request_timeout: float = 45.0):
        self.api_key = api_key
        self.base_url = "https://api.mistral.ai/v1/chat/completions"
        self.per_request_timeout = per_request_timeout

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=3)
    async def generate_report(self, findings: List[Dict[str, str]], topic: str, system_instructions: Optional[str] = None) -> str:
        findings_text = "\n\n".join([
            f"📌 **{f['title']}**\n"
            f"📝 {f['snippet']}\n"
            f"🔗 Источник [{f.get('_source_index','')}]: {f.get('link','')}"
            for f in findings[:20]  # Ограничиваем количество для лучшего качества
        ])
        
        system_prompt = system_instructions or (
            "Вы — эксперт-аналитик с глубокими знаниями в различных областях. "
            "Создавайте структурированные, информативные отчёты на основе предоставленных данных. "
            "Используйте академический стиль, избегайте рекламных фраз. "
            "Обязательно указывайте ссылки на источники в формате [1], [2] и т.д. "
            "Анализируйте тренды, выявляйте закономерности и делайте обоснованные выводы."
        )
        
        user_prompt = f"""
Создайте подробный аналитический отчёт по теме: "{topic}"

Структура отчёта:
1. 📋 **Краткое резюме** (2-3 предложения)
2. 🔍 **Детальный анализ** (основные аспекты и находки)
3. 📊 **Ключевые тренды и статистика**
4. ⚡ **Вызовы и проблемы**
5. 🚀 **Возможности и перспективы**
6. 💡 **Выводы и рекомендации**

Данные для анализа:
{findings_text}

Требования:
- Используйте только предоставленные данные
- Указывайте источники в квадратных скобках
- Выделяйте ключевые моменты
- Делайте выводы на основе фактов
"""
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        payload = {
            "model": "mistral-large-latest",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.3,
            "max_tokens": 4000,
        }
        try:
            timeout = aiohttp.ClientTimeout(total=self.per_request_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.base_url, headers=headers, json=payload) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if data and 'choices' in data and data['choices']:
                        choice = data['choices'][0]
                        if 'message' in choice and 'content' in choice['message']:
                            return choice['message']['content']
                        if 'text' in choice:
                            return choice['text']
                    return "❌ Не удалось получить ответ от Mistral API."
        except Exception as e:
            logger.error(f"Ошибка Mistral generate_report: {e}")
            raise

# -----------------------
# Main bot
# -----------------------
class ResearchBot:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.serper_api_key = os.getenv('SERPER_API_KEY')
        self.mistral_api_key = os.getenv('MISTRAL_API_KEY')
        self.max_results_default = int(os.getenv('MAX_RESULTS_PER_QUERY', 20))
        self.max_concurrent = int(os.getenv('MAX_CONCURRENT_SEARCHES', 4))
        self.deep_analysis_default = os.getenv('DEEP_ANALYSIS_ENABLED', 'true').lower() == 'true'
        self.default_lang = os.getenv('DEFAULT_LANG', 'ru')

        self.serper = SerperAPI(self.serper_api_key, per_request_timeout=float(os.getenv('SERPER_REQ_TIMEOUT', 15.0)))
        self.llm_generator = MistralGenerator(self.mistral_api_key, per_request_timeout=float(os.getenv('MISTRAL_REQ_TIMEOUT', 45.0)))

        self._tasks: Dict[int, asyncio.Task] = {}
        self.active_researches: Dict[int, Dict[str, Any]] = {}

        init_db()

    # ---------- Utilities ----------
    def _get_user_settings(self, chat_id: int) -> dict:
        settings = load_user_settings(chat_id)
        if not settings:
            settings = {
                'max_results': self.max_results_default,
                'deep_analysis': self.deep_analysis_default,
                'lang': self.default_lang
            }
            save_user_settings(chat_id, settings)
        return settings

    def _format_time(self, seconds: int) -> str:
        """Форматирование времени в читаемый вид"""
        if seconds < 60:
            return f"{seconds} сек"
        elif seconds < 3600:
            return f"{seconds // 60} мин {seconds % 60} сек"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours} ч {minutes} мин"

    # ---------- Bot handlers ----------
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        welcome_text = (
            "🔬 <b>Research Bot — Ваш персональный аналитик</b>\n\n"
            "🎯 <b>Что я умею:</b>\n"
            "• Провожу глубокие исследования по любой теме\n"
            "• Анализирую актуальную информацию из интернета\n"
            "• Создаю структурированные отчёты с источниками\n"
            "• Генерирую PDF-документы\n\n"
            "📋 <b>Основные команды:</b>\n"
            "🔍 /research <тема> — начать исследование\n"
            "📊 /status — статус текущего исследования\n"
            "❌ /cancel — отменить исследование\n"
            "⚙️ /settings — настройки бота\n"
            "📚 /sources — список источников\n"
            "❓ /help — подробная справка\n\n"
            "💡 <b>Просто отправьте мне тему для исследования!</b>"
        )
        await update.message.reply_text(welcome_text, parse_mode='HTML')

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = (
            "📖 <b>Подробная справка по Research Bot</b>\n\n"
            "🚀 <b>Быстрый старт:</b>\n"
            "1️⃣ Отправьте тему: <code>Искусственный интеллект в медицине</code>\n"
            "2️⃣ Следите за прогрессом в реальном времени\n"
            "3️⃣ Получите готовый отчёт в Markdown и PDF\n\n"
            "⚙️ <b>Настройки:</b>\n"
            "• <code>/settings sources 25</code> — количество источников (1-50)\n"
            "• <code>/settings depth on</code> — глубокий анализ (on/off)\n"
            "• <code>/settings lang en</code> — язык отчёта (ru/en)\n\n"
            "📊 <b>Дополнительные команды:</b>\n"
            "• <code>/status</code> — текущий прогресс\n"
            "• <code>/sources</code> — список найденных источников\n"
            "• <code>/cancel</code> — остановить исследование\n\n"
            "💡 <b>Советы:</b>\n"
            "• Формулируйте тему конкретно\n"
            "• Используйте ключевые слова\n"
            "• Один запрос = одно исследование"
        )
        await update.message.reply_text(help_text, parse_mode='HTML')

    async def settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        args = context.args
        current = self._get_user_settings(chat_id)
        
        if not args:
            depth_status = "включён" if current['deep_analysis'] else "выключен"
            text = (
                "⚙️ <b>Текущие настройки</b>\n\n"
                f"📊 <b>Количество источников:</b> {current['max_results']}\n"
                f"🔍 <b>Глубокий анализ:</b> {depth_status}\n"
                f"🌐 <b>Язык отчёта:</b> {current['lang'].upper()}\n\n"
                "📝 <b>Изменить настройки:</b>\n"
                "• <code>/settings sources 25</code>\n"
                "• <code>/settings depth on</code>\n"
                "• <code>/settings lang en</code>"
            )
            await update.message.reply_text(text, parse_mode='HTML')
            return

        if len(args) >= 2:
            key = args[0].lower()
            value = args[1].lower()
            
            if key in ['sources', 'source', 'max', 'max_results']:
                try:
                    val = int(value)
                    if val < 1 or val > 50:
                        await update.message.reply_text("❌ Количество источников должно быть от 1 до 50")
                        return
                    current['max_results'] = val
                    save_user_settings(chat_id, current)
                    await update.message.reply_text(f"✅ <b>Количество источников:</b> {val}", parse_mode='HTML')
                except ValueError:
                    await update.message.reply_text("❌ Укажите число от 1 до 50\n<b>Пример:</b> <code>/settings sources 25</code>", parse_mode='HTML')
                    
            elif key in ['depth', 'deep', 'analysis', 'deep_analysis']:
                if value in ['on', 'true', '1', 'yes', 'вкл']:
                    current['deep_analysis'] = True
                    status = "включён"
                elif value in ['off', 'false', '0', 'no', 'выкл']:
                    current['deep_analysis'] = False
                    status = "выключен"
                else:
                    await update.message.reply_text("❌ Используйте <code>on</code> или <code>off</code>\n<b>Пример:</b> <code>/settings depth on</code>", parse_mode='HTML')
                    return
                save_user_settings(chat_id, current)
                
            elif key in ['lang', 'language']:
                if value not in ['ru', 'en']:
                    await update.message.reply_text("❌ Поддерживаемые языки: <code>ru</code>, <code>en</code>\n<b>Пример:</b> <code>/settings lang en</code>", parse_mode='HTML')
                    return
                current['lang'] = value
                save_user_settings(chat_id, current)
                await update.message.reply_text(f"✅ <b>Язык отчёта:</b> {value.upper()}", parse_mode='HTML')
            else:
                await update.message.reply_text("❌ Неизвестная настройка. Используйте: <code>sources</code>, <code>depth</code>, <code>lang</code>", parse_mode='HTML')
        else:
            await update.message.reply_text("❌ Укажите параметр и значение\n<b>Пример:</b> <code>/settings sources 25</code>", parse_mode='HTML')

    async def research_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text(
                "❌ <b>Укажите тему для исследования</b>\n\n"
                "📝 <b>Пример:</b> <code>/research искусственный интеллект в медицине</code>",
                parse_mode='HTML'
            )
            return
        topic = " ".join(context.args)
        await self.start_research(update, topic)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        topic = (update.message.text or "").strip()
        if not topic or topic.startswith('/'):
            return
        if len(topic) < 5:
            await update.message.reply_text(
                "❌ <b>Тема слишком короткая</b>\n\n"
                "💡 Опишите тему подробнее (минимум 5 символов)\n"
                "📝 <b>Пример:</b> <i>Развитие квантовых компьютеров</i>",
                parse_mode='HTML'
            )
            return
        await self.start_research(update, topic)

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        if chat_id in self.active_researches:
            r = self.active_researches[chat_id]
            elapsed = int(time.time() - r['start_time'])
            status_emoji = {
                'running': '🔄',
                'done': '✅',
                'cancelled': '❌',
                'error': '⚠️'
            }
            emoji = status_emoji.get(r.get('status', 'running'), '🔄')
            text = (
                f"{emoji} <b>Статус исследования</b>\n\n"
                f"📋 <b>Тема:</b> {r['topic']}\n"
                f"⏱ <b>Время:</b> {self._format_time(elapsed)}\n"
                f"📊 <b>Статус:</b> {r.get('status', 'выполняется')}"
            )
        else:
            text = "📭 <b>Нет активных исследований</b>\n\n💡 Отправьте тему для начала нового исследования"
        await update.message.reply_text(text, parse_mode='HTML')

    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        if chat_id in self._tasks:
            task = self._tasks[chat_id]
            task.cancel()
            if chat_id in self.active_researches:
                self.active_researches[chat_id]['status'] = 'cancelled'
                save_research_to_db(
                    chat_id, 
                    self.active_researches[chat_id]['topic'], 
                    self.active_researches[chat_id], 
                    'cancelled', 
                    self.active_researches[chat_id]['start_time']
                )
            del self._tasks[chat_id]
            await update.message.reply_text("❌ <b>Исследование отменено</b>", parse_mode='HTML')
        else:
            await update.message.reply_text("⚠️ <b>Нет активного исследования для отмены</b>", parse_mode='HTML')

    async def sources_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        if chat_id not in self.active_researches:
            await update.message.reply_text(
                "📭 <b>Нет данных об источниках</b>\n\n"
                "💡 Сначала проведите исследование",
                parse_mode='HTML'
            )
            return
            
        research = self.active_researches[chat_id]
        sources = research.get('sources_list', [])
        if not sources:
            await update.message.reply_text("📭 <b>Источники пока не найдены</b>", parse_mode='HTML')
            return
            
        out_lines = [f"📚 <b>Источники по теме:</b> {research['topic']}\n"]
        for idx, s in enumerate(sources[:30], start=1):  # Ограничиваем до 30 источников
            title = s.get('title', 'Без названия')[:80]
            link = s.get('link', '')
            out_lines.append(f"{idx}. {title}\n🔗 {link}\n")
            
        txt = "\n".join(out_lines)
        fname = f"sources_{research['topic'][:20].replace(' ', '_')}_{int(time.time())}.txt"
        
        await update.effective_chat.send_document(
            document=txt.encode('utf-8'), 
            filename=fname, 
            caption="📚 <b>Список источников исследования</b>",
            parse_mode='HTML'
        )

    # ---------- Research flow ----------
    async def start_research(self, update: Update, topic: str):
        chat_id = update.effective_chat.id
        if chat_id in self._tasks:
            await update.message.reply_text(
                "⚠️ <b>У вас уже есть активное исследование</b>\n\n"
                "🔄 Дождитесь завершения или отмените командой /cancel",
                parse_mode='HTML'
            )
            return

        settings = self._get_user_settings(chat_id)
        
        start_msg = await update.message.reply_text(
            f"🔬 <b>Запускаю исследование</b>\n\n"
            f"📋 <b>Тема:</b> {topic}\n"
            f"📊 <b>Источников:</b> до {settings['max_results']}\n"
            f"🔍 <b>Глубокий анализ:</b> {'включен' if settings['deep_analysis'] else 'выключен'}\n\n"
            "⏳ <i>Подготавливаю поисковые запросы...</i>",
            parse_mode='HTML'
        )
        
        progress_message_id = start_msg.message_id
        metadata = {
            'topic': topic,
            'progress_message_id': progress_message_id,
            'start_time': time.time(),
            'status': 'running',
            'settings': settings
        }
        self.active_researches[chat_id] = metadata
        save_research_to_db(chat_id, topic, metadata, 'running', metadata['start_time'])

        task = asyncio.create_task(self._research_task_runner(chat_id, topic, progress_message_id, settings))
        self._tasks[chat_id] = task

    async def _research_task_runner(self, chat_id: int, topic: str, progress_message_id: int, settings: dict):
        try:
            results = await self._run_research_logic(topic, chat_id, progress_message_id, settings)
            md_text = self._build_report_markdown(results)
            results['full_report_text_md'] = md_text
            results['sources_list'] = results.get('sources', [])

            self.active_researches[chat_id].update({
                'status': 'done',
                'completed_time': int(time.time() - self.active_researches[chat_id]['start_time']),
                'full_report_text_md': md_text,
                'sources_list': results['sources_list']
            })
            save_research_to_db(
                chat_id, topic, self.active_researches[chat_id], 'done', 
                self.active_researches[chat_id]['start_time']
            )

            # Финальное сообщение о завершении
            completion_time = self._format_time(self.active_researches[chat_id]['completed_time'])
            try:
                await self.application.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_message_id,
                    text=(
                        f"✅ <b>Исследование завершено!</b>\n\n"
                        f"📋 <b>Тема:</b> {topic}\n"
                        f"⏱ <b>Время выполнения:</b> {completion_time}\n"
                        f"📊 <b>Найдено источников:</b> {len(results['sources_list'])}\n\n"
                        "📄 <i>Отправляю отчёт...</i>"
                    ),
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.warning(f"Не удалось обновить сообщение о прогрессе: {e}")

            # Отправка отчёта
            md_filename = f"report_{topic.replace(' ','_')[:40]}_{int(time.time())}.md"
            await self.application.bot.send_document(
                chat_id=chat_id,
                document=md_text.encode('utf-8'),
                filename=md_filename,
                caption=(
                    f"📋 <b>Исследовательский отчёт</b>\n\n"
                    f"📝 <b>Тема:</b> {topic}\n"
                    f"📊 <b>Источников:</b> {len(results['sources_list'])}\n"
                    f"⏱ <b>Время:</b> {completion_time}"
                ),
                parse_mode='HTML'
            )

            # Отправка PDF если доступно
            if REPORTLAB_AVAILABLE:
                try:
                    pdf_bytes = self._render_pdf_bytes(topic, md_text)
                    pdf_filename = f"report_{topic.replace(' ','_')[:40]}_{int(time.time())}.pdf"
                    await self.application.bot.send_document(
                        chat_id=chat_id,
                        document=pdf_bytes,
                        filename=pdf_filename,
                        caption="📄 <b>PDF версия отчёта</b>",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"Ошибка генерации PDF: {e}")
                    await self.application.bot.send_message(
                        chat_id=chat_id,
                        text="⚠️ <b>Не удалось создать PDF версию</b>\nИспользуйте Markdown файл",
                        parse_mode='HTML'
                    )
            else:
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text="ℹ️ <b>PDF генерация недоступна</b>\nУстановите библиотеку reportlab для создания PDF",
                    parse_mode='HTML'
                )

        except asyncio.CancelledError:
            logger.info(f"Research task cancelled for {chat_id}")
            if chat_id in self.active_researches:
                self.active_researches[chat_id]['status'] = 'cancelled'
                save_research_to_db(
                    chat_id, topic, self.active_researches[chat_id], 'cancelled', 
                    self.active_researches[chat_id]['start_time']
                )
            try:
                await self.application.bot.send_message(
                    chat_id=chat_id, 
                    text="❌ <b>Исследование отменено пользователем</b>",
                    parse_mode='HTML'
                )
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Ошибка при исследовании: {e}", exc_info=True)
            try:
                await self.application.bot.send_message(
                    chat_id=chat_id, 
                    text=f"❌ <b>Ошибка при выполнении исследования</b>\n\n<code>{str(e)}</code>",
                    parse_mode='HTML'
                )
            except Exception:
                pass
            if chat_id in self.active_researches:
                self.active_researches[chat_id]['status'] = 'error'
                save_research_to_db(
                    chat_id, topic, self.active_researches[chat_id], 'error', 
                    self.active_researches[chat_id]['start_time']
                )
        finally:
            if chat_id in self._tasks:
                del self._tasks[chat_id]

    async def _run_research_logic(self, topic: str, chat_id: int, progress_message_id: int, settings: dict) -> dict:
        results = {
            'topic': topic,
            'searches': [],
            'key_findings': [],
            'sources': [],
            'timestamp': datetime.now().isoformat()
        }
        
        # Генерируем улучшенные поисковые запросы
        queries = await self.generate_search_queries(topic, settings)
        total_steps = len(queries) + 3
        current_step = 0

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def search_with_semaphore(query):
            async with semaphore:
                try:
                    resp = await asyncio.wait_for(
                        self.serper.search(query, num_results=settings['max_results']), 
                        timeout=self.serper.per_request_timeout + 5
                    )
                    return resp
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout для запроса: {query}")
                    return {}
                except Exception as e:
                    logger.error(f"Ошибка поиска для запроса {query}: {e}")
                    return {}

        # Выполняем поиски батчами
        for i in range(0, len(queries), self.max_concurrent):
            batch = queries[i:i + self.max_concurrent]
            tasks = [asyncio.create_task(search_with_semaphore(q)) for q in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for j, res in enumerate(batch_results):
                current_step += 1
                query_name = batch[j][:50] + "..." if len(batch[j]) > 50 else batch[j]
                await self._update_progress(
                    chat_id, progress_message_id, current_step, total_steps, 
                    f"🔍 Поиск: {query_name}"
                )
                
                if isinstance(res, dict) and res.get('organic'):
                    r_items = res.get('organic', [])[:settings['max_results']]
                    results['searches'].append({'query': batch[j], 'results': r_items})
                    
                    for item in r_items:
                        if item.get('snippet') and len(item.get('snippet', '')) > 20:
                            src_index = len(results['sources']) + 1
                            results['key_findings'].append({
                                'title': item.get('title', 'Без названия'),
                                'snippet': item.get('snippet', ''),
                                'link': item.get('link', ''),
                                '_source_index': src_index
                            })
                            results['sources'].append({
                                'title': item.get('title', 'Без названия'), 
                                'link': item.get('link', '')
                            })
                
                await asyncio.sleep(0.3)  # Небольшая пауза между запросами
            
            await asyncio.sleep(1.5)  # Пауза между батчами

        # Фильтруем и улучшаем качество данных
        current_step += 1
        await self._update_progress(
            chat_id, progress_message_id, current_step, total_steps, 
            "📊 Обработка и фильтрация данных"
        )
        
        # Удаляем дубликаты и низкокачественные результаты
        unique_findings = []
        seen_titles = set()
        for finding in results['key_findings']:
            title_lower = finding['title'].lower()
            if title_lower not in seen_titles and len(finding['snippet']) > 30:
                seen_titles.add(title_lower)
                unique_findings.append(finding)
        
        results['key_findings'] = unique_findings[:25]  # Ограничиваем для лучшего качества
        await asyncio.sleep(1)

        # Генерация отчёта с помощью LLM
        current_step += 1
        await self._update_progress(
            chat_id, progress_message_id, current_step, total_steps, 
            "🧠 Генерация аналитического отчёта"
        )
        
        try:
            report_text = await asyncio.wait_for(
                self.llm_generator.generate_report(results['key_findings'], topic),
                timeout=self.llm_generator.per_request_timeout + 10
            )
        except asyncio.TimeoutError:
            logger.warning("Timeout при генерации отчёта")
            report_text = "⚠️ Превышено время ожидания ответа от AI. Попробуйте позже или упростите тему."
        except Exception as e:
            logger.error(f"Ошибка генерации отчёта: {e}")
            report_text = f"⚠️ Ошибка при создании отчёта: {str(e)}"

        results['full_report_text'] = report_text

        # Финализация
        current_step += 1
        await self._update_progress(
            chat_id, progress_message_id, current_step, total_steps, 
            "📄 Подготовка итогового отчёта"
        )
        await asyncio.sleep(0.5)

        return results

    async def generate_search_queries(self, topic: str, settings: dict) -> List[str]:
        """Генерирует улучшенные поисковые запросы"""
        base_queries = [
            f"{topic} обзор 2025",
            f"{topic} исследование анализ",
            f"{topic} статистика данные тренды",
            f"{topic} развитие перспективы",
            f"{topic} проблемы вызовы решения",
            f"{topic} инновации технологии",
            f"{topic} рынок прогнозы",
            f"{topic} экспертное мнение аналитика"
        ]
        
        if settings.get('deep_analysis'):
            deep_queries = [
                f"{topic} case study практические примеры",
                f"{topic} лучшие практики опыт",
                f"{topic} исследования университетов",
                f"{topic} отчёты консалтинговых компаний",
                f"{topic} белые книги whitepaper",
                f"{topic} научные публикации"
            ]
            base_queries.extend(deep_queries)
        
        # Добавляем специфичные запросы в зависимости от темы
        topic_lower = topic.lower()
        if any(word in topic_lower for word in ['технология', 'tech', 'ии', 'ai', 'блокчейн', 'искусственный интеллект']):
            base_queries.extend([
                f"{topic} внедрение применение",
                f"{topic} стартапы компании лидеры"
            ])
        elif any(word in topic_lower for word in ['медицина', 'здоровье', 'лечение']):
            base_queries.extend([
                f"{topic} клинические исследования",
                f"{topic} эффективность результаты"
            ])
        elif any(word in topic_lower for word in ['экономика', 'финансы', 'бизнес']):
            base_queries.extend([
                f"{topic} экономический эффект",
                f"{topic} инвестиции рынок"
            ])
        
        return base_queries[:16]  # Ограничиваем количество запросов

    async def _update_progress(self, chat_id: int, message_id: int, step: int, total: int, current_step_name: str):
        """Обновляет прогресс выполнения с улучшенной визуализацией"""
        pct = min(100, int(step * 100 / max(1, total)))
        
        # Создаём красивый прогресс-бар
        filled_blocks = int(pct / 5)
        empty_blocks = 20 - filled_blocks
        progress_bar = "🟩" * filled_blocks + "⬜" * empty_blocks
        
        # Эмодзи для разных этапов
        if "Поиск" in current_step_name:
            emoji = "🔍"
        elif "Обработка" in current_step_name or "фильтрация" in current_step_name:
            emoji = "📊"
        elif "Генерация" in current_step_name or "отчёт" in current_step_name:
            emoji = "🧠"
        elif "Подготовка" in current_step_name:
            emoji = "📄"
        else:
            emoji = "⚙️"
        
        text = (
            f"{emoji} <b>Исследование выполняется</b>\n\n"
            f"📋 <b>Текущий этап:</b>\n{current_step_name}\n\n"
            f"📊 <b>Прогресс:</b> {pct}% ({step}/{total})\n"
            f"{progress_bar}\n\n"
            f"⏱ <i>Примерное время: {max(1, (total-step)*15)} сек</i>"
        )
        
        try:
            await self.application.bot.edit_message_text(
                chat_id=chat_id, 
                message_id=message_id, 
                text=text, 
                parse_mode='HTML'
            )
        except Exception as e:
            # Если не удалось отредактировать, отправляем новое сообщение
            try:
                await self.application.bot.send_message(
                    chat_id=chat_id, 
                    text=f"{emoji} {current_step_name} — {pct}%"
                )
            except Exception:
                logger.debug("Не удалось отправить уведомление о прогрессе")

    def _build_report_markdown(self, results: dict) -> str:
        """Создаёт улучшенный Markdown отчёт"""
        md = f"# 📊 Исследовательский отчёт: {results.get('topic','')}\n\n"
        md += f"**📅 Дата создания:** {datetime.fromisoformat(results.get('timestamp')).strftime('%d.%m.%Y %H:%M')}\n"
        md += f"**📊 Источников проанализировано:** {len(results.get('sources', []))}\n"
        md += f"**🔍 Ключевых находок:** {len(results.get('key_findings', []))}\n\n"
        
        md += "---\n\n"
        
        # Основной отчёт от LLM
        md += "## 🎯 Аналитический отчёт\n\n"
        md += results.get('full_report_text', 'Отчёт не сгенерирован') + "\n\n"
        
        md += "---\n\n"
        
        # Ключевые находки с улучшенным форматированием
        md += "## 🔍 Детальные находки\n\n"
        for i, kf in enumerate(results.get('key_findings', [])[:20], start=1):
            src_idx = kf.get('_source_index', i)
            title = kf.get('title', 'Без названия')
            snippet = kf.get('snippet', '')
            
            md += f"### {i}. {title}\n\n"
            md += f"**Описание:** {snippet}\n\n"
            md += f"**Источник:** [{src_idx}] {kf.get('link', '')}\n\n"
            md += "---\n\n"
        
        # Список источников
        md += "## 📚 Источники\n\n"
        for idx, s in enumerate(results.get('sources', []), start=1):
            title = s.get('title', 'Без названия')
            link = s.get('link', '')
            md += f"{idx}. **{title}**  \n   🔗 [{link}]({link})\n\n"
        
        # Метаинформация
        md += "---\n\n"
        md += "## ℹ️ Информация о создании\n\n"
        md += f"- **Поисковых запросов выполнено:** {len(results.get('searches', []))}\n"
        md += f"- **Уникальных источников найдено:** {len(results.get('sources', []))}\n"
        md += f"- **Время создания:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
        md += f"- **Генератор:** Research Bot v2.0\n\n"
        
        return md

    def _render_pdf_bytes(self, title: str, md_text: str) -> bytes:
        """Создаёт улучшенный PDF с поддержкой кириллицы"""
        from io import BytesIO
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
        from reportlab.lib.units import inch
        
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=1*inch)
        
        # Стили
        styles = getSampleStyleSheet()
        title_style = styles['Title']
        heading_style = styles['Heading1']
        normal_style = styles['Normal']
        
        # Устанавливаем шрифт с поддержкой кириллицы
        if PDF_FONT_NAME and PDF_FONT_NAME != 'Helvetica':
            try:
                title_style.fontName = PDF_FONT_NAME
                heading_style.fontName = PDF_FONT_NAME
                normal_style.fontName = PDF_FONT_NAME
            except Exception as e:
                logger.warning(f"Не удалось установить шрифт {PDF_FONT_NAME}: {e}")
        
        story = []
        
        # Заголовок
        story.append(Paragraph(f"Исследовательский отчёт: {title}", title_style))
        story.append(Spacer(1, 0.2*inch))
        
        # Обрабатываем markdown текст
        lines = md_text.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                story.append(Spacer(1, 0.1*inch))
            elif line.startswith('# '):
                story.append(Paragraph(line[2:], title_style))
            elif line.startswith('## '):
                story.append(Paragraph(line[3:], heading_style))
            elif line.startswith('### '):
                story.append(Paragraph(line[4:], heading_style))
            else:
                # Убираем markdown разметку для PDF
                clean_line = line.replace('**', '').replace('*', '').replace('`', '')
                if len(clean_line) > 0:
                    try:
                        story.append(Paragraph(clean_line, normal_style))
                    except Exception as e:
                        # Если не удалось добавить строку, пропускаем её
                        logger.debug(f"Пропущена строка в PDF: {e}")
        
        doc.build(story)
        buffer.seek(0)
        return buffer.read()

    # ---------- Run ----------
    def run(self):
        if not self.token:
            logger.error("❌ Отсутствует TELEGRAM_BOT_TOKEN в переменных окружения")
            exit(1)
        if not self.serper_api_key:
            logger.error("❌ Отсутствует SERPER_API_KEY в переменных окружения")
            exit(1)
        if not self.mistral_api_key:
            logger.error("❌ Отсутствует MISTRAL_API_KEY в переменных окружения")
            exit(1)
            
        self.application = Application.builder().token(self.token).build()
        
        # Добавляем обработчик ошибок
        async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
            logger.error("Exception while handling an update:", exc_info=context.error)
            if update and hasattr(update, 'effective_chat') and update.effective_chat:
                try:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text="❌ <b>Произошла ошибка</b>\n\nПопробуйте позже или обратитесь к администратору.",
                        parse_mode='HTML'
                    )
                except Exception:
                    pass
        
        self.application.add_error_handler(error_handler)
        
        # Регистрируем обработчики
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("research", self.research_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("cancel", self.cancel_command))
        self.application.add_handler(CommandHandler("settings", self.settings_command))
        self.application.add_handler(CommandHandler("sources", self.sources_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        logger.info("🚀 Research Bot v2.0 запущен успешно!")
        print("🚀 Research Bot v2.0 запущен! Нажмите Ctrl+C для остановки.")
        
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    required_vars = ['TELEGRAM_BOT_TOKEN', 'SERPER_API_KEY', 'MISTRAL_API_KEY']
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        print(f"❌ Отсутствуют обязательные переменные окружения: {', '.join(missing)}")
        print("\n📝 Создайте файл .env со следующими переменными:")
        for var in missing:
            print(f"{var}=your_api_key_here")
        exit(1)
        
    bot = ResearchBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен пользователем")
    except Exception as e:
        logger.error("Критическая ошибка: %s", e, exc_info=True)
        print(f"💥 Критическая ошибка: {e}")
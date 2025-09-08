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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler

# Попытка импортировать reportlab для PDF; если не установлен, просто будем отдавать .md
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    REPORTLAB_AVAILABLE = True
    # --- ДОБАВЛЕНИЕ ---
    # Путь к файлу шрифта с поддержкой кириллицы (например, DejaVuSans.ttf)
    FONT_PATH = "DejaVuSans.ttf"
    if os.path.exists(FONT_PATH):
        pdfmetrics.registerFont(TTFont('DejaVuSans', FONT_PATH))
        PDF_FONT_NAME = 'DejaVuSans'
    else:
        # Если шрифт не найден, возвращаемся к стандартному, но это вызовет "квадраты"
        PDF_FONT_NAME = 'Helvetica'
        logging.warning("Файл шрифта DejaVuSans.ttf не найден. PDF будет генерироваться без поддержки кириллицы.")
except Exception:
    REPORTLAB_AVAILABLE = False
    PDF_FONT_NAME = None # Для совместимости

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
    def __init__(self, api_key: str, per_request_timeout: float = 10.0):
        self.api_key = api_key
        self.base_url = "https://google.serper.dev"
        self.per_request_timeout = per_request_timeout

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=5, factor=2)
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
    def __init__(self, api_key: str, per_request_timeout: float = 30.0):
        self.api_key = api_key
        self.base_url = "https://api.mistral.ai/v1/chat/completions"
        self.per_request_timeout = per_request_timeout

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=5)
    async def generate_report(self, findings: List[Dict[str, str]], topic: str, system_instructions: Optional[str] = None) -> str:
        findings_text = "\n\n".join([f"Заголовок: {f['title']}\nОписание: {f['snippet']}\nИсточник индекс: [{f.get('_source_index','')}] ({f.get('link','')})"
                                     for f in findings])
        system_prompt = system_instructions or (
            "Вы — опытный аналитик. Пишите академическим языком, кратко и содержательно. "
            "Не используйте слоганы. Даёте ссылки на источники в виде [1], [2] и т.д. "
            "Не придумывайте факты — опирайтесь только на предоставленные находки."
        )
        user_prompt = f"""
На основе находок, составьте связный отчет по теме "{topic}". Разбейте на разделы:
1) Краткое описание
2) Основные выводы (bullet points)
3) Тренды и направления
4) Вызовы и возможности
5) Заключение

Ниже — данные:
{findings_text}
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
            "temperature": 0.2,
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
                    return "Не удалось получить ответ от Mistral API."
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
        self.max_results_default = int(os.getenv('MAX_RESULTS_PER_QUERY', 15))
        self.max_concurrent = int(os.getenv('MAX_CONCURRENT_SEARCHES', 3))
        self.deep_analysis_default = os.getenv('DEEP_ANALYSIS_ENABLED', 'true').lower() == 'true'
        self.default_lang = os.getenv('DEFAULT_LANG', 'ru')

        self.serper = SerperAPI(self.serper_api_key, per_request_timeout=float(os.getenv('SERPER_REQ_TIMEOUT', 10.0)))
        self.llm_generator = MistralGenerator(self.mistral_api_key, per_request_timeout=float(os.getenv('MISTRAL_REQ_TIMEOUT', 30.0)))

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

    # ---------- Bot handlers ----------
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        welcome_text = (
            "🔬 <b>Добро пожаловать в Research Bot!</b>\n\n"
            "Я помогу создать исследовательский отчёт по теме.\n\n"
            "<b>Команды:</b>\n"
            "/research &lt;тема&gt; - начать исследование\n"
            "/status - статус активного исследования\n"
            "/cancel - отменить активное исследование\n"
            "/settings - показать/изменить настройки (пример: /settings sources 10)\n"
            "/sources - получить список источников (текущего исследования)\n"
        )
        await update.message.reply_text(welcome_text, parse_mode='HTML')

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = (
            "🔍 <b>Как использовать:</b>\n"
            "1) /research &lt;тема&gt; — начать\n"
            "2) Проследить прогресс (обновления приходят в тот же чат)\n"
            "3) После завершения — получить отчёт и PDF\n\n"
            "Настройки:\n"
            "/settings sources &lt;число&gt;\n"
            "/settings depth on|off\n"
            "/settings lang ru|en\n"
        )
        await update.message.reply_text(help_text, parse_mode='HTML')

    async def settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        args = context.args
        current = self._get_user_settings(chat_id)
        if not args:
            text = f"⚙️ <b>Текущие настройки</b>:\n" \
                   f"Количество источников: {current['max_results']}\n" \
                   f"Глубокий анализ: {'вкл' if current['deep_analysis'] else 'выкл'}\n" \
                   f"Язык отчета: {current['lang']}\n\n" \
                   "Чтобы изменить, используйте:\n" \
                   "/settings sources 10\n" \
                   "/settings depth on\n" \
                   "/settings lang en"
            await update.message.reply_text(text, parse_mode='HTML')
            return

        if len(args) >= 2:
            key = args[0].lower()
            value = args[1].lower()
            if key in ['sources', 'source', 'max', 'max_results']:
                try:
                    val = int(value)
                    current['max_results'] = max(1, min(50, val))
                    save_user_settings(chat_id, current)
                    await update.message.reply_text(f"✅ Количество источников установлено: {current['max_results']}")
                except ValueError:
                    await update.message.reply_text("❌ Неверное число. Пример: /settings sources 10")
            elif key in ['depth', 'deep', 'analysis', 'deep_analysis']:
                if value in ['on', 'true', '1', 'yes']:
                    current['deep_analysis'] = True
                elif value in ['off', 'false', '0', 'no']:
                    current['deep_analysis'] = False
                else:
                    await update.message.reply_text("❌ Используйте on или off. Пример: /settings depth on")
                    return
                save_user_settings(chat_id, current)
                await update.message.reply_text(f"✅ Глубокий анализ: {'вкл' if current['deep_analysis'] else 'выкл'}")
            elif key in ['lang', 'language']:
                if value not in ['ru', 'en']:
                    await update.message.reply_text("❌ Поддерживаемые языки: ru, en. Пример: /settings lang en")
                    return
                current['lang'] = value
                save_user_settings(chat_id, current)
                await update.message.reply_text(f"✅ Язык отчета установлен: {value}")
            else:
                await update.message.reply_text("❌ Неизвестная настройка. Смотри /help")
        else:
            await update.message.reply_text("❌ Укажите параметр и значение. Пример: /settings sources 10")

    async def research_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("❌ Укажите тему: /research Искусственный интеллект в медицине")
            return
        topic = " ".join(context.args)
        await self.start_research(update, topic)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        topic = (update.message.text or "").strip()
        if not topic or topic.startswith('/'):
            return
        if len(topic) < 5:
            await update.message.reply_text("❌ Тема слишком короткая. Опишите подробнее (минимум 5 символов).")
            return
        await self.start_research(update, topic)

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        if chat_id in self.active_researches:
            r = self.active_researches[chat_id]
            elapsed = int(time.time() - r['start_time'])
            text = f"📊 <b>Активное исследование</b>\nТема: {r['topic']}\nВремя: {elapsed} сек\nСтатус: {r.get('status','running')}"
        else:
            text = "📭 У вас нет активных исследований."
        await update.message.reply_text(text, parse_mode='HTML')

    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        if chat_id in self._tasks:
            task = self._tasks[chat_id]
            task.cancel()
            if chat_id in self.active_researches:
                self.active_researches[chat_id]['status'] = 'cancelled'
                save_research_to_db(chat_id, self.active_researches[chat_id]['topic'], self.active_researches[chat_id], 'cancelled', self.active_researches[chat_id]['start_time'])
            del self._tasks[chat_id]
            await update.message.reply_text("❌ Исследование отменено.")
        else:
            await update.message.reply_text("⚠️ Нет активного исследования для отмены.")

    async def sources_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        if chat_id not in self.active_researches:
            await update.message.reply_text("📭 Нет активного исследования или нет сохранённых результатов.")
            return
        research = self.active_researches[chat_id]
        sources = research.get('sources_list', [])
        if not sources:
            await update.message.reply_text("📭 Источников пока нет.")
            return
        out_lines = []
        for idx, s in enumerate(sources, start=1):
            out_lines.append(f"{idx}. {s.get('title','')} - {s.get('link','')}")
        txt = "\n".join(out_lines)
        fname = f"sources_{chat_id}_{int(time.time())}.txt"
        await update.effective_chat.send_document(document=txt.encode('utf-8'), filename=fname, caption="📚 Список источников")

    async def handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        if data and data.startswith("download_pdf_"):
            chat_id = int(data.split("_")[-1])
            if chat_id in self.active_researches:
                research = self.active_researches[chat_id]
                md_text = research.get('full_report_text_md', '')
                pdf_bytes = self._render_pdf_bytes(research['topic'], md_text) if REPORTLAB_AVAILABLE else None
                if pdf_bytes:
                    await context.bot.send_document(chat_id=chat_id, document=pdf_bytes, filename=f"report_{research['topic'][:30].replace(' ', '_')}.pdf")
                else:
                    await context.bot.send_message(chat_id=chat_id, text="❗ PDF генерация недоступна (reportlab не установлен). Отправляю Markdown.")
                    await context.bot.send_document(chat_id=chat_id, document=md_text.encode('utf-8'), filename=f"report_{research['topic'][:30].replace(' ', '_')}.md")
            else:
                await query.edit_message_text("⚠️ Исследование не найдено.")

    # ---------- Research flow ----------
    async def start_research(self, update: Update, topic: str):
        chat_id = update.effective_chat.id
        if chat_id in self._tasks:
            await update.message.reply_text("⚠️ У вас уже есть активное исследование. Отмените его командой /cancel или дождитесь завершения.")
            return

        settings = self._get_user_settings(chat_id)
        max_results = settings['max_results']
        deep_analysis = settings['deep_analysis']
        lang = settings['lang']

        start_msg = await update.message.reply_text(
            f"🔬 <b>Начинаю исследование:</b>\n\"{topic}\"\n\n"
            "Я отправлю обновления прогресса в этом чате.", parse_mode='HTML'
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

        await update.message.reply_text("✅ Исследование запущено. Прогресс будет показываться здесь.")

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
            save_research_to_db(chat_id, topic, self.active_researches[chat_id], 'done', self.active_researches[chat_id]['start_time'])

            try:
                await self.application.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_message_id,
                    text=f"✅ <b>Исследование завершено</b>\nТема: {topic}\nВремя: {self.active_researches[chat_id]['completed_time']} с",
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.warning(f"Не удалось редактировать прогресс сообщение: {e}")

            md_filename = f"report_{topic.replace(' ','_')[:40]}_{int(time.time())}.md"
            await self.application.bot.send_document(
                chat_id=chat_id,
                document=md_text.encode('utf-8'),
                filename=md_filename,
                caption=f"📋 <b>Отчёт:</b> {topic}",
                parse_mode='HTML'
            )

            kb = [[InlineKeyboardButton("📥 Скачать PDF", callback_data=f"download_pdf_{chat_id}")]]
            if not REPORTLAB_AVAILABLE:
                kb.append([InlineKeyboardButton("ℹ️ PDF недоступен (установите reportlab)", callback_data=f"noop_{chat_id}")])
            reply_markup = InlineKeyboardMarkup(kb)
            await self.application.bot.send_message(chat_id=chat_id, text="Вы можете скачать отчёт в PDF:", reply_markup=reply_markup)

        except asyncio.CancelledError:
            logger.info(f"Research task cancelled for {chat_id}")
            if chat_id in self.active_researches:
                self.active_researches[chat_id]['status'] = 'cancelled'
                save_research_to_db(chat_id, topic, self.active_researches[chat_id], 'cancelled', self.active_researches[chat_id]['start_time'])
            try:
                await self.application.bot.send_message(chat_id=chat_id, text="❌ Исследование отменено.")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Ошибка при исследовании: {e}", exc_info=True)
            try:
                await self.application.bot.send_message(chat_id=chat_id, text=f"❌ Ошибка при выполнении исследования: {e}")
            except Exception:
                pass
            if chat_id in self.active_researches:
                self.active_researches[chat_id]['status'] = 'error'
                save_research_to_db(chat_id, topic, self.active_researches[chat_id], 'error', self.active_researches[chat_id]['start_time'])
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
        queries = await self.generate_search_queries(topic, settings)
        total_steps = len(queries) + 3
        current_step = 0

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def search_with_semaphore(query):
            async with semaphore:
                try:
                    resp = await asyncio.wait_for(self.serper.search(query, num_results=settings['max_results']), timeout=self.serper.per_request_timeout + 2)
                    return resp
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout Serper for query: {query}")
                    return {}
                except Exception as e:
                    logger.error(f"Serper error for query {query}: {e}")
                    return {}

        for i in range(0, len(queries), self.max_concurrent):
            batch = queries[i:i + self.max_concurrent]
            tasks = [asyncio.create_task(search_with_semaphore(q)) for q in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for j, res in enumerate(batch_results):
                current_step += 1
                await self._update_progress(chat_id, progress_message_id, current_step, total_steps, f"🔍 Поиск ({current_step}/{total_steps})")
                if isinstance(res, dict) and res.get('organic'):
                    r_items = res.get('organic', [])[:settings['max_results']]
                    results['searches'].append({'query': batch[j], 'results': r_items})
                    for item in r_items:
                        if item.get('snippet'):
                            src_index = len(results['sources']) + 1
                            results['key_findings'].append({
                                'title': item.get('title',''),
                                'snippet': item.get('snippet'),
                                'link': item.get('link',''),
                                '_source_index': src_index
                            })
                            results['sources'].append({'title': item.get('title',''), 'link': item.get('link','')})
                await asyncio.sleep(0.2)
            await asyncio.sleep(1) 

        current_step += 1
        await self._update_progress(chat_id, progress_message_id, current_step, total_steps, "📊 Анализ данных")
        await asyncio.sleep(1)

        current_step += 1
        await self._update_progress(chat_id, progress_message_id, current_step, total_steps, "🧠 Синтез (LLM)")
        try:
            report_text = await asyncio.wait_for(
                self.llm_generator.generate_report(results['key_findings'], topic),
                timeout=self.llm_generator.per_request_timeout + 5
            )
        except asyncio.TimeoutError:
            logger.warning("Mistral timeout")
            report_text = "⚠️ LLM не успела ответить в отведённое время. Попробуйте позже."
        except Exception as e:
            logger.error(f"Mistral error: {e}")
            report_text = f"⚠️ Ошибка генерации отчёта: {e}"

        results['full_report_text'] = report_text

        current_step += 1
        await self._update_progress(chat_id, progress_message_id, current_step, total_steps, "📄 Формирование отчёта")
        await asyncio.sleep(0.5)

        return results

    async def generate_search_queries(self, topic: str, settings: dict) -> List[str]:
        base_queries = [
            f"{topic} обзор",
            f"{topic} исследование",
            f"{topic} статистика данные",
            f"{topic} тенденции развитие",
            f"{topic} проблемы вызовы",
            f"{topic} решения инновации",
            f"{topic} прогнозы",
            f"{topic} экспертное мнение"
        ]
        if settings.get('deep_analysis'):
            base_queries.extend([
                f"{topic} case study пример",
                f"{topic} аналитика отчет",
                f"{topic} лучшие практики"
            ])
        return base_queries[:self.max_concurrent * 4]

    async def _update_progress(self, chat_id: int, message_id: int, step: int, total: int, current_step_name: str):
        pct = int(step * 100 / max(1, total))
        blocks = int(pct / 5)
        progress_bar = "█" * blocks + "░" * (20 - blocks)
        text = f"🔬 <b>Исследование в процессе...</b>\n\n" \
               f"<b>Этап:</b> {current_step_name}\n" \
               f"<b>Прогресс:</b> [{progress_bar}] {pct}% ({step}/{total})"
        try:
            await self.application.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode='HTML')
        except Exception as e:
            try:
                await self.application.bot.send_message(chat_id=chat_id, text=f"{current_step_name} — {pct}%")
            except Exception:
                logger.debug("Не удалось отправить уведомление о прогрессе")

    def _build_report_markdown(self, results: dict) -> str:
        md = ""
        md += f"# Отчёт: {results.get('topic','')}\n\n"
        md += f"*Создано: {results.get('timestamp')}\n\n---\n\n"
        md += "## Сгенерированный текст (LLM)\n\n"
        md += results.get('full_report_text', '') + "\n\n"
        md += "\n---\n\n## Ключевые находки (с привязкой к источникам)\n\n"
        for i, kf in enumerate(results.get('key_findings', []), start=1):
            src_idx = kf.get('_source_index') or i
            md += f"{i}. **{kf.get('title','')}** — {kf.get('snippet','')}  [{src_idx}]\n\n"
        md += "\n---\n\n## Источники\n\n"
        for idx, s in enumerate(results.get('sources', []), start=1):
            md += f"{idx}. [{s.get('title','')}]({s.get('link','')})\n"
        return md

    def _render_pdf_bytes(self, title: str, md_text: str) -> bytes:
        from io import BytesIO
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4
        x_margin = 40
        y = height - 60
        # --- ИЗМЕНЕНИЕ: ИСПОЛЬЗУЕМ ЗАРЕГИСТРИРОВАННЫЙ ШРИФТ ---
        font_name = PDF_FONT_NAME if PDF_FONT_NAME and PDF_FONT_NAME != 'Helvetica' else 'Helvetica'
        c.setFont(font_name, 16)
        c.drawString(x_margin, y, f"Отчёт: {title}")
        y -= 30
        c.setFont(font_name, 10)
        for line in md_text.splitlines():
            if y < 60:
                c.showPage()
                y = height - 60
            if len(line) > 200:
                parts = [line[i:i+200] for i in range(0, len(line), 200)]
            else:
                parts = [line]
            for p in parts:
                c.drawString(x_margin, y, p)
                y -= 12
        c.save()
        buffer.seek(0)
        return buffer.read()

    # ---------- Run ----------
    def run(self):
        if not self.token:
            print("Missing TELEGRAM_BOT_TOKEN in env.")
            exit(1)
        self.application = Application.builder().token(self.token).build()
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("research", self.research_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("cancel", self.cancel_command))
        self.application.add_handler(CommandHandler("settings", self.settings_command))
        self.application.add_handler(CommandHandler("sources", self.sources_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback_query))
        logger.info("🚀 Research Bot запущен (с SQLite, таймаутами и background tasks).")
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    required_vars = ['TELEGRAM_BOT_TOKEN', 'SERPER_API_KEY', 'MISTRAL_API_KEY']
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        print("Missing env vars:", ", ".join(missing))
        exit(1)
    bot = ResearchBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    except Exception as e:
        logger.error("Critical error: %s", e, exc_info=True)
        print("Critical error:", e)
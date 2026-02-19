import asyncio
import logging
import re
import time
import calendar as pycal
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from pathlib import Path
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.error import BadRequest

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException

from webdriver_manager.chrome import ChromeDriverManager


# ---------------- config ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logger = logging.getLogger("bumpix-bot")

TOKEN = "PASTE_YOUR_NEW_TOKEN_HERE"  # <-- вставь токен
HEADLESS = True
ADMIN_CHAT_ID = 125030638  # <-- chat_id для обратной связи (если нужно)

WAIT_POLL = 0.1
EXECUTOR = ThreadPoolExecutor(max_workers=6)

CHROMEDRIVER_PATH = ChromeDriverManager().install()
CHROME_SERVICE = Service(CHROMEDRIVER_PATH)

SEL_PICKER_CALENDAR = "div.picker_calendar"

ROOMS = {
    "grey": {"title": "⚪ Серая комната", "url": "https://bumpix.net/soundlevel"},
    "blue": {"title": "🔵 Синяя комната", "url": "https://bumpix.net/500141"},
    "green": {"title": "🟢 Зелёная комната", "url": "https://bumpix.net/517424"},
}

CABINET_URL = "https://bumpix.net/soundlevel"

MY_RECORDS_URLS = [
    "https://bumpix.net/page/client-appointments",
    "https://bumpix.net/ru/page/client-appointments",
    "https://bumpix.net/uk/page/client-appointments",
    "https://bumpix.net/en/page/client-appointments",
]

MAX_DAYS_AHEAD = 365
RECORDS_PAGE_SIZE = 5
PAGE_SIZE = 20

PROFILES_DIR = Path("./chrome_profiles").resolve()
PROFILES_DIR.mkdir(parents=True, exist_ok=True)

PHONE_HINT = "Введите номер телефона (логин) в формате +7XXXXXXXXXX\nПример: +79991234567"
PHONE_BAD = "Телефон некорректный. Формат: +7XXXXXXXXXX\nПример: +79991234567"

# На скрине кнопка: id="appointmentButton", class="btn btn-purple mar_top_10"
APPOINTMENT_BTN_SELECTOR = "button#appointmentButton, #appointmentButton, button.btn.btn-purple.mar_top_10"

# Комментарий находится в зоне appointmentControls (по скрину)
COMMENT_ROOT_SELECTOR = "#appointmentControls"
COMMENT_INPUT_SELECTORS = [
    "#appointmentControls textarea",
    "#appointmentControls input[type='text']",
    "#appointmentControls input:not([type])",
    "#appointmentControls input",
    "#appointmentControls [contenteditable='true']",
    "textarea[placeholder*='Коммент']",
    "input[placeholder*='Коммент']",
    "textarea[placeholder*='коммент']",
    "input[placeholder*='коммент']",
    "textarea[name*='comment']",
    "input[name*='comment']",
]


# ---------------- telegram helpers ----------------
def kb(rows):
    return InlineKeyboardMarkup(rows)


async def safe_answer(q):
    try:
        await q.answer()
    except BadRequest:
        pass


def get_logged_flag(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.user_data.get("cab_verified_records"))


# ---------------- feedback feature ----------------
def feedback_keyboard():
    return kb(
        [
            [InlineKeyboardButton("❌ Отмена", callback_data="feedback_cancel")],
            [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")],
        ]
    )


async def feedback_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["feedback_mode"] = True
    txt = (
        "Напиши сообщение автору бота (можно текст, фото или файл).\n\n"
        "После отправки я перешлю это автору и выйду из режима обратной связи."
    )
    if update.message:
        await update.message.reply_text(txt, reply_markup=feedback_keyboard())
    elif update.callback_query:
        await update.callback_query.edit_message_text(txt, reply_markup=feedback_keyboard())


async def feedback_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["feedback_mode"] = False
    logged = get_logged_flag(context)
    if update.callback_query:
        await update.callback_query.edit_message_text("Отменено. Выберите комнату:", reply_markup=room_keyboard(logged))
    elif update.message:
        await update.message.reply_text("Отменено. Выберите комнату:", reply_markup=room_keyboard(logged))


async def feedback_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("feedback_mode"):
        return

    if not ADMIN_CHAT_ID:
        context.user_data["feedback_mode"] = False
        await update.message.reply_text(
            "У автора бота не настроен ADMIN_CHAT_ID, поэтому я не могу переслать сообщение.",
            reply_markup=room_keyboard(get_logged_flag(context)),
        )
        return

    user = update.effective_user
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    username = f"@{user.username}" if user and user.username else "-"
    full_name = user.full_name if user else "-"
    user_id = user.id if user else "-"

    header = (
        "📩 Обратная связь\n"
        f"Время: {now}\n"
        f"От: {full_name}\n"
        f"Username: {username}\n"
        f"user_id: {user_id}\n"
    )

    msg = update.message
    try:
        if msg.text and not msg.text.startswith("/"):
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=header + "\n" + msg.text)
        elif msg.photo:
            photo = msg.photo[-1]
            cap = header + (("\n" + msg.caption) if msg.caption else "")
            await context.bot.send_photo(chat_id=ADMIN_CHAT_ID, photo=photo.file_id, caption=cap[:1024])
        elif msg.document:
            cap = header + (("\n" + msg.caption) if msg.caption else "")
            await context.bot.send_document(chat_id=ADMIN_CHAT_ID, document=msg.document.file_id, caption=cap[:1024])
        else:
            await msg.reply_text("Пожалуйста, отправь текст, фото или файл.", reply_markup=feedback_keyboard())
            return

        context.user_data["feedback_mode"] = False
        await msg.reply_text("Спасибо! Я переслал сообщение автору.", reply_markup=room_keyboard(get_logged_flag(context)))
    except Exception as e:
        logger.exception("feedback send failed: %s", e)
        context.user_data["feedback_mode"] = False
        await msg.reply_text(
            "Не удалось переслать сообщение автору (ошибка отправки). Попробуйте позже.",
            reply_markup=room_keyboard(get_logged_flag(context)),
        )


# ---------------- cabinet feature (login/register + my records) ----------------
def cabinet_menu_keyboard():
    return kb(
        [
            [InlineKeyboardButton("🧾 Регистрация", callback_data="cab_reg")],
            [InlineKeyboardButton("🔐 Вход", callback_data="cab_login")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cab_cancel")],
            [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")],
        ]
    )


def cabinet_cancel_keyboard():
    return kb(
        [
            [InlineKeyboardButton("❌ Отмена", callback_data="cab_cancel")],
            [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")],
        ]
    )


def normalize_phone(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    s = re.sub(r"[^0-9+]", "", s)
    return s


def normalize_phone_ru_to_plus7(s: str) -> str:
    s = normalize_phone(s)
    digits = re.sub(r"[^0-9]", "", s)
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    return s


def is_valid_ru_phone_plus7(s: str) -> bool:
    digits = re.sub(r"[^0-9]", "", s or "")
    return len(digits) == 11 and digits.startswith("7")


async def cabinet_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["cabinet"] = {"active": True, "mode": None, "step": None, "data": {}}
    text = (
        "👤 Личный кабинет\n\n"
        "Выберите действие:\n"
        "- Регистрация\n"
        "- Вход\n\n"
        "Важно: кнопка «📒 Мои записи» появится только после проверенного входа."
    )
    if update.message:
        await update.message.reply_text(text, reply_markup=cabinet_menu_keyboard())
    else:
        await update.callback_query.edit_message_text(text, reply_markup=cabinet_menu_keyboard())


async def cabinet_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("cabinet", None)
    logged = get_logged_flag(context)
    if update.callback_query:
        await update.callback_query.edit_message_text("Отменено. Выберите комнату:", reply_markup=room_keyboard(logged))
    elif update.message:
        await update.message.reply_text("Отменено. Выберите комнату:", reply_markup=room_keyboard(logged))


# ---------------- selenium helpers ----------------
def make_driver(headless: bool, profile_dir: Optional[Path]):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--lang=ru-RU")
    opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    try:
        opts.page_load_strategy = "eager"
    except Exception:
        pass

    if profile_dir is not None:
        profile_dir.mkdir(parents=True, exist_ok=True)
        opts.add_argument(f"--user-data-dir={str(profile_dir)}")
        opts.add_argument("--profile-directory=Default")

    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    driver.set_page_load_timeout(35)
    return driver


def open_page(driver, url: str):
    driver.get(url)
    WebDriverWait(driver, 25, poll_frequency=WAIT_POLL).until(EC.presence_of_element_located((By.TAG_NAME, "body")))


def robust_click(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.03)
        el.click()
    except StaleElementReferenceException:
        raise
    except Exception:
        driver.execute_script("arguments[0].click();", el)


def is_disabled_like(driver, el):
    try:
        disabled = el.get_attribute("disabled")
        aria = (el.get_attribute("aria-disabled") or "").lower()
        cls = (el.get_attribute("class") or "").lower()
        pe = driver.execute_script("return window.getComputedStyle(arguments[0]).pointerEvents;", el)
        return bool(disabled) or (aria == "true") or ("disabled" in cls) or (pe == "none")
    except Exception:
        return True


def page_source_has_any(driver, needles: list[str]) -> bool:
    src = (driver.page_source or "").lower()
    return any(n.lower() in src for n in needles)


def looks_like_auth_required(driver) -> bool:
    return page_source_has_any(
        driver,
        [
            "sign in required",
            "you have not signed in yet",
            "потрібна авторизація",
            "ви ще не авторизовані",
            "нужна авторизац",
            "не авториз",
            "registration",
            "sign in",
            "forgot password",
        ],
    )


def looks_like_logged_in(driver) -> bool:
    return page_source_has_any(driver, ["выйти", "logout"])


def _js_get_visible_modal_root(driver):
    return driver.execute_script(
        r"""
        function visible(el){
          if (!el) return false;
          const st = window.getComputedStyle(el);
          if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') return false;
          const r = el.getBoundingClientRect();
          return r.width > 50 && r.height > 50;
        }
        const mods = Array.from(document.querySelectorAll('.modal, [role="dialog"]'));
        for (const m of mods) {
          if (visible(m)) return m;
        }
        return null;
        """
    )


def _js_modal_visible(driver) -> bool:
    return bool(_js_get_visible_modal_root(driver))


def _js_find_and_click_by_text(driver, texts: list[str]) -> bool:
    return driver.execute_script(
        r"""
        const arr = (arguments[0] || []).map(x => String(x||'').trim().toLowerCase());
        function visible(el){
          if (!el) return false;
          const st = window.getComputedStyle(el);
          if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') return false;
          const r = el.getBoundingClientRect();
          return r.width > 10 && r.height > 10;
        }
        const nodes = Array.from(document.querySelectorAll('a,button,span,div'));
        for (const n of nodes) {
          const tx = (n.textContent || '').trim().toLowerCase();
          if (!tx) continue;
          if (!arr.includes(tx)) continue;
          if (!visible(n)) continue;
          const clickEl = n.closest('a,button') || n;
          clickEl.scrollIntoView({block:'center'});
          clickEl.click();
          return true;
        }
        return false;
        """,
        texts,
    )


def _find_input_in_modal_by_placeholder(driver, placeholder_sub: str, timeout=14):
    placeholder_sub = (placeholder_sub or "").strip().lower()

    def cond(d):
        root = _js_get_visible_modal_root(d)
        if not root:
            return False
        try:
            inputs = root.find_elements(By.CSS_SELECTOR, "input")
        except Exception:
            return False
        for inp in inputs:
            try:
                ph = (inp.get_attribute("placeholder") or "").strip().lower()
                if placeholder_sub in ph:
                    return inp
            except Exception:
                continue
        return False

    return WebDriverWait(driver, timeout, poll_frequency=WAIT_POLL).until(cond)


def _fill_input_sendkeys(inp, value: str):
    try:
        inp.click()
    except Exception:
        pass
    try:
        inp.send_keys(Keys.CONTROL, "a")
        inp.send_keys(Keys.BACKSPACE)
    except Exception:
        try:
            inp.clear()
        except Exception:
            pass
    inp.send_keys(value)


def _click_modal_button_by_text(driver, text_variants: list[str]) -> bool:
    root = _js_get_visible_modal_root(driver)
    if not root:
        return False
    want = [t.strip().lower() for t in (text_variants or []) if t and t.strip()]
    if not want:
        return False
    try:
        btns = root.find_elements(By.CSS_SELECTOR, "button,a,input[type='button'],input[type='submit']")
    except Exception:
        return False
    for b in btns:
        try:
            tt = (b.get_attribute("value") if (b.tag_name or "").lower() == "input" else b.text) or ""
            if tt.strip().lower() in want:
                robust_click(driver, b)
                return True
        except Exception:
            continue
    return False


def _read_modal_errors(driver) -> str:
    return driver.execute_script(
        r"""
        function visible(el){
          if (!el) return false;
          const st = window.getComputedStyle(el);
          if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') return false;
          const r = el.getBoundingClientRect();
          return r.width > 10 && r.height > 8;
        }
        const root = (function(){
          const mods = Array.from(document.querySelectorAll('.modal, [role="dialog"]'));
          for (const m of mods) if (visible(m)) return m;
          return document;
        })();
        const nodes = Array.from(root.querySelectorAll('.alert, .help-block, .text-danger, .error, .has-error'));
        const texts = [];
        for (const n of nodes) {
          const t = (n.innerText || n.textContent || '').trim();
          if (t && t.length <= 700) texts.push(t);
        }
        const uniq = [...new Set(texts)];
        return uniq.join('\n');
        """
    )


def verify_records_access(driver) -> bool:
    for u in MY_RECORDS_URLS:
        try:
            open_page(driver, u)
        except Exception:
            continue
        time.sleep(0.25)
        if not looks_like_auth_required(driver):
            return True
    return False


# ---------------- string utils ----------------
def clean_spaces(s: str) -> str:
    s = (s or "").replace("\u00a0", " ").replace("\u202f", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tidy_service_title(name: str, duration: str, cost: str) -> str:
    name = clean_spaces(name)
    duration = clean_spaces(duration)
    cost = clean_spaces(cost)
    if cost:
        cost = re.sub(r"\s*(руб\.?|₽)\s*$", "", cost, flags=re.IGNORECASE).strip()
    if cost:
        cost = f"{cost} руб."
    parts = [p for p in (name, duration, cost) if p]
    return " — ".join(parts) if parts else ""


def short_raw_service_fallback(raw: str) -> str:
    raw = clean_spaces(raw)
    if not raw:
        return ""
    raw = re.sub(r"\b(Выбрать время|Выбрано услуг|Услуги не выбраны)\b.*$", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\s{2,}", " ", raw).strip()
    return raw[:140]


# ---------------- timeBlocks helpers ----------------
def get_timeblocks_html(driver):
    return driver.execute_script(
        """
        const tb = document.querySelector('#timeBlocks');
        return tb ? (tb.innerHTML || '') : null;
        """
    )


def get_timeblocks_text(driver):
    return driver.execute_script(
        """
        const tb = document.querySelector('#timeBlocks');
        return tb ? (tb.innerText || '') : '';
        """
    ) or ""


def is_server_error_timeblocks(driver):
    low = (get_timeblocks_text(driver) or "").strip().lower()
    return ("servererror" in low) or ("при запросе к серверу произошла ошибка" in low) or ("попробуйте позже" in low)


def is_placeholder_timeblocks(driver):
    txt = (get_timeblocks_text(driver) or "").strip()
    if not txt:
        return True
    return bool(re.fullmatch(r"[.\s]+", txt))


def wait_timeblocks_changed(driver, prev_html, timeout=10):
    WebDriverWait(driver, timeout, poll_frequency=WAIT_POLL).until(
        lambda d: (get_timeblocks_html(d) or "") != (prev_html or "")
    )


def wait_timeblocks_stable(driver, timeout=12, stable_for_sec=0.7):
    end = time.time() + timeout
    last = None
    last_change = time.time()
    while time.time() < end:
        cur = get_timeblocks_html(driver) or ""
        if last is None:
            last = cur
            last_change = time.time()
        elif cur != last:
            last = cur
            last_change = time.time()
        else:
            if time.time() - last_change >= stable_for_sec:
                return True
        time.sleep(0.1)
    raise TimeoutException("timeBlocks did not become stable")


def wait_timeblocks_not_placeholder(driver, timeout=10):
    WebDriverWait(driver, timeout, poll_frequency=WAIT_POLL).until(
        lambda d: (not is_placeholder_timeblocks(d)) or is_server_error_timeblocks(d)
    )


# ---------------- slot parsing ----------------
def extract_times_now(driver):
    times_raw = driver.execute_script(
        r"""
        const tb = document.querySelector('#timeBlocks');
        if (!tb) return [];
        const isHidden = (el) => {
          const st = window.getComputedStyle(el);
          return st.display === 'none' || st.visibility === 'hidden';
        };
        const isDisabled = (el) => {
          if (!el) return true;
          const cls = (el.getAttribute('class') || '').toLowerCase();
          const aria = (el.getAttribute('aria-disabled') || '').toLowerCase();
          const disabled = el.getAttribute('disabled');
          const pe = window.getComputedStyle(el).pointerEvents;
          return !!disabled || aria === 'true' || cls.includes('disabled') || pe === 'none';
        };
        const nodes = Array.from(tb.querySelectorAll('label,button,a'));
        const out = [];
        const re = /\b\d{1,2}:\d{2}\b/;
        for (const el of nodes) {
          if (isHidden(el)) continue;
          const tag = el.tagName.toLowerCase();
          let ok = false;
          if (tag === 'label') {
            const inpInside = el.querySelector('input');
            const htmlFor = (el.getAttribute('for') || '').trim();
            const cls = (el.getAttribute('class') || '').toLowerCase();
            ok = !!inpInside || !!htmlFor || cls.includes('btn-time');
            if (inpInside && isDisabled(inpInside)) ok = false;
          } else {
            ok = true;
          }
          if (!ok) continue;
          if (isDisabled(el)) continue;
          const t = (el.textContent || el.innerText || '').trim();
          if (re.test(t)) out.push(t);
        }
        return out;
        """
    )

    if not times_raw:
        txt = get_timeblocks_text(driver) or ""
        times_raw = re.findall(r"\b\d{1,2}:\d{2}\b", txt)

    out, seen = [], set()
    for t in times_raw or []:
        m = re.search(r"\b\d{1,2}:\d{2}\b", t)
        if not m:
            continue
        v = m.group(0)
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def parse_times_mode(driver, tries=26, sleep_sec=0.2, min_votes=2):
    samples = []
    for _ in range(tries):
        if not is_server_error_timeblocks(driver):
            cur = extract_times_now(driver)
            if cur:
                samples.append(tuple(cur))
        time.sleep(sleep_sec)
    if not samples:
        return []
    counts = Counter(samples)
    best, votes = counts.most_common(1)[0]
    if votes < min_votes:
        best = max(samples, key=len)
    return list(best)


# ---------------- services parsing ----------------
@dataclass(frozen=True)
class ServiceItem:
    sid: str
    title: str


def bumpix_get_services_with_driver(driver, url: str):
    open_page(driver, url)

    WebDriverWait(driver, 18, poll_frequency=WAIT_POLL).until(
        lambda d: (d.execute_script("return document.querySelectorAll('input.data_service[data-service-id]').length") or 0)
        > 0
    )

    rows = driver.execute_script(
        r"""
        const inputs = Array.from(document.querySelectorAll('input.data_service[data-service-id]'));
        const out = [];
        for (const input of inputs) {
          const item = input.closest('div.master_service_item') || input.closest('[class*="master_service_item"]');
          const id = (input.getAttribute('data-service-id') || '').trim();
          if (!item || !id) continue;

          const name = (item.querySelector('.msn_body')?.textContent || '').trim();
          const duration = (item.querySelector('.sduration')?.textContent || '').trim();
          const cost = (item.querySelector('.scost')?.textContent || '').trim();
          const raw = (item.innerText || '').trim();

          out.push({id, name, duration, cost, raw});
        }
        return out;
        """
    )

    services = []
    seen = set()
    for r in rows:
        sid = clean_spaces(r.get("id"))
        if not sid or sid in seen:
            continue
        name = r.get("name") or ""
        duration = r.get("duration") or ""
        cost = r.get("cost") or ""
        raw = r.get("raw") or ""
        title = tidy_service_title(name, duration, cost)
        if not title:
            title = short_raw_service_fallback(raw)
        if title:
            seen.add(sid)
            services.append(ServiceItem(sid=sid, title=title))
    return services


# ---------------- selecting services ----------------
def clear_all_services(driver):
    driver.execute_script(
        r"""
        const inputs = Array.from(document.querySelectorAll('input.data_service[data-service-id]'));
        for (const inp of inputs) {
          const label = inp.closest('label');
          if (inp.checked) {
            inp.checked = false;
            inp.dispatchEvent(new Event('input', {bubbles:true}));
            inp.dispatchEvent(new Event('change', {bubbles:true}));
          }
          if (label && label.classList.contains('active')) {
            label.classList.remove('active');
          }
        }
        """
    )
    time.sleep(0.12)


def click_service_by_id(driver, sid: str):
    for _ in range(8):
        try:
            inp = WebDriverWait(driver, 10, poll_frequency=WAIT_POLL).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, f"input.data_service[data-service-id='{sid}']"))
            )
            label = None
            try:
                label = driver.execute_script("return arguments[0].closest('label')", inp)
            except Exception:
                label = None

            if label:
                robust_click(driver, label)
            else:
                robust_click(driver, inp)

            driver.execute_script(
                r"""
                const inp = arguments[0];
                const label = inp.closest('label');
                inp.checked = true;
                inp.dispatchEvent(new Event('input', {bubbles:true}));
                inp.dispatchEvent(new Event('change', {bubbles:true}));
                if (label) label.classList.add('active');
                """,
                inp,
            )
            return
        except StaleElementReferenceException:
            time.sleep(0.12)
            continue

    raise RuntimeError(f"Не удалось выбрать услугу {sid} (stale).")


def wait_services_selected(driver, sids, timeout=15):
    sids = list(map(str, sids))

    def cond(d):
        return d.execute_script(
            r"""
            const sids = arguments[0];
            function selected(sid) {
              const inp = document.querySelector(`input.data_service[data-service-id="${sid}"]`);
              if (!inp) return false;
              const label = inp.closest('label');
              const checked = !!inp.checked;
              const active = label ? label.classList.contains('active') : false;
              return checked || active;
            }
            for (const sid of sids) {
              if (!selected(sid)) return false;
            }
            return true;
            """,
            sids,
        )

    WebDriverWait(driver, timeout, poll_frequency=WAIT_POLL).until(cond)


def select_services(driver, sids):
    clear_all_services(driver)
    for sid in sids:
        click_service_by_id(driver, str(sid))
        time.sleep(0.08)
    wait_services_selected(driver, sids, timeout=15)


# ---------------- choose time ----------------
def find_choose_time_button(driver):
    xpaths = [
        "//button[contains(normalize-space(.),'Выбрать время')]",
        "//a[contains(normalize-space(.),'Выбрать время')]",
        "//input[(@type='button' or @type='submit') and contains(@value,'Выбрать время')]",
        "//*[@role='button' and contains(normalize-space(.),'Выбрать время')]",
    ]
    for xp in xpaths:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                if (el.tag_name or "").lower() not in ("button", "a", "input"):
                    continue
                if not is_disabled_like(driver, el):
                    return el
        except Exception:
            continue
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "button.btn-orange, button.btn.btn-orange")
        for el in els:
            if not is_disabled_like(driver, el):
                return el
    except Exception:
        pass
    return None


def click_choose_time(driver, timeout=22):
    def cond(d):
        el = find_choose_time_button(d)
        if not el:
            return False
        if is_disabled_like(d, el):
            return False
        return el

    btn = WebDriverWait(driver, timeout, poll_frequency=WAIT_POLL).until(cond)
    robust_click(driver, btn)


# ---------------- calendar/date selection (UTC safe) ----------------
def wait_calendar_visible(driver, timeout=14):
    WebDriverWait(driver, timeout, poll_frequency=WAIT_POLL).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, SEL_PICKER_CALENDAR))
    )


def wait_calendar_days_present_js(driver, timeout=14):
    WebDriverWait(driver, timeout, poll_frequency=WAIT_POLL).until(
        lambda d: (d.execute_script("return document.querySelectorAll('td.day').length") or 0) > 0
    )


def click_calendar_nav(driver, direction: str):
    btn = driver.execute_script(
        r"""
        const dir = arguments[0];
        const root = document.querySelector('.picker_calendar') || document;
        const th = root.querySelector(`.datepicker-days th.${dir}`) || root.querySelector(`th.${dir}`);
        return th || null;
        """,
        direction,
    )
    if btn:
        robust_click(driver, btn)
    time.sleep(0.12)


def get_calendar_view_year_month_utc(driver):
    res = driver.execute_script(
        r"""
        const root = document.querySelector('.picker_calendar') || document;
        const cells = Array.from(root.querySelectorAll('td.day'));
        for (const c of cells) {
          const cls = c.getAttribute('class') || '';
          if (cls.includes('old') || cls.includes('new') || cls.includes('disabled')) continue;
          const ms = c.getAttribute('data-date');
          if (!ms) continue;
          const dt = new Date(Number(ms));
          return {y: dt.getUTCFullYear(), m: dt.getUTCMonth()};
        }
        return null;
        """
    )
    if not res:
        return None
    return int(res.get("y")), int(res.get("m"))


def find_day_cell_for_date_utc(driver, y: int, m0: int, d: int):
    return driver.execute_script(
        r"""
        const y = arguments[0], m = arguments[1], d = arguments[2];
        const root = document.querySelector('.picker_calendar') || document;
        const tds = Array.from(root.querySelectorAll('td.day'));
        for (const c of tds) {
          const cls = c.getAttribute('class') || '';
          if (cls.includes('old') || cls.includes('new') || cls.includes('disabled')) continue;
          const ms = c.getAttribute('data-date');
          if (!ms) continue;
          const dt = new Date(Number(ms));
          if (dt.getUTCFullYear() === y && dt.getUTCMonth() === m && dt.getUTCDate() === d) return c;
        }
        return null;
        """,
        int(y),
        int(m0),
        int(d),
    )


def click_specific_date(driver, target_date: date):
    try:
        driver.switch_to.default_content()
    except Exception:
        pass

    wait_calendar_days_present_js(driver, timeout=14)
    y, m0, d = target_date.year, target_date.month - 1, target_date.day
    prev = get_timeblocks_html(driver) or ""

    for _ in range(14):
        cell = find_day_cell_for_date_utc(driver, y, m0, d)
        if cell:
            for _ in range(10):
                try:
                    robust_click(driver, cell)
                    try:
                        wait_timeblocks_changed(driver, prev, timeout=8)
                    except TimeoutException:
                        pass
                    try:
                        wait_timeblocks_stable(driver, timeout=12, stable_for_sec=0.7)
                    except TimeoutException:
                        pass
                    try:
                        wait_timeblocks_not_placeholder(driver, timeout=8)
                    except TimeoutException:
                        pass
                    return
                except StaleElementReferenceException:
                    time.sleep(0.12)
                    cell = find_day_cell_for_date_utc(driver, y, m0, d)
                    if not cell:
                        break
                    continue

        view = get_calendar_view_year_month_utc(driver)
        if not view:
            click_calendar_nav(driver, "next")
            wait_calendar_days_present_js(driver, timeout=14)
            continue

        vy, vm = view
        if (y, m0) > (vy, vm):
            click_calendar_nav(driver, "next")
        else:
            click_calendar_nav(driver, "prev")
        wait_calendar_days_present_js(driver, timeout=14)

    raise RuntimeError("Не удалось выбрать дату в календаре.")


# ---------------- main scenario (find times) ----------------
@dataclass(frozen=True)
class TimesResult:
    status: str  # "OK" | "EMPTY" | "ERROR"
    times: list[str]
    error: Optional[str] = None


def get_times_for_selection(driver, url: str, sids, target_date: date) -> TimesResult:
    open_page(driver, url)
    select_services(driver, sids)
    click_choose_time(driver, timeout=22)
    wait_calendar_visible(driver, timeout=14)

    for attempt in range(5):
        click_specific_date(driver, target_date)

        if is_server_error_timeblocks(driver):
            time.sleep(0.8 + attempt * 0.4)
            continue

        times = parse_times_mode(driver, tries=26, sleep_sec=0.2, min_votes=2)
        if times:
            time.sleep(0.25)
            confirm = parse_times_mode(driver, tries=10, sleep_sec=0.18, min_votes=1)
            return TimesResult(status="OK", times=confirm if confirm else times)

        if not is_placeholder_timeblocks(driver):
            return TimesResult(status="EMPTY", times=[])

        time.sleep(0.5)
        driver.refresh()
        WebDriverWait(driver, 25, poll_frequency=WAIT_POLL).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        select_services(driver, sids)
        click_choose_time(driver, timeout=22)
        wait_calendar_visible(driver, timeout=14)

    click_specific_date(driver, target_date)
    if is_server_error_timeblocks(driver):
        return TimesResult(status="ERROR", times=[], error="Ошибка сервера при получении слотов")

    times = parse_times_mode(driver, tries=28, sleep_sec=0.2, min_votes=1)
    if times:
        return TimesResult(status="OK", times=times)
    return TimesResult(status="EMPTY", times=[])


# ---------------- booking helpers ----------------
def click_time_slot(driver, time_str: str) -> bool:
    time_str = (time_str or "").strip()
    if not time_str:
        return False

    ok = driver.execute_script(
        r"""
        const target = String(arguments[0] || '').trim();
        const tb = document.querySelector('#timeBlocks');
        if (!tb || !target) return false;

        const isHidden = (el) => {
          const st = window.getComputedStyle(el);
          return st.display === 'none' || st.visibility === 'hidden';
        };
        const isDisabled = (el) => {
          if (!el) return true;
          const cls = (el.getAttribute('class') || '').toLowerCase();
          const aria = (el.getAttribute('aria-disabled') || '').toLowerCase();
          const disabled = el.getAttribute('disabled');
          const pe = window.getComputedStyle(el).pointerEvents;
          return !!disabled || aria === 'true' || cls.includes('disabled') || pe === 'none';
        };

        const nodes = Array.from(tb.querySelectorAll('label,button,a'));
        for (const el of nodes) {
          if (isHidden(el)) continue;
          const txt = (el.textContent || el.innerText || '').trim();
          if (!txt) continue;
          if (!txt.includes(target)) continue;
          if (isDisabled(el)) continue;
          el.scrollIntoView({block:'center'});
          el.click();
          return true;
        }
        return false;
        """,
        time_str,
    )
    return bool(ok)


def _maybe_open_comment_ui(driver):
    driver.execute_script(
        r"""
        const root = document.querySelector(arguments[0]) || document;
        const candidates = [
          '.fa-comment', '.fa-comments',
          '.glyphicon-comment',
          '[class*="comment"]',
          '[title*="Коммент"]', '[title*="коммент"]',
          '[aria-label*="Коммент"]', '[aria-label*="коммент"]'
        ];
        for (const sel of candidates) {
          const el = root.querySelector(sel);
          if (!el) continue;
          const btn = el.closest('button,a,span,div') || el;
          const st = window.getComputedStyle(btn);
          if (st.display === 'none' || st.visibility === 'hidden') continue;
          const r = btn.getBoundingClientRect();
          if (r.width < 8 || r.height < 8) continue;
          try { btn.click(); } catch(e) {}
          break;
        }
        """,
        COMMENT_ROOT_SELECTOR,
    )


def _find_first_visible(driver, selectors: list[str], timeout=12):
    end = time.time() + timeout
    last_exc = None
    while time.time() < end:
        for sel in selectors:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
            except Exception as e:
                last_exc = e
                continue
            try:
                if not el.is_displayed():
                    continue
                r = el.rect
                if (r.get("width", 0) or 0) < 40 or (r.get("height", 0) or 0) < 16:
                    continue
                return el
            except Exception as e:
                last_exc = e
                continue
        time.sleep(0.2)
    if last_exc:
        raise TimeoutException(str(last_exc))
    raise TimeoutException("Element not found")


def fill_comment_strict(driver, comment: str, timeout=14) -> bool:
    comment = (comment or "").strip()
    if not comment:
        return False

    _maybe_open_comment_ui(driver)

    try:
        el = _find_first_visible(driver, COMMENT_INPUT_SELECTORS, timeout=timeout)
    except TimeoutException:
        return False

    tag = (el.tag_name or "").lower()
    is_contenteditable = (el.get_attribute("contenteditable") or "").lower() == "true"

    try:
        if is_contenteditable:
            driver.execute_script(
                r"""
                const el = arguments[0];
                const val = arguments[1];
                el.focus();
                el.innerText = '';
                el.textContent = '';
                el.innerHTML = '';
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.textContent = val;
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
                """,
                el,
                comment,
            )
        elif tag in ("input", "textarea"):
            _fill_input_sendkeys(el, comment)
            try:
                el.send_keys(Keys.TAB)
            except Exception:
                pass
        else:
            driver.execute_script(
                r"""
                const el = arguments[0];
                const val = arguments[1];
                el.focus();
                el.textContent = val;
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
                """,
                el,
                comment,
            )
    except Exception:
        return False

    end = time.time() + 8
    while time.time() < end:
        try:
            if is_contenteditable:
                cur = (el.text or "").strip()
            else:
                cur = (el.get_attribute("value") or "").strip()
        except Exception:
            cur = ""
        if comment in cur or cur == comment:
            return True
        time.sleep(0.2)
    return False


def click_appointment_button(driver) -> bool:
    try:
        btn = WebDriverWait(driver, 12, poll_frequency=WAIT_POLL).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, APPOINTMENT_BTN_SELECTOR))
        )
    except TimeoutException:
        return False
    if is_disabled_like(driver, btn):
        return False
    robust_click(driver, btn)
    return True


def wait_booking_feedback(driver, timeout=10) -> str:
    end = time.time() + timeout
    needles = [
        "спасибо",
        "успеш",
        "запис",
        "appointment",
        "подтверж",
        "ваша запись",
        "created",
        "success",
        "ошибка",
        "error",
    ]
    while time.time() < end:
        src = (driver.page_source or "")
        low = src.lower()
        for n in needles:
            if n in low:
                return n
        time.sleep(0.25)
    return ""


@dataclass(frozen=True)
class BookingAttempt:
    time: str
    ok: bool
    message: str


def book_appointment_flow(driver, url: str, sids, target_date: date, time_str: str, comment: str) -> BookingAttempt:
    try:
        open_page(driver, url)
        select_services(driver, sids)
        click_choose_time(driver, timeout=22)
        wait_calendar_visible(driver, timeout=14)
        click_specific_date(driver, target_date)

        if is_server_error_timeblocks(driver):
            return BookingAttempt(time=time_str, ok=False, message="Серверная ошибка в timeBlocks")

        try:
            wait_timeblocks_stable(driver, timeout=12, stable_for_sec=0.7)
        except TimeoutException:
            pass
        try:
            wait_timeblocks_not_placeholder(driver, timeout=8)
        except TimeoutException:
            pass

        if not click_time_slot(driver, time_str):
            return BookingAttempt(time=time_str, ok=False, message=f"Не смог кликнуть слот {time_str}")

        time.sleep(0.2)
        if not fill_comment_strict(driver, comment, timeout=14):
            return BookingAttempt(time=time_str, ok=False, message="Не нашёл/не смог заполнить поле комментария")

        time.sleep(0.2)
        if not click_appointment_button(driver):
            return BookingAttempt(time=time_str, ok=False, message="Кнопка «Записаться» не найдена/не кликабельна")

        hint = wait_booking_feedback(driver, timeout=10)
        if hint in ("ошибка", "error"):
            return BookingAttempt(time=time_str, ok=False, message="После клика обнаружен текст ошибки на странице")

        return BookingAttempt(time=time_str, ok=True, message="Комментарий заполнен, «Записаться» нажата")
    except Exception as e:
        return BookingAttempt(time=time_str, ok=False, message=str(e) or "Unknown error")


# ---------------- Cabinet Selenium logic ----------------
@dataclass(frozen=True)
class AuthResult:
    ok: bool
    message: str
    verified_records: bool = False


@dataclass(frozen=True)
class RecordsResult:
    ok: bool
    records: list[str]
    message: str


def cabinet_login_with_driver(driver, url: str, phone: str, password: str) -> AuthResult:
    open_page(driver, url)

    if looks_like_logged_in(driver) and verify_records_access(driver):
        return AuthResult(True, "Сессия уже активна, «Мои записи» доступны.", verified_records=True)

    _js_find_and_click_by_text(driver, ["вход", "войти", "sign in", "login"])
    WebDriverWait(driver, 14, poll_frequency=WAIT_POLL).until(lambda d: bool(_js_modal_visible(d)))

    try:
        inp_phone = _find_input_in_modal_by_placeholder(driver, "телефон", timeout=10)
    except TimeoutException:
        inp_phone = _find_input_in_modal_by_placeholder(driver, "номер", timeout=10)

    inp_pass = _find_input_in_modal_by_placeholder(driver, "парол", timeout=10)

    _fill_input_sendkeys(inp_phone, phone)
    time.sleep(0.1)
    _fill_input_sendkeys(inp_pass, password)

    if not _click_modal_button_by_text(driver, ["войти", "вход", "sign in"]):
        return AuthResult(False, "Не нашёл кнопку «Войти/Sign in» в модалке.", verified_records=False)

    t_end = time.time() + 18
    while time.time() < t_end:
        err = _read_modal_errors(driver)
        if err:
            return AuthResult(False, err, verified_records=False)
        if not _js_modal_visible(driver):
            break
        if looks_like_logged_in(driver):
            break
        time.sleep(0.25)

    try:
        driver.refresh()
        WebDriverWait(driver, 18, poll_frequency=WAIT_POLL).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except Exception:
        pass

    err = _read_modal_errors(driver)
    if err:
        return AuthResult(False, err, verified_records=False)

    if verify_records_access(driver):
        return AuthResult(True, "Вход подтверждён: «Мои записи» доступны.", verified_records=True)

    return AuthResult(
        False,
        "Вход не подтверждён: «Мои записи» всё ещё требуют авторизацию (возможно, вход не прошёл или аккаунт не имеет доступа к этой странице).",
        verified_records=False,
    )


def cabinet_register_with_driver(driver, url: str, name: str, phone: str, password: str, password2: str) -> AuthResult:
    open_page(driver, url)

    _js_find_and_click_by_text(driver, ["регистрация", "sign up", "registration"])
    WebDriverWait(driver, 14, poll_frequency=WAIT_POLL).until(lambda d: bool(_js_modal_visible(d)))

    try:
        inp_name = _find_input_in_modal_by_placeholder(driver, "имя", timeout=10)
    except TimeoutException:
        inp_name = None

    inp_phone = _find_input_in_modal_by_placeholder(driver, "телефон", timeout=10)
    inp_pass = _find_input_in_modal_by_placeholder(driver, "парол", timeout=10)

    try:
        inp_pass2 = _find_input_in_modal_by_placeholder(driver, "повтор", timeout=10)
    except TimeoutException:
        inp_pass2 = None

    if inp_name:
        _fill_input_sendkeys(inp_name, name)
    time.sleep(0.05)
    _fill_input_sendkeys(inp_phone, phone)
    time.sleep(0.05)
    _fill_input_sendkeys(inp_pass, password)
    time.sleep(0.05)
    if inp_pass2:
        _fill_input_sendkeys(inp_pass2, password2)

    if not _click_modal_button_by_text(driver, ["зарегистрироваться", "sign up", "registration"]):
        return AuthResult(False, "Не нашёл кнопку «Зарегистрироваться/Sign up» в модалке.", verified_records=False)

    t_end = time.time() + 18
    while time.time() < t_end:
        err = _read_modal_errors(driver)
        if err:
            return AuthResult(False, err, verified_records=False)
        if not _js_modal_visible(driver):
            return AuthResult(True, "Окно регистрации закрылось (похоже на успешную регистрацию).", verified_records=False)
        time.sleep(0.25)

    err = _read_modal_errors(driver)
    return AuthResult(False, err or "Не удалось определить результат регистрации.", verified_records=False)


def _js_extract_my_records(driver) -> list[str]:
    return driver.execute_script(
        r"""
        function visible(el){
          if (!el) return false;
          const st = window.getComputedStyle(el);
          if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') return false;
          const r = el.getBoundingClientRect();
          return r.width > 20 && r.height > 10;
        }
        const nodes = Array.from(document.querySelectorAll('div,li,article,section,main')).filter(visible);
        const out = [];
        const seen = new Set();
        const reDate = /\b\d{1,2}\s+[A-Za-zА-Яа-яЁёІіЇїЄє]+(?:\s+\d{4})?\b/;
        const reDate2 = /\b\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}\b/;
        const reTime = /\b\d{1,2}:\d{2}\b/;

        function isJunkLine(s){
          const t = (s || '').trim();
          if (!t) return true;
          const l = t.toLowerCase();
          if (l === 'отзыв') return true;
          if (l === 'новая запись') return true;
          if (l.includes('отзыв') && t.length <= 20) return true;
          if (l.includes('новая запись') && t.length <= 30) return true;
          return false;
        }

        for (const el of nodes) {
          let tx = (el.innerText || '').trim();
          if (!tx) continue;
          if (tx.length < 50 || tx.length > 1200) continue;
          const low = tx.toLowerCase();
          if (!(low.includes('комнат') || low.includes('room'))) continue;

          const hasDate = reDate.test(tx) || reDate2.test(tx);
          const hasTime = reTime.test(tx);
          const hasMoney = low.includes('руб') || low.includes('uah') || low.includes('$');
          const hasStatus = low.includes('заверш') || low.includes('отмен') || low.includes('актив') || low.includes('cancel') || low.includes('moved');

          if (!((hasDate || hasTime) && (hasMoney || hasStatus))) continue;

          tx = tx.replace(/\u00a0/g, ' ')
            .replace(/[ \t]+\n/g, '\n')
            .replace(/\n{3,}/g, '\n\n')
            .trim();

          const key = tx.slice(0, 180);
          if (seen.has(key)) continue;
          seen.add(key);

          const lines = tx.split('\n')
            .map(s => (s || '').trim())
            .filter(Boolean)
            .filter(s => !isJunkLine(s));

          out.push(lines.slice(0, 14).join('\n'));
          if (out.length >= 40) break;
        }
        return out;
        """
    )


def cabinet_open_my_records_with_driver(driver) -> RecordsResult:
    last_url = None
    for u in MY_RECORDS_URLS:
        last_url = u
        try:
            open_page(driver, u)
        except Exception:
            continue
        time.sleep(0.25)
        if not looks_like_auth_required(driver):
            break

    if last_url is None:
        return RecordsResult(False, [], "Не удалось открыть страницу «Мои записи» (URL не загрузились).")

    if looks_like_auth_required(driver):
        return RecordsResult(False, [], "Нужна авторизация. Страница записей просит вход.")

    recs = _js_extract_my_records(driver) or []
    if not recs:
        return RecordsResult(True, [], "Записей не найдено (возможно, их нет).")
    return RecordsResult(True, recs, f"Найдено записей: {len(recs)}")


# ---------------- Workers: per Telegram user ----------------
class ServicesCache:
    def __init__(self):
        self.lock = RLock()
        self.by_url: dict[str, list[ServiceItem]] = {}
        self.ts_by_url: dict[str, float] = {}
        self.ttl = 10 * 60

    def get(self, url: str):
        with self.lock:
            now = time.time()
            items = self.by_url.get(url)
            ts = self.ts_by_url.get(url, 0.0)
            if items and (now - ts) < self.ttl:
                return list(items)
            return None

    def put(self, url: str, items: list[ServiceItem]):
        with self.lock:
            self.by_url[url] = list(items)
            self.ts_by_url[url] = time.time()


SERVICES_CACHE = ServicesCache()


class BumpixUserWorker:
    def __init__(self, tg_user_id: int):
        self.tg_user_id = tg_user_id
        self.lock = RLock()
        self.driver = None
        self.profile_dir = PROFILES_DIR / f"u_{tg_user_id}"

    def _ensure_driver(self):
        if self.driver is None:
            self.driver = make_driver(headless=HEADLESS, profile_dir=self.profile_dir)

    def reset_driver(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        self.driver = None

    def get_services(self, url: str):
        cached = SERVICES_CACHE.get(url)
        if cached:
            return cached
        with self.lock:
            self._ensure_driver()
            try:
                services = bumpix_get_services_with_driver(self.driver, url)
            except (WebDriverException, StaleElementReferenceException):
                self.reset_driver()
                self._ensure_driver()
                services = bumpix_get_services_with_driver(self.driver, url)
            SERVICES_CACHE.put(url, services)
            return services

    def get_times(self, url: str, sids, target_date: date) -> TimesResult:
        with self.lock:
            self._ensure_driver()
            try:
                return get_times_for_selection(self.driver, url, sids, target_date)
            except (WebDriverException, StaleElementReferenceException) as e:
                self.reset_driver()
                self._ensure_driver()
                try:
                    return get_times_for_selection(self.driver, url, sids, target_date)
                except Exception as e2:
                    return TimesResult(status="ERROR", times=[], error=str(e2) or str(e))

    def book_appointments(self, url: str, sids, target_date: date, times: list[str], comment: str) -> list[BookingAttempt]:
        with self.lock:
            self._ensure_driver()
            out: list[BookingAttempt] = []
            for t in times:
                try:
                    out.append(book_appointment_flow(self.driver, url, sids, target_date, t, comment))
                except (WebDriverException, StaleElementReferenceException) as e:
                    self.reset_driver()
                    self._ensure_driver()
                    try:
                        out.append(book_appointment_flow(self.driver, url, sids, target_date, t, comment))
                    except Exception as e2:
                        out.append(BookingAttempt(time=t, ok=False, message=str(e2) or str(e)))
            return out

    def cabinet_login(self, url: str, phone: str, password: str) -> AuthResult:
        with self.lock:
            self._ensure_driver()
            try:
                return cabinet_login_with_driver(self.driver, url, phone, password)
            except (WebDriverException, StaleElementReferenceException) as e:
                self.reset_driver()
                self._ensure_driver()
                try:
                    return cabinet_login_with_driver(self.driver, url, phone, password)
                except Exception as e2:
                    return AuthResult(False, str(e2) or str(e), verified_records=False)

    def cabinet_register(self, url: str, name: str, phone: str, password: str, password2: str) -> AuthResult:
        with self.lock:
            self._ensure_driver()
            try:
                return cabinet_register_with_driver(self.driver, url, name, phone, password, password2)
            except (WebDriverException, StaleElementReferenceException) as e:
                self.reset_driver()
                self._ensure_driver()
                try:
                    return cabinet_register_with_driver(self.driver, url, name, phone, password, password2)
                except Exception as e2:
                    return AuthResult(False, str(e2) or str(e), verified_records=False)

    def get_my_records(self) -> RecordsResult:
        with self.lock:
            self._ensure_driver()
            try:
                return cabinet_open_my_records_with_driver(self.driver)
            except (WebDriverException, StaleElementReferenceException) as e:
                self.reset_driver()
                self._ensure_driver()
                try:
                    return cabinet_open_my_records_with_driver(self.driver)
                except Exception as e2:
                    return RecordsResult(False, [], str(e2) or str(e))


WORKERS: dict[int, BumpixUserWorker] = {}
WORKERS_LOCK = RLock()


def get_worker_for_update(update: Update) -> BumpixUserWorker:
    uid = update.effective_user.id
    with WORKERS_LOCK:
        w = WORKERS.get(uid)
        if not w:
            w = BumpixUserWorker(uid)
            WORKERS[uid] = w
        return w


# ---------------- UI: rooms/services/calendar/times ----------------
def room_keyboard(logged_in: bool):
    rows = [
        [InlineKeyboardButton(ROOMS["grey"]["title"], callback_data="room:grey")],
        [InlineKeyboardButton(ROOMS["blue"]["title"], callback_data="room:blue")],
        [InlineKeyboardButton(ROOMS["green"]["title"], callback_data="room:green")],
    ]

    if logged_in:
        rows.append([InlineKeyboardButton("📒 Мои записи", callback_data="my_records")])

    rows += [
        [InlineKeyboardButton("👤 Личный кабинет", callback_data="cabinet")],
        [InlineKeyboardButton("✉️ Обратная связь", callback_data="feedback")],
        [InlineKeyboardButton("🧹 Сбросить веб-сессию", callback_data="reset_web")],
    ]
    return kb(rows)


def services_keyboard(services, selected_idx_set, page: int, room_key: str, logged_in: bool):
    total = len(services)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start = page * PAGE_SIZE
    end = min(start + PAGE_SIZE, total)

    rows = []
    for i in range(start, end):
        s = services[i]
        mark = "✅ " if i in selected_idx_set else "☐ "
        rows.append([InlineKeyboardButton((mark + s.title)[:60], callback_data=f"tgl:{i}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"pg:{page-1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"pg:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("✅ Далее", callback_data="next"), InlineKeyboardButton("🧹 Сброс", callback_data="reset")])
    rows.append([InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")])

    if logged_in:
        rows.append([InlineKeyboardButton("📒 Мои записи", callback_data="my_records")])

    rows.append([InlineKeyboardButton("👤 Личный кабинет", callback_data="cabinet")])
    rows.append([InlineKeyboardButton("✉️ Обратная связь", callback_data="feedback")])
    rows.append([InlineKeyboardButton("🧹 Сбросить веб-сессию", callback_data="reset_web")])
    return kb(rows)


def times_keyboard(times: list[str], iso: str, room_key: str, logged_in: bool, selected_times=None):
    times = (times or [])[:30]
    selected = set(selected_times or [])
    rows = []
    per_row = 4
    for i in range(0, len(times), per_row):
        chunk = times[i : i + per_row]
        row = []
        for t in chunk:
            label = f"✅ {t}" if t in selected else t
            row.append(InlineKeyboardButton(label, callback_data=f"time:{iso}:{t}"))
        rows.append(row)

    if selected:
        rows.append([InlineKeyboardButton("📝 К записи", callback_data=f"to_booking:{iso}")])

    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"date:{iso}"), InlineKeyboardButton("📅 Другой день", callback_data="pick_date")])
    rows.append([InlineKeyboardButton("↩️ Услуги", callback_data=f"room:{room_key}"), InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")])

    if logged_in:
        rows.append([InlineKeyboardButton("📒 Мои записи", callback_data="my_records")])

    rows.append([InlineKeyboardButton("👤 Личный кабинет", callback_data="cabinet")])
    rows.append([InlineKeyboardButton("✉️ Обратная связь", callback_data="feedback")])
    rows.append([InlineKeyboardButton("🧹 Сбросить веб-сессию", callback_data="reset_web")])
    return kb(rows)


# ---------------- Inline calendar (Telegram UI) ----------------
RU_MONTHS = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
RU_DOW = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def clamp_month(year: int, month: int):
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    return year, month


def ym_add(year: int, month: int, delta_months: int):
    return clamp_month(year, month + delta_months)


def parse_ym(s: str):
    y, m = s.split("-", 1)
    return int(y), int(m)


def iso_day(y: int, m: int, d: int) -> str:
    return f"{y:04d}-{m:02d}-{d:02d}"


def parse_iso_day(s: str) -> date:
    y, m, d = s.split("-", 2)
    return date(int(y), int(m), int(d))


def calendar_keyboard(year: int, month: int, min_date: date, room_key: str, logged_in: bool):
    max_date = min_date + timedelta(days=MAX_DAYS_AHEAD)
    min_ym = (min_date.year, min_date.month)
    cur_ym = (year, month)
    max_ym = (max_date.year, max_date.month)

    prev_enabled = cur_ym > min_ym
    next_enabled = cur_ym < max_ym

    rows = []
    nav = [
        InlineKeyboardButton(
            "⬅️" if prev_enabled else " ",
            callback_data=f"calnav:{year:04d}-{month:02d}:-1" if prev_enabled else "calnoop",
        ),
        InlineKeyboardButton(f"{RU_MONTHS[month-1]} {year}", callback_data="calnoop"),
        InlineKeyboardButton(
            "➡️" if next_enabled else " ",
            callback_data=f"calnav:{year:04d}-{month:02d}:+1" if next_enabled else "calnoop",
        ),
    ]
    rows.append(nav)
    rows.append([InlineKeyboardButton(x, callback_data="calnoop") for x in RU_DOW])

    cal = pycal.Calendar(firstweekday=0)  # Monday
    weeks = cal.monthdayscalendar(year, month)
    for w in weeks:
        r = []
        for d in w:
            if d == 0:
                r.append(InlineKeyboardButton(" ", callback_data="calnoop"))
                continue
            dt = date(year, month, d)
            if dt < min_date or dt > max_date:
                r.append(InlineKeyboardButton("·", callback_data="calnoop"))
                continue
            r.append(InlineKeyboardButton(str(d), callback_data=f"date:{iso_day(year, month, d)}"))
        rows.append(r)

    rows.append([InlineKeyboardButton("↩️ Назад к услугам", callback_data=f"room:{room_key}")])
    rows.append([InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")])

    if logged_in:
        rows.append([InlineKeyboardButton("📒 Мои записи", callback_data="my_records")])

    rows.append([InlineKeyboardButton("👤 Личный кабинет", callback_data="cabinet")])
    rows.append([InlineKeyboardButton("✉️ Обратная связь", callback_data="feedback")])
    rows.append([InlineKeyboardButton("🧹 Сбросить веб-сессию", callback_data="reset_web")])
    return kb(rows)


# ---------------- my records (telegram view) ----------------
def render_records_page(records: list[str], page: int, per_page: int):
    total = len(records)
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = min(start + per_page, total)
    chunk = records[start:end]

    text = f"📒 Мои записи\n\nПоказано: {start+1}–{end} из {total}\n"
    for i, item in enumerate(chunk, start=start + 1):
        text += f"\n{i}) {item}\n"

    rows = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"rec:{page-1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"rec:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data="my_records")])
    rows.append([InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")])
    rows.append([InlineKeyboardButton("👤 Личный кабинет", callback_data="cabinet")])
    rows.append([InlineKeyboardButton("🧹 Сбросить веб-сессию", callback_data="reset_web")])
    return text[:3900], kb(rows)


# ---------------- message router (feedback/cabinet/booking-comment) ----------------
async def cabinet_receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cab = context.user_data.get("cabinet")
    if not cab or not cab.get("active"):
        return

    msg = update.message
    text = (msg.text or "").strip()
    mode = cab.get("mode")
    step = cab.get("step")
    data = cab.setdefault("data", {})

    if mode == "login":
        if step == "phone":
            phone = normalize_phone_ru_to_plus7(text)
            if not is_valid_ru_phone_plus7(phone):
                await msg.reply_text(PHONE_BAD, reply_markup=cabinet_cancel_keyboard())
                return
            data["phone"] = phone
            cab["step"] = "password"
            await msg.reply_text("Введите пароль:", reply_markup=cabinet_cancel_keyboard())
            return

        if step == "password":
            if len(text) < 4:
                await msg.reply_text("Пароль слишком короткий. Введите ещё раз:", reply_markup=cabinet_cancel_keyboard())
                return
            data["password"] = text
            await msg.reply_text("Пробую выполнить вход…")

            phone = data.get("phone")
            password = data.get("password")

            worker = get_worker_for_update(update)
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(EXECUTOR, lambda: worker.cabinet_login(CABINET_URL, phone, password))

            context.user_data.pop("cabinet", None)
            if result.ok and result.verified_records:
                context.user_data["cab_logged_in"] = True
                context.user_data["cab_verified_records"] = True
                await msg.reply_text(f"✅ {result.message}", reply_markup=room_keyboard(True))
            else:
                context.user_data["cab_logged_in"] = False
                context.user_data["cab_verified_records"] = False
                await msg.reply_text(f"❌ Не удалось подтвердить вход.\n\n{result.message}", reply_markup=room_keyboard(False))
            return

    if mode == "reg":
        if step == "name":
            if len(text) < 3:
                await msg.reply_text("Имя и фамилия слишком короткие. Введите ещё раз:", reply_markup=cabinet_cancel_keyboard())
                return
            data["name"] = text
            cab["step"] = "phone"
            await msg.reply_text(PHONE_HINT, reply_markup=cabinet_cancel_keyboard())
            return

        if step == "phone":
            phone = normalize_phone_ru_to_plus7(text)
            if not is_valid_ru_phone_plus7(phone):
                await msg.reply_text(PHONE_BAD, reply_markup=cabinet_cancel_keyboard())
                return
            data["phone"] = phone
            cab["step"] = "password"
            await msg.reply_text("Придумайте пароль:", reply_markup=cabinet_cancel_keyboard())
            return

        if step == "password":
            if len(text) < 4:
                await msg.reply_text("Пароль слишком короткий. Введите ещё раз:", reply_markup=cabinet_cancel_keyboard())
                return
            data["password"] = text
            cab["step"] = "password2"
            await msg.reply_text("Повторите пароль:", reply_markup=cabinet_cancel_keyboard())
            return

        if step == "password2":
            if text != data.get("password"):
                await msg.reply_text("Пароли не совпадают. Повторите ещё раз:", reply_markup=cabinet_cancel_keyboard())
                return
            data["password2"] = text
            await msg.reply_text("Пробую зарегистрировать…")

            name = data.get("name")
            phone = data.get("phone")
            password = data.get("password")
            password2 = data.get("password2")

            worker = get_worker_for_update(update)
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                EXECUTOR, lambda: worker.cabinet_register(CABINET_URL, name, phone, password, password2)
            )

            context.user_data.pop("cabinet", None)
            if result.ok:
                await msg.reply_text(
                    f"✅ Регистрация выполнена.\n\n{result.message}\n\nТеперь выполните вход, чтобы увидеть «📒 Мои записи».",
                    reply_markup=room_keyboard(get_logged_flag(context)),
                )
            else:
                await msg.reply_text(
                    f"❌ Не удалось зарегистрироваться.\n\n{result.message}",
                    reply_markup=room_keyboard(get_logged_flag(context)),
                )
            return

    await msg.reply_text("Сначала выберите действие (Регистрация/Вход).", reply_markup=cabinet_menu_keyboard())


async def any_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ожидание комментария для записи -> после Enter заполняем форму на сайте -> потом жмём "Записаться"
    if context.user_data.get("booking_comment_mode"):
        msg = update.message
        comment = (msg.text or "").strip() if msg else ""
        if not comment:
            await msg.reply_text("Пожалуйста, отправьте текстовый комментарий.")
            return

        context.user_data["booking_comment_mode"] = False

        draft = context.user_data.get("booking_draft") or {}
        draft["comment"] = comment
        context.user_data["booking_draft"] = draft

        room_key = draft.get("room_key")
        date_iso = draft.get("date_iso")
        times = draft.get("times") or []
        titles = draft.get("titles") or []

        sids = context.user_data.get("sids") or []
        url = context.user_data.get("room_url")

        if not room_key or not date_iso or not times or not sids or not url:
            await msg.reply_text(
                "Черновик записи неполный. Пожалуйста, выберите комнату/услуги/дату/слоты заново.",
                reply_markup=room_keyboard(get_logged_flag(context)),
            )
            return

        try:
            target = parse_iso_day(date_iso)
        except Exception:
            await msg.reply_text("Некорректная дата в черновике. Выберите дату заново.", reply_markup=room_keyboard(get_logged_flag(context)))
            return

        await msg.reply_text("Принял комментарий. Заполняю поле комментария на сайте и нажимаю «Записаться»…")

        worker = get_worker_for_update(update)
        loop = asyncio.get_running_loop()
        attempts: list[BookingAttempt] = await loop.run_in_executor(
            EXECUTOR, lambda: worker.book_appointments(url, sids, target, list(times), comment)
        )

        ok_list = [a for a in attempts if a.ok]
        bad_list = [a for a in attempts if not a.ok]

        types_text = ", ".join(titles) if titles else "(не выбрано)"
        times_text = ", ".join(times) if times else "(не выбрано)"

        text = (
            "📝 Результат записи\n\n"
            f"Комната: {ROOMS[room_key]['title']}\n"
            f"Дата: {date_iso}\n"
            f"Тип: {types_text}\n"
            f"Слоты: {times_text}\n"
            f"Комментарий: {comment}\n\n"
        )

        if ok_list:
            text += "✅ Успешно:\n" + "\n".join([f"- {a.time}: {a.message}" for a in ok_list]) + "\n\n"
        if bad_list:
            text += "❌ Не удалось:\n" + "\n".join([f"- {a.time}: {a.message}" for a in bad_list]) + "\n\n"

        if ADMIN_CHAT_ID:
            try:
                user = update.effective_user
                who = f"{user.full_name} (@{user.username}) id={user.id}" if user else "unknown"
                await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"📝 Попытка записи\nОт: {who}\n\n{text}")
            except Exception:
                pass

        await msg.reply_text(text[:3900], reply_markup=room_keyboard(get_logged_flag(context)))
        return

    if context.user_data.get("feedback_mode"):
        return await feedback_receive(update, context)

    cab = context.user_data.get("cabinet")
    if cab and cab.get("active"):
        if update.message and update.message.text:
            return await cabinet_receive_text(update, context)
        await update.message.reply_text("Для личного кабинета отправьте, пожалуйста, текст.", reply_markup=cabinet_cancel_keyboard())
        return


# ---------------- handlers ----------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Выберите комнату:", reply_markup=room_keyboard(get_logged_flag(context)))


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("feedback_mode"):
        await feedback_cancel(update, context)
        return
    if context.user_data.get("cabinet"):
        await cabinet_cancel(update, context)
        return
    if context.user_data.get("booking_comment_mode"):
        context.user_data["booking_comment_mode"] = False
        await update.message.reply_text("Отменено.", reply_markup=room_keyboard(get_logged_flag(context)))
        return
    await update.message.reply_text("Нечего отменять.", reply_markup=room_keyboard(get_logged_flag(context)))


async def cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data or ""

    if context.user_data.get("feedback_mode") and data not in ("feedback", "feedback_cancel", "rooms"):
        await q.answer("Сначала завершите/отмените обратную связь.", show_alert=False)
        return

    cab = context.user_data.get("cabinet")
    if cab and cab.get("active"):
        if data not in ("cab_reg", "cab_login", "cab_cancel", "rooms"):
            await q.answer("Завершите/отмените 'Личный кабинет'.", show_alert=False)
            return

    logged_in = get_logged_flag(context)

    if data == "feedback":
        await feedback_start(update, context)
        return

    if data == "feedback_cancel":
        await feedback_cancel(update, context)
        return

    if data == "cabinet":
        await cabinet_start(update, context)
        return

    if data == "cab_cancel":
        await cabinet_cancel(update, context)
        return

    if data == "cab_reg":
        context.user_data["cabinet"] = {"active": True, "mode": "reg", "step": "name", "data": {}}
        await q.edit_message_text("Регистрация.\n\nВведите имя и фамилию:", reply_markup=cabinet_cancel_keyboard())
        return

    if data == "cab_login":
        context.user_data["cabinet"] = {"active": True, "mode": "login", "step": "phone", "data": {}}
        await q.edit_message_text(f"Вход.\n\n{PHONE_HINT}", reply_markup=cabinet_cancel_keyboard())
        return

    if data == "reset_web":
        worker = get_worker_for_update(update)
        worker.reset_driver()
        context.user_data["cab_logged_in"] = False
        context.user_data["cab_verified_records"] = False
        await q.edit_message_text(
            "Веб-сессия сброшена (драйвер пересоздан). Профиль Chrome сохранён.",
            reply_markup=room_keyboard(False),
        )
        return

    if data == "rooms":
        keep_verified = bool(context.user_data.get("cab_verified_records"))
        context.user_data.clear()
        if keep_verified:
            context.user_data["cab_verified_records"] = True
            context.user_data["cab_logged_in"] = True
        await q.edit_message_text("Выберите комнату:", reply_markup=room_keyboard(get_logged_flag(context)))
        return

    if data == "my_records":
        if not logged_in:
            await q.edit_message_text("Сначала выполните вход через «Личный кабинет».", reply_markup=room_keyboard(False))
            return

        await q.edit_message_text("Получаю список записей…")
        worker = get_worker_for_update(update)
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(EXECUTOR, lambda: worker.get_my_records())

        if not res.ok:
            if "Нужна авторизация" in (res.message or ""):
                context.user_data["cab_logged_in"] = False
                context.user_data["cab_verified_records"] = False
            await q.edit_message_text(f"❌ Не удалось получить записи.\n\n{res.message}", reply_markup=room_keyboard(get_logged_flag(context)))
            return

        context.user_data["records_cache"] = res.records
        context.user_data["records_page"] = 0
        text, markup = render_records_page(res.records, 0, RECORDS_PAGE_SIZE)
        await q.edit_message_text(text, reply_markup=markup)
        return

    if data.startswith("rec:"):
        if not logged_in:
            await q.edit_message_text("Сначала выполните вход через «Личный кабинет».", reply_markup=room_keyboard(False))
            return

        page = int(data.split("rec:", 1)[1])
        recs = context.user_data.get("records_cache") or []
        if not recs:
            await q.edit_message_text("Кэш записей пуст. Нажмите «Мои записи» ещё раз.", reply_markup=room_keyboard(get_logged_flag(context)))
            return

        text, markup = render_records_page(recs, page, RECORDS_PAGE_SIZE)
        await q.edit_message_text(text, reply_markup=markup)
        return

    if data == "calnoop":
        return

    # toggle время (мультивыбор)
    if data.startswith("time:"):
        parts = data.split(":", 3)
        if len(parts) != 4:
            await q.answer("Некорректные данные времени")
            return

        _, iso, hh, mm = parts
        picked = f"{hh}:{mm}"

        if context.user_data.get("picked_times_iso") != iso:
            context.user_data["picked_times_iso"] = iso
            context.user_data["picked_times"] = set()
            context.user_data.pop("booking_draft", None)

        picked_set = context.user_data.setdefault("picked_times", set())
        if picked in picked_set:
            picked_set.remove(picked)
        else:
            picked_set.add(picked)

        room_key = context.user_data.get("room_key", "grey")
        titles = context.user_data.get("titles", []) or []
        times = context.user_data.get("last_times", []) or []

        try:
            target = parse_iso_day(iso)
            pretty_date = target.strftime("%d.%m.%Y")
        except Exception:
            pretty_date = iso

        header = " + ".join(titles[:2])
        if len(titles) > 2:
            header += f" (+{len(titles)-2} ещё)"

        chosen_sorted = sorted(picked_set, key=lambda x: (int(x.split(":")[0]), int(x.split(":")[1])))
        chosen_line = "—" if not chosen_sorted else ", ".join(chosen_sorted)

        text = (
            f"{ROOMS[room_key]['title']}\n{header}\n\n"
            f"Дата: {pretty_date}\n\n"
            f"Выбрано: {chosen_line}\n\n"
            "Выберите время:"
        )

        await q.edit_message_text(
            text,
            reply_markup=times_keyboard(times, iso, room_key, get_logged_flag(context), selected_times=chosen_sorted),
        )
        return

    # "К записи" -> подтверждение (ФИКС: требуем авторизацию)
    if data.startswith("to_booking:"):
        iso = data.split("to_booking:", 1)[1].strip()

        if not get_logged_flag(context):
            await q.edit_message_text(
                "Вы еще не авторизованы.\n\nПожалуйста, выполните вход в личный кабинет.",
                reply_markup=kb(
                    [
                        [InlineKeyboardButton("👤 Авторизоваться", callback_data="cabinet")],
                        [InlineKeyboardButton("↩️ Назад к слотам", callback_data=f"date:{iso}")],
                        [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")],
                    ]
                ),
            )
            return

        picked_set = context.user_data.get("picked_times", set()) or set()
        if context.user_data.get("picked_times_iso") != iso:
            picked_set = set()

        if not picked_set:
            await q.answer("Сначала выберите хотя бы один слот.")
            return

        room_key = context.user_data.get("room_key", "grey")
        titles = context.user_data.get("titles", []) or []
        chosen_sorted = sorted(picked_set, key=lambda x: (int(x.split(":")[0]), int(x.split(":")[1])))

        try:
            target = parse_iso_day(iso)
            pretty_date = target.strftime("%d.%m.%Y")
        except Exception:
            pretty_date = iso

        types_text = "\n".join([f"- {t}" for t in titles]) if titles else "- (не выбрано)"
        times_text = "\n".join([f"- {t}" for t in chosen_sorted])

        context.user_data["booking_draft"] = {
            "room_key": room_key,
            "date_iso": iso,
            "times": chosen_sorted,
            "titles": titles,
        }

        text = (
            "Вы подтверждаете запись?\n\n"
            f"Комната: {ROOMS[room_key]['title']}\n"
            f"Дата: {pretty_date}\n\n"
            "Тип записи:\n"
            f"{types_text}\n\n"
            "Временные слоты:\n"
            f"{times_text}"
        )

        await q.edit_message_text(
            text,
            reply_markup=kb(
                [
                    [
                        InlineKeyboardButton("✅ Да", callback_data="booking_yes"),
                        InlineKeyboardButton("❌ Отменить", callback_data="booking_cancel"),
                    ],
                ]
            ),
        )
        return

    if data == "booking_cancel":
        draft = context.user_data.get("booking_draft") or {}
        iso = draft.get("date_iso") or context.user_data.get("picked_times_iso")
        room_key = draft.get("room_key") or context.user_data.get("room_key", "grey")
        times = context.user_data.get("last_times", []) or []

        picked_set = context.user_data.get("picked_times", set()) or set()
        chosen_sorted = sorted(picked_set, key=lambda x: (int(x.split(":")[0]), int(x.split(":")[1])))

        await q.edit_message_text(
            "Отменено. Вернулись к выбору слотов:",
            reply_markup=times_keyboard(times, iso, room_key, get_logged_flag(context), selected_times=chosen_sorted),
        )
        return

    if data == "booking_yes":
        # доп. защита: авторизация могла "слететь" между шагами
        if not get_logged_flag(context):
            await q.edit_message_text(
                "Вы еще не авторизованы.\n\nПожалуйста, выполните вход в личный кабинет.",
                reply_markup=kb(
                    [
                        [InlineKeyboardButton("👤 Авторизоваться", callback_data="cabinet")],
                        [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")],
                    ]
                ),
            )
            return

        draft = context.user_data.get("booking_draft")
        if not draft:
            await q.answer("Черновик записи не найден. Выберите слоты заново.")
            return

        context.user_data["booking_comment_mode"] = True
        await q.edit_message_text(
            "Введите комментарий и отправьте сообщением (Enter):",
            reply_markup=kb([[InlineKeyboardButton("❌ Отменить", callback_data="booking_comment_cancel")]]),
        )
        return

    if data == "booking_comment_cancel":
        context.user_data["booking_comment_mode"] = False

        draft = context.user_data.get("booking_draft") or {}
        iso = draft.get("date_iso") or context.user_data.get("picked_times_iso")
        room_key = draft.get("room_key") or context.user_data.get("room_key", "grey")
        times = context.user_data.get("last_times", []) or []

        picked_set = context.user_data.get("picked_times", set()) or set()
        chosen_sorted = sorted(picked_set, key=lambda x: (int(x.split(":")[0]), int(x.split(":")[1])))

        await q.edit_message_text(
            "Отменено. Вернулись к выбору слотов:",
            reply_markup=times_keyboard(times, iso, room_key, get_logged_flag(context), selected_times=chosen_sorted),
        )
        return

    if data.startswith("room:"):
        room_key = data.split("room:", 1)[1]
        if room_key not in ROOMS:
            await q.edit_message_text("Неизвестная комната.", reply_markup=room_keyboard(get_logged_flag(context)))
            return

        url = ROOMS[room_key]["url"]
        context.user_data["room_key"] = room_key
        context.user_data["room_url"] = url

        context.user_data.pop("booking_draft", None)
        context.user_data.pop("picked_times_iso", None)
        context.user_data.pop("picked_times", None)

        await q.edit_message_text("Загружаю услуги…")
        worker = get_worker_for_update(update)
        loop = asyncio.get_running_loop()
        services = await loop.run_in_executor(EXECUTOR, lambda: worker.get_services(url))

        context.user_data["services"] = services
        context.user_data["sel"] = set()
        context.user_data["page"] = 0

        if not services:
            extra = [[InlineKeyboardButton("📒 Мои записи", callback_data="my_records")]] if get_logged_flag(context) else []
            await q.edit_message_text(
                "Не удалось получить список услуг. Попробуйте ещё раз.",
                reply_markup=kb(
                    [
                        [InlineKeyboardButton("🔄 Обновить", callback_data=f"room:{room_key}")],
                        [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")],
                        *extra,
                        [InlineKeyboardButton("👤 Личный кабинет", callback_data="cabinet")],
                        [InlineKeyboardButton("✉️ Обратная связь", callback_data="feedback")],
                        [InlineKeyboardButton("🧹 Сбросить веб-сессию", callback_data="reset_web")],
                    ]
                ),
            )
            return

        await q.edit_message_text(
            f"{ROOMS[room_key]['title']}\n\nВыберите одну или несколько услуг:",
            reply_markup=services_keyboard(services, context.user_data["sel"], 0, room_key, get_logged_flag(context)),
        )
        return

    if data.startswith("pg:"):
        services = context.user_data.get("services", [])
        room_key = context.user_data.get("room_key", "grey")
        if not services:
            await q.edit_message_text("Список услуг пуст.", reply_markup=room_keyboard(get_logged_flag(context)))
            return

        page = int(data.split("pg:", 1)[1])
        context.user_data["page"] = page
        sel = context.user_data.get("sel", set())
        await q.edit_message_text(
            f"{ROOMS[room_key]['title']}\n\nВыбрано услуг: {len(sel)}",
            reply_markup=services_keyboard(services, sel, page, room_key, get_logged_flag(context)),
        )
        return

    if data.startswith("tgl:"):
        services = context.user_data.get("services", [])
        room_key = context.user_data.get("room_key", "grey")
        if not services:
            return

        i = int(data.split("tgl:", 1)[1])
        sel = context.user_data.setdefault("sel", set())
        if i in sel:
            sel.remove(i)
        else:
            sel.add(i)

        await q.edit_message_text(
            f"{ROOMS[room_key]['title']}\n\nВыбрано услуг: {len(sel)}",
            reply_markup=services_keyboard(services, sel, context.user_data.get("page", 0), room_key, get_logged_flag(context)),
        )
        return

    if data == "reset":
        services = context.user_data.get("services", [])
        room_key = context.user_data.get("room_key", "grey")
        context.user_data["sel"] = set()
        await q.edit_message_text(
            f"{ROOMS[room_key]['title']}\n\nВыберите одну или несколько услуг:",
            reply_markup=services_keyboard(services, context.user_data["sel"], context.user_data.get("page", 0), room_key, get_logged_flag(context)),
        )
        return

    if data == "next":
        services = context.user_data.get("services", [])
        sel = context.user_data.get("sel", set())
        room_key = context.user_data.get("room_key", "grey")

        if not sel:
            await q.edit_message_text(
                "Выберите хотя бы одну услугу.",
                reply_markup=services_keyboard(services, sel, context.user_data.get("page", 0), room_key, get_logged_flag(context)),
            )
            return

        sids = [services[i].sid for i in sorted(sel)]
        titles = [services[i].title for i in sorted(sel)]
        context.user_data["sids"] = sids
        context.user_data["titles"] = titles

        today = date.today()
        context.user_data["cal_min_date"] = today.isoformat()

        context.user_data.pop("booking_draft", None)
        context.user_data.pop("picked_times_iso", None)
        context.user_data.pop("picked_times", None)

        await q.edit_message_text(
            "Выберите дату (прошедшие дни недоступны):",
            reply_markup=calendar_keyboard(today.year, today.month, today, room_key, get_logged_flag(context)),
        )
        return

    if data.startswith("calnav:"):
        room_key = context.user_data.get("room_key", "grey")
        min_iso = context.user_data.get("cal_min_date") or date.today().isoformat()
        min_date = parse_iso_day(min_iso)

        _, rest = data.split("calnav:", 1)
        ym, delta = rest.rsplit(":", 1)
        y, m = parse_ym(ym)
        dm = int(delta)

        ny, nm = ym_add(y, m, dm)
        max_date = min_date + timedelta(days=MAX_DAYS_AHEAD)

        if (ny, nm) < (min_date.year, min_date.month):
            return
        if (ny, nm) > (max_date.year, max_date.month):
            return

        await q.edit_message_reply_markup(reply_markup=calendar_keyboard(ny, nm, min_date, room_key, get_logged_flag(context)))
        return

    if data.startswith("date:"):
        room_key = context.user_data.get("room_key")
        url = context.user_data.get("room_url")
        sids = context.user_data.get("sids", [])
        titles = context.user_data.get("titles", [])

        if not room_key or not url or not sids:
            await q.edit_message_text("Сначала выберите комнату и услуги.", reply_markup=room_keyboard(get_logged_flag(context)))
            return

        iso = data.split("date:", 1)[1].strip()
        try:
            target = parse_iso_day(iso)
        except Exception:
            await q.edit_message_text("Некорректная дата.", reply_markup=room_keyboard(get_logged_flag(context)))
            return

        today = date.today()
        if target < today:
            await q.answer("Нельзя выбирать прошедшие дни.")
            return
        if target > today + timedelta(days=MAX_DAYS_AHEAD):
            await q.answer("Слишком далеко. Выберите дату ближе.")
            return

        await q.edit_message_text("Ищу свободные слоты…")
        worker = get_worker_for_update(update)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(EXECUTOR, lambda: worker.get_times(url, sids, target))

        header = " + ".join(titles[:2])
        if len(titles) > 2:
            header += f" (+{len(titles)-2} ещё)"
        pretty_date = target.strftime("%d.%m.%Y")

        if result.status == "OK" and result.times:
            context.user_data["last_times"] = result.times
            context.user_data["last_date_iso"] = iso

            if context.user_data.get("picked_times_iso") != iso:
                context.user_data["picked_times_iso"] = iso
                context.user_data["picked_times"] = set()
                context.user_data.pop("booking_draft", None)

            picked_set = context.user_data.get("picked_times", set()) or set()
            chosen_sorted = sorted(picked_set, key=lambda x: (int(x.split(":")[0]), int(x.split(":")[1])))

            text = (
                f"{ROOMS[room_key]['title']}\n{header}\n\n"
                f"Дата: {pretty_date}\n\n"
                "Выберите время:"
            )

            await q.edit_message_text(
                text,
                reply_markup=times_keyboard(result.times, iso, room_key, get_logged_flag(context), selected_times=chosen_sorted),
            )
            return

        if result.status == "EMPTY":
            text = f"{ROOMS[room_key]['title']}\n{header}\n\nДата: {pretty_date}\n\nНет свободных слотов."
        else:
            msg = result.error or "Не удалось получить актуальные слоты. Попробуйте ещё раз."
            text = f"{ROOMS[room_key]['title']}\n{header}\n\nДата: {pretty_date}\n\n{msg}"

        extra = [[InlineKeyboardButton("📒 Мои записи", callback_data="my_records")]] if get_logged_flag(context) else []
        await q.edit_message_text(
            text,
            reply_markup=kb(
                [
                    [InlineKeyboardButton("🔄 Обновить", callback_data=f"date:{iso}")],
                    [InlineKeyboardButton("📅 Другой день", callback_data="pick_date")],
                    [InlineKeyboardButton("↩️ Услуги", callback_data=f"room:{room_key}")],
                    [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")],
                    *extra,
                    [InlineKeyboardButton("👤 Личный кабинет", callback_data="cabinet")],
                    [InlineKeyboardButton("✉️ Обратная связь", callback_data="feedback")],
                    [InlineKeyboardButton("🧹 Сбросить веб-сессию", callback_data="reset_web")],
                ]
            ),
        )
        return

    if data == "pick_date":
        room_key = context.user_data.get("room_key", "grey")
        min_iso = context.user_data.get("cal_min_date") or date.today().isoformat()
        min_date = parse_iso_day(min_iso)
        await q.edit_message_text(
            "Выберите дату (прошедшие дни недоступны):",
            reply_markup=calendar_keyboard(min_date.year, min_date.month, min_date, room_key, get_logged_flag(context)),
        )
        return


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error: %s", context.error)


def main():
    app = Application.builder().token("TOKEN").build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("feedback", feedback_start))
    app.add_handler(CommandHandler("cabinet", cabinet_start))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CallbackQueryHandler(cb))
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.Document.ALL) & (~filters.COMMAND), any_message_router))

    app.add_error_handler(on_error)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

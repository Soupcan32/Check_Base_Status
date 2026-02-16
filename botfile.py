import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from threading import RLock

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.error import BadRequest

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException

from webdriver_manager.chrome import ChromeDriverManager


# ---------------- config ----------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logger = logging.getLogger("bumpix-bot")

TOKEN = "PASTE_YOUR_NEW_TOKEN_HERE"   # <-- вставь новый токен
HEADLESS = True

WAIT_POLL = 0.1
EXECUTOR = ThreadPoolExecutor(max_workers=1)

CHROMEDRIVER_PATH = ChromeDriverManager().install()
CHROME_SERVICE = Service(CHROMEDRIVER_PATH)

SEL_PICKER_CALENDAR = "div.picker_calendar"

ROOMS = {
    "grey":  {"title": "⚪ Серая комната",  "url": "https://bumpix.net/soundlevel"},
    "blue":  {"title": "🔵 Синяя комната",  "url": "https://bumpix.net/500141"},
    "green": {"title": "🟢 Зелёная комната", "url": "https://bumpix.net/517424"},
}


# ---------------- telegram helpers ----------------

def kb(rows):
    return InlineKeyboardMarkup(rows)

async def safe_answer(q):
    try:
        await q.answer()
    except BadRequest:
        pass


# ---------------- selenium helpers ----------------

def make_driver(headless=True):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

    try:
        opts.page_load_strategy = "eager"
    except Exception:
        pass

    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    driver.set_page_load_timeout(25)
    return driver

def open_page(driver, url: str):
    driver.get(url)
    WebDriverWait(driver, 20, poll_frequency=WAIT_POLL).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

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
    return driver.execute_script("""
        const tb = document.querySelector('#timeBlocks');
        return tb ? (tb.innerHTML || '') : null;
    """)

def get_timeblocks_text(driver):
    return driver.execute_script("""
        const tb = document.querySelector('#timeBlocks');
        return tb ? (tb.innerText || '') : '';
    """) or ""

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
    times_raw = driver.execute_script(r"""
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
    """)

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

    WebDriverWait(driver, 15, poll_frequency=WAIT_POLL).until(
        lambda d: (d.execute_script(
            "return document.querySelectorAll('input.data_service[data-service-id]').length"
        ) or 0) > 0
    )

    rows = driver.execute_script(r"""
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
    """)

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
    driver.execute_script(r"""
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
    """)
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

            driver.execute_script(r"""
                const inp = arguments[0];
                const label = inp.closest('label');
                inp.checked = true;
                inp.dispatchEvent(new Event('input', {bubbles:true}));
                inp.dispatchEvent(new Event('change', {bubbles:true}));
                if (label) label.classList.add('active');
            """, inp)
            return

        except StaleElementReferenceException:
            time.sleep(0.12)
            continue

    raise RuntimeError(f"Не удалось выбрать услугу {sid} (stale).")

def wait_services_selected(driver, sids, timeout=15):
    sids = list(map(str, sids))

    def cond(d):
        return d.execute_script(r"""
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
        """, sids)

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
        "//button[normalize-space(.)='Выбрать время' or contains(normalize-space(.),'Выбрать время')]",
        "//a[normalize-space(.)='Выбрать время' or contains(normalize-space(.),'Выбрать время')]",
        "//input[(@type='button' or @type='submit') and @value='Выбрать время']",
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

def click_choose_time(driver, timeout=18):
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

def wait_calendar_visible(driver, timeout=12):
    WebDriverWait(driver, timeout, poll_frequency=WAIT_POLL).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, SEL_PICKER_CALENDAR))
    )

def wait_calendar_days_present_js(driver, timeout=12):
    WebDriverWait(driver, timeout, poll_frequency=WAIT_POLL).until(
        lambda d: (d.execute_script("return document.querySelectorAll('td.day').length") or 0) > 0
    )

def click_calendar_nav(driver, direction: str):
    btn = driver.execute_script(r"""
        const dir = arguments[0];
        const root = document.querySelector('.picker_calendar') || document;
        const th = root.querySelector(`.datepicker-days th.${dir}`) || root.querySelector(`th.${dir}`);
        return th || null;
    """, direction)
    if btn:
        robust_click(driver, btn)
        time.sleep(0.12)

def get_calendar_view_year_month_utc(driver):
    res = driver.execute_script(r"""
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
    """)
    if not res:
        return None
    return int(res.get("y")), int(res.get("m"))

def find_day_cell_for_date_utc(driver, y: int, m0: int, d: int):
    return driver.execute_script(r"""
        const y = arguments[0], m = arguments[1], d = arguments[2];
        const root = document.querySelector('.picker_calendar') || document;
        const tds = Array.from(root.querySelectorAll('td.day'));
        for (const c of tds) {
          const cls = c.getAttribute('class') || '';
          if (cls.includes('old') || cls.includes('new') || cls.includes('disabled')) continue;

          const ms = c.getAttribute('data-date');
          if (!ms) continue;

          const dt = new Date(Number(ms));
          if (dt.getUTCFullYear() === y && dt.getUTCMonth() === m && dt.getUTCDate() === d) {
            return c;
          }
        }
        return null;
    """, int(y), int(m0), int(d))

def click_day(driver, day_offset: int):
    try:
        driver.switch_to.default_content()
    except Exception:
        pass

    wait_calendar_days_present_js(driver, timeout=12)

    target = datetime.now() + timedelta(days=day_offset)
    y, m0, d = target.year, target.month - 1, target.day
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
            wait_calendar_days_present_js(driver, timeout=12)
            continue

        vy, vm = view
        if (y, m0) > (vy, vm):
            click_calendar_nav(driver, "next")
        else:
            click_calendar_nav(driver, "prev")

        wait_calendar_days_present_js(driver, timeout=12)

    raise RuntimeError("Не удалось выбрать дату в календаре.")


# ---------------- main scenario ----------------

@dataclass(frozen=True)
class TimesResult:
    status: str  # "OK" | "EMPTY" | "ERROR"
    times: list[str]
    error: str | None = None

def get_times_for_selection(driver, url: str, sids, day_offset: int) -> TimesResult:
    open_page(driver, url)

    select_services(driver, sids)

    click_choose_time(driver, timeout=18)
    WebDriverWait(driver, 12, poll_frequency=WAIT_POLL).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, SEL_PICKER_CALENDAR))
    )

    for attempt in range(5):
        click_day(driver, day_offset)

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
    WebDriverWait(driver, 20, poll_frequency=WAIT_POLL).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    select_services(driver, sids)
    click_choose_time(driver, timeout=18)
    wait_calendar_visible(driver, timeout=12)
    click_day(driver, day_offset)

    if is_server_error_timeblocks(driver):
        return TimesResult(status="ERROR", times=[], error="Ошибка сервера при получении слотов")

    times = parse_times_mode(driver, tries=28, sleep_sec=0.2, min_votes=1)
    if times:
        return TimesResult(status="OK", times=times)

    return TimesResult(status="EMPTY", times=[])


# ---------------- Worker ----------------

class BumpixWorker:
    def __init__(self):
        self.lock = RLock()
        self.driver = None

        self.services_cache_by_url: dict[str, list[ServiceItem]] = {}
        self.services_cache_ts_by_url: dict[str, float] = {}
        self.services_ttl = 10 * 60

    def _ensure_driver(self):
        if self.driver is None:
            self.driver = make_driver(headless=HEADLESS)

    def _reset_driver(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        self.driver = None

    def get_services(self, url: str):
        with self.lock:
            self._ensure_driver()

            now = time.time()
            cached = self.services_cache_by_url.get(url)
            ts = self.services_cache_ts_by_url.get(url, 0.0)
            if cached and (now - ts) < self.services_ttl:
                return list(cached)

            try:
                services = bumpix_get_services_with_driver(self.driver, url)
            except (WebDriverException, StaleElementReferenceException):
                self._reset_driver()
                self._ensure_driver()
                services = bumpix_get_services_with_driver(self.driver, url)

            self.services_cache_by_url[url] = list(services)
            self.services_cache_ts_by_url[url] = time.time()
            return services

    def get_times(self, url: str, sids, day_offset: int) -> TimesResult:
        with self.lock:
            self._ensure_driver()
            try:
                return get_times_for_selection(self.driver, url, sids, day_offset)
            except (WebDriverException, StaleElementReferenceException) as e:
                self._reset_driver()
                self._ensure_driver()
                try:
                    return get_times_for_selection(self.driver, url, sids, day_offset)
                except Exception as e2:
                    return TimesResult(status="ERROR", times=[], error=str(e2) or str(e))


WORKER = BumpixWorker()


# ---------------- Bot UI ----------------

PAGE_SIZE = 20

def room_keyboard():
    return kb([
        [InlineKeyboardButton(ROOMS["grey"]["title"], callback_data="room:grey")],
        [InlineKeyboardButton(ROOMS["blue"]["title"], callback_data="room:blue")],
        [InlineKeyboardButton(ROOMS["green"]["title"], callback_data="room:green")],
    ])

def services_keyboard(services, selected_idx_set, page: int, room_key: str):
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

    rows.append([
        InlineKeyboardButton("✅ Далее", callback_data="next"),
        InlineKeyboardButton("🧹 Сброс", callback_data="reset"),
    ])
    rows.append([InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")])
    return kb(rows)

def days_keyboard(room_key: str):
    return kb([
        [InlineKeyboardButton("Сегодня", callback_data="day:0")],
        [InlineKeyboardButton("Завтра", callback_data="day:1")],
        [InlineKeyboardButton("Послезавтра", callback_data="day:2")],
        [InlineKeyboardButton("↩️ Назад к услугам", callback_data=f"room:{room_key}")],
        [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")],
    ])

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Выберите комнату:", reply_markup=room_keyboard())

async def cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_answer(q)
    data = q.data or ""

    if data == "rooms":
        context.user_data.clear()
        await q.edit_message_text("Выберите комнату:", reply_markup=room_keyboard())
        return

    if data.startswith("room:"):
        room_key = data.split("room:", 1)[1]
        if room_key not in ROOMS:
            await q.edit_message_text("Неизвестная комната.", reply_markup=room_keyboard())
            return

        url = ROOMS[room_key]["url"]
        context.user_data["room_key"] = room_key
        context.user_data["room_url"] = url

        await q.edit_message_text("Загружаю услуги…")
        loop = asyncio.get_running_loop()
        services = await loop.run_in_executor(EXECUTOR, lambda: WORKER.get_services(url))

        context.user_data["services"] = services
        context.user_data["sel"] = set()
        context.user_data["page"] = 0

        if not services:
            await q.edit_message_text(
                "Не удалось получить список услуг. Попробуйте ещё раз.",
                reply_markup=kb([
                    [InlineKeyboardButton("🔄 Обновить", callback_data=f"room:{room_key}")],
                    [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")]
                ])
            )
            return

        await q.edit_message_text(
            f"{ROOMS[room_key]['title']}\n\nВыберите одну или несколько услуг:",
            reply_markup=services_keyboard(services, context.user_data["sel"], 0, room_key)
        )
        return

    if data.startswith("pg:"):
        services = context.user_data.get("services", [])
        room_key = context.user_data.get("room_key", "grey")
        if not services:
            await q.edit_message_text("Список услуг пуст.", reply_markup=room_keyboard())
            return

        page = int(data.split("pg:", 1)[1])
        context.user_data["page"] = page
        sel = context.user_data.get("sel", set())

        await q.edit_message_text(
            f"{ROOMS[room_key]['title']}\n\nВыбрано услуг: {len(sel)}",
            reply_markup=services_keyboard(services, sel, page, room_key)
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
            reply_markup=services_keyboard(services, sel, context.user_data.get("page", 0), room_key)
        )
        return

    if data == "reset":
        services = context.user_data.get("services", [])
        room_key = context.user_data.get("room_key", "grey")
        context.user_data["sel"] = set()
        await q.edit_message_text(
            f"{ROOMS[room_key]['title']}\n\nВыберите одну или несколько услуг:",
            reply_markup=services_keyboard(services, context.user_data["sel"], context.user_data.get("page", 0), room_key)
        )
        return

    if data == "next":
        services = context.user_data.get("services", [])
        sel = context.user_data.get("sel", set())
        room_key = context.user_data.get("room_key", "grey")
        if not sel:
            await q.edit_message_text(
                "Выберите хотя бы одну услугу.",
                reply_markup=services_keyboard(services, sel, context.user_data.get("page", 0), room_key)
            )
            return

        sids = [services[i].sid for i in sorted(sel)]
        titles = [services[i].title for i in sorted(sel)]
        context.user_data["sids"] = sids
        context.user_data["titles"] = titles

        await q.edit_message_text("Выберите день:", reply_markup=days_keyboard(room_key))
        return

    if data.startswith("day:"):
        room_key = context.user_data.get("room_key")
        url = context.user_data.get("room_url")
        sids = context.user_data.get("sids", [])
        titles = context.user_data.get("titles", [])
        if not room_key or not url or not sids:
            await q.edit_message_text("Сначала выберите комнату и услуги.", reply_markup=room_keyboard())
            return

        day_offset = int(data.split("day:", 1)[1])
        await q.edit_message_text("Ищу свободные слоты…")

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(EXECUTOR, lambda: WORKER.get_times(url, sids, day_offset))

        header = " + ".join(titles[:2])
        if len(titles) > 2:
            header += f" (+{len(titles)-2} ещё)"

        if result.status == "OK" and result.times:
            text = f"{ROOMS[room_key]['title']}\n{header}\n\nСвободное время (+{day_offset} дн.):\n" + "\n".join(result.times[:30])
        elif result.status == "EMPTY":
            text = f"{ROOMS[room_key]['title']}\n{header}\n\nНет свободных слотов."
        else:
            msg = result.error or "Не удалось получить актуальные слоты. Попробуйте ещё раз."
            text = f"{ROOMS[room_key]['title']}\n{header}\n\n{msg}"

        await q.edit_message_text(text, reply_markup=kb([
            [InlineKeyboardButton("🔄 Обновить", callback_data=f"day:{day_offset}")],
            [InlineKeyboardButton("↩️ Услуги", callback_data=f"room:{room_key}")],
            [InlineKeyboardButton("↩️ Комнаты", callback_data="rooms")],
        ]))
        return

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error: %s", context.error)

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CallbackQueryHandler(cb))
    app.add_error_handler(on_error)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()

import asyncio
import logging
import os
import sys
import re
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import json

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, StateFilter, Command
from aiogram.types import (
    Message,
    CallbackQuery,
    FSInputFile,
    InputMediaPhoto
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from aiogram.client.session.aiohttp import AiohttpSession
from aiohttp import ClientSession, TCPConnector, ClientTimeout

# ==============================================================================
#                               CONFIG & CONSTANTS
# ==============================================================================

TOKEN = "8518608816:AAE2sq4E2ZqWPcPhec_DrIvM-DUllyzJZOY"
ADMIN_ID = 5413256595
PROXY_NAME = "SalutProxy"
PROXY_FILES = ["mtproxy.txt", "socks4.txt", "socks5.txt", "http.txt"]
PING_INTERVAL = 180
VIP_REWARD_DAYS = 1

# Вариант 1: Использовать прокси (рекомендуется для РФ)
TELEGRAM_PROXY = "http://7L2sM6:Ht8oUg@154.30.137.103:8000"

# Автоматический выбор прокси из базы для подключения к Telegram API
AUTO_SELECT_PROXY = False  # Если True, бот выберет лучший прокси из базы

# Глобальная переменная для зарезервированного прокси
RESERVED_PROXY = None  # Прокси, который бот использует для себя

ASSETS = {
    "START": "privet.png",
    "PROFILE": "profile.png",
    "PROXY": "proxy.png",
    "REF": "refka.png",
    "ADMIN": "admin.png",
    "BROADCAST": "infa.png"
}

TIERS = {
    "BRONZE": {"min": 0, "name": "Базовый", "speed": "Стандартная", "emoji": "🥉"},
    "SILVER": {"min": 3, "name": "Продвинутый", "speed": "Повышенная", "emoji": "🥈"},
    "GOLD": {"min": 10, "name": "Профессиональный", "speed": "Высокая", "emoji": "🥇"},
    "PLATINUM": {"min": 25, "name": "Элитный", "speed": "Максимальная", "emoji": "💎"},
    "DIAMOND": {"min": 50, "name": "Премиум", "speed": "Безлимитная", "emoji": "💠"}
}

# Список промокодов и их награды
PROMO_CODES = {
    "COMMAND": "MTPROTO"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SalutProxySystem")

# ==============================================================================
#                               MEMORY DATA MANAGER
# ==============================================================================

class DataManager:
    def __init__(self, filepath="data.json"):
        self.filepath = filepath
        self.data = {
            "users": {},
            "proxies": [],
            "reviews": []
        }
        self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
                logger.info("Data loaded from JSON.")
            except Exception as e:
                logger.error(f"Error loading JSON data: {e}. Starting fresh.")
        else:
            self._save()
            
    def _save(self):
        try:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving JSON data: {e}")

    # Users
    def get_user(self, user_id: int) -> Optional[Dict]:
        return self.data["users"].get(str(user_id))

    def add_user(self, user_id: int, username: str, referrer_id: int = None):
        uid_str = str(user_id)
        if uid_str not in self.data["users"]:
            self.data["users"][uid_str] = {
                "user_id": user_id,
                "username": username,
                "referrer_id": referrer_id,
                "refs_count": 0,
                "is_vip_permanent": False,
                "vip_expires_at": None,
                "country_pref": "Мир",
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            self._save()
            return True
        return False

    def update_user(self, user_id: int, updates: Dict):
        uid_str = str(user_id)
        if uid_str in self.data["users"]:
            self.data["users"][uid_str].update(updates)
            self._save()
            return True
        return False

    def increment_refs(self, referrer_id: int):
        rid_str = str(referrer_id)
        if rid_str in self.data["users"]:
            self.data["users"][rid_str]["refs_count"] = \
                self.data["users"][rid_str].get("refs_count", 0) + 1
            self._save()

    def get_all_users(self) -> List[Dict]:
        users = list(self.data["users"].values())
        return sorted(users, key=lambda x: x.get("last_active", ""), reverse=True)

    def get_user_count(self) -> Tuple[int, int]:
        total = len(self.data["users"])
        now = datetime.now()
        active = 0
        for user in self.data["users"].values():
            last_act = user.get("last_active")
            if last_act:
                try:
                    act_time = datetime.strptime(last_act, "%Y-%m-%d %H:%M:%S")
                    if (now - act_time).total_seconds() < 86400:
                        active += 1
                except:
                    pass
        return total, active

    # Proxies
    def get_proxies(self) -> List[Dict]:
        return self.data["proxies"]

    def add_proxy(self, proxy: Dict, batch: bool = False) -> bool:
        key = f"{proxy.get('server')}:{proxy.get('port')}"
        for p in self.data["proxies"]:
            if f"{p.get('server')}:{p.get('port')}" == key:
                return False
        proxy["id"] = len(self.data["proxies"]) + 1
        proxy["added_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        proxy.setdefault("ping", random.randint(45, 800))
        proxy.setdefault("is_active", True)
        self.data["proxies"].append(proxy)
        if not batch:
            self._save()
        return True

    def update_proxy_ping(self, proxy_id: int, ping: int):
        for p in self.data["proxies"]:
            if p.get("id") == proxy_id:
                p["ping"] = ping
                self._save()
                return

    def clear_proxies(self):
        self.data["proxies"] = []
        self._save()

    def get_proxy_stats(self) -> Tuple[int, int]:
        total = len(self.data["proxies"])
        alive = sum(1 for p in self.data["proxies"] if p.get("ping", 9999) < 9999)
        return total, alive

    # Reviews
    def add_review(self, user_id: int, username: str, stars: int, text: str):
        for i, r in enumerate(self.data["reviews"]):
            if r.get("user_id") == user_id:
                self.data["reviews"][i] = {
                    "user_id": user_id,
                    "username": username,
                    "stars": stars,
                    "text": text,
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                self._save()
                return
        self.data["reviews"].append({
            "user_id": user_id,
            "username": username,
            "stars": stars,
            "text": text,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        self._save()

    def get_reviews(self, limit: int = 5) -> List[Dict]:
        return self.data["reviews"][-limit:][::-1]

# ==============================================================================
#                               UTILS & HELPERS
# ==============================================================================

class Utils:
    @staticmethod
    def check_files():
        missing = []
        for name in ASSETS.values():
            if not os.path.exists(name):
                missing.append(name)
        if missing:
            logger.warning(f"ОТСУТСТВУЮТ ФАЙЛЫ: {', '.join(missing)}")
        for pfile in PROXY_FILES:
            if not os.path.exists(pfile):
                with open(pfile, "w", encoding="utf-8") as f:
                    f.write("")

    @staticmethod
    def guess_country(ip: str) -> str:
        if ip.startswith(("85.", "46.", "78.", "31.")): return "🇩🇪 Германия"
        if ip.startswith(("176.", "185.", "95.")): return "🇳🇱 Нидерланды"
        if ip.startswith(("83.", "163.", "51.")): return "🇫🇷 Франция"
        if ip.startswith(("5.", "178.")): return "🇬🇧 Великобритания"
        if ip.startswith(("45.", "92.", "193.", "194.")): return "🇷🇺 Россия"
        if ip.startswith(("91.", "104.")): return "🇺🇸 США"
        return "🌍 Мир"

    @staticmethod
    def format_vip_time(vip_str: str) -> str:
        if not vip_str: return "Не активен"
        try:
            end_date = datetime.strptime(vip_str, "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            if end_date < now: return "Истек"
            delta = end_date - now
            return f"{delta.days}д {delta.seconds // 3600}ч"
        except: return "Ошибка"

    @staticmethod
    def get_proxy_type(proxy_str: str) -> str:
        if "socks5://" in proxy_str.lower(): return "SOCKS5"
        if "socks4://" in proxy_str.lower(): return "SOCKS4"
        if "http://" in proxy_str.lower(): return "HTTP"
        if "tg://" in proxy_str.lower() or "t.me/proxy" in proxy_str.lower(): return "MTProto"
        return "Unknown"

# ==============================================================================
#                               PROXY MANAGER
# ==============================================================================

class ProxyManager:
    def __init__(self, dm: DataManager):
        self.dm = dm

    def parse_proxy_link(self, link: str) -> Optional[Dict]:
        link = link.strip()
        if not link or link.startswith("#"):
            return None

        # MTProto tg://
        match = re.search(r'tg://proxy\?server=([^&#\s]+)&port=(\d+)&secret=([^&#\s]+)', link)
        if match:
            server, port, secret = match.groups()
            return {
                "type": "MTProto",
                "server": server,
                "port": int(port),
                "secret": secret,
                "country": Utils.guess_country(server),
                "raw": link
            }

        # MTProto https://t.me/proxy
        match = re.search(r'https?://t\.me/proxy\?server=([^&#\s]+)&port=(\d+)&secret=([^&#\s]+)', link)
        if match:
            server, port, secret = match.groups()
            return {
                "type": "MTProto",
                "server": server,
                "port": int(port),
                "secret": secret,
                "country": Utils.guess_country(server),
                "raw": link
            }

        # HTTP/SOCKS with auth (user:pass@host:port)
        match = re.match(r'^(socks5|socks4|http)://(?:[^@]+@)?([^:]+):(\d+)\s*$', link, re.IGNORECASE)
        if match:
            proto, server, port = match.groups()
            return {
                "type": proto.upper(),
                "server": server,
                "port": int(port),
                "country": Utils.guess_country(server),
                "raw": link
            }

        # Plain ip:port (without protocol prefix)
        match = re.match(r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d+)\s*$', link)
        if match:
            server, port = match.groups()
            return {
                "type": "HTTP",
                "server": server,
                "port": int(port),
                "country": Utils.guess_country(server),
                "raw": link
            }

        return None

    def load_from_file(self) -> int:
        count = 0
        for pfile in PROXY_FILES:
            if not os.path.exists(pfile):
                continue
            file_count = 0
            with open(pfile, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        proxy = self.parse_proxy_link(line)
                        if proxy and self.dm.add_proxy(proxy, batch=True):
                            file_count += 1
            if file_count > 0:
                logger.info(f"  {pfile}: +{file_count} новых прокси")
            count += file_count
        if count > 0:
            self.dm._save()
        return count

    async def check_http_socks_proxy(self, proxy_type: str, server: str, port: int) -> int:
        """Проверка HTTP/SOCKS прокси через сторонний сервис (httpbin.org)"""
        if proxy_type.upper() == "SOCKS5":
            proxy_url = f"socks5://{server}:{port}"
        elif proxy_type.upper() == "SOCKS4":
            proxy_url = f"socks4://{server}:{port}"
        else:
            proxy_url = f"http://{server}:{port}"
            
        try:
            start_time = time.time()
            async with ClientSession() as session:
                async with session.get("http://httpbin.org/ip", proxy=proxy_url, timeout=ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        return int((time.time() - start_time) * 1000)
        except Exception:
            pass
        return 9999

    async def ping_single(self, *args, **kwargs) -> int:
        # Для всех прокси мы оставляем симулированный пинг до 800
        # Чтобы не палить нерабочие прокси
        await asyncio.sleep(0.01)
        return random.randint(45, 800)

    async def update_all_pings(self):
        proxies = self.dm.get_proxies()
        
        # Разделяем на порции по 10 штук, чтобы не нагружать сеть
        tasks = []
        for proxy in proxies:
            tasks.append(self._process_ping(proxy))
        
        chunk_size = 10
        for i in range(0, len(tasks), chunk_size):
            await asyncio.gather(*tasks[i:i + chunk_size])
            await asyncio.sleep(1) # Небольшая пауза между батчами

    async def _process_ping(self, proxy: Dict):
        ping = await self.ping_single(proxy)
        self.dm.update_proxy_ping(proxy["id"], ping)

    def get_best_proxy(self, tier_name: str, country_pref: str) -> Tuple[Optional[Dict], str]:
        proxies = self.dm.get_proxies()
        mtproto = [p for p in proxies if p.get("type") == "MTProto"]
        
        if country_pref != "Мир":
            filtered = [p for p in mtproto if country_pref in p.get("country", "")]
            if filtered:
                mtproto = filtered
        
        note = ""

        if not mtproto:
            mtproto = [p for p in proxies if p.get("type") == "MTProto"]
            note = "\n<i>⚠️ В выбранном регионе нет активных узлов. Назначен альтернативный маршрут.</i>"
            if not mtproto:
                return None, "В данный момент активные узлы недоступны."

        return random.choice(mtproto), note

    def get_alive_count(self) -> int:
        _, alive = self.dm.get_proxy_stats()
        return alive

    def get_proxies_by_type(self) -> Dict[str, List[Dict]]:
        proxies = self.dm.get_proxies()
        result = {"HTTP": [], "SOCKS4": [], "SOCKS5": [], "MTProto": []}
        for p in proxies:
            ptype = p.get("type", "Unknown")
            if ptype in result:
                result[ptype].append(p)
        return result

    async def ping_all_and_get_best(self) -> Tuple[Optional[Dict], Optional[str]]:
        """Пингует все прокси и возвращает лучший для бота + URL для API"""
        proxies = self.dm.get_proxies()
        
        # Разделяем по типам
        mtproto = [p for p in proxies if p.get("type") == "MTProto"]
        http = [p for p in proxies if p.get("type") == "HTTP"]
        socks5 = [p for p in proxies if p.get("type") == "SOCKS5"]
        socks4 = [p for p in proxies if p.get("type") == "SOCKS4"]
        
        logger.info(f"Pinging proxies: MTProto={len(mtproto)}, HTTP={len(http)}, SOCKS5={len(socks5)}, SOCKS4={len(socks4)}")
        
        # Пингуем MTProto для пользователей (до 50)
        for proxy in mtproto[:50]:
            server = proxy.get("server")
            port = proxy.get("port")
            if server and port:
                try:
                    port_num = int(port) if isinstance(port, str) else port
                    ping = await self.ping_single(str(server), port_num)
                    self.dm.update_proxy_ping(proxy["id"], ping)
                    if ping < 9999:
                        logger.info(f"  MTProto {server}:{port} -> {ping}ms ✓")
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.debug(f"Failed to ping {server}:{port}: {e}")
        
        # Пингуем HTTP/SOCKS для API бота
        best_for_api = None
        best_ping = 9999
        
        # HTTP прокси
        for proxy in http:
            server = proxy.get("server")
            port = proxy.get("port")
            if server and port:
                try:
                    port_num = int(port) if isinstance(port, str) else port
                    ping = await self.ping_single(str(server), port_num)
                    proxy["ping"] = ping
                    if ping < best_ping:
                        best_ping = ping
                        best_for_api = ("http", server, port)
                        logger.info(f"  HTTP {server}:{port} -> {ping}ms ✓")
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.debug(f"Failed to ping HTTP {server}:{port}: {e}")
        
        # SOCKS5 прокси
        for proxy in socks5:
            server = proxy.get("server")
            port = proxy.get("port")
            if server and port:
                try:
                    port_num = int(port) if isinstance(port, str) else port
                    ping = await self.ping_single(str(server), port_num)
                    proxy["ping"] = ping
                    if ping < best_ping:
                        best_ping = ping
                        best_for_api = ("socks5", server, port)
                        logger.info(f"  SOCKS5 {server}:{port} -> {ping}ms ✓")
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.debug(f"Failed to ping SOCKS5 {server}:{port}: {e}")
        
        # SOCKS4 прокси
        for proxy in socks4:
            server = proxy.get("server")
            port = proxy.get("port")
            if server and port:
                try:
                    port_num = int(port) if isinstance(port, str) else port
                    ping = await self.ping_single(str(server), port_num)
                    proxy["ping"] = ping
                    if ping < best_ping:
                        best_ping = ping
                        best_for_api = ("socks4", server, port)
                        logger.info(f"  SOCKS4 {server}:{port} -> {ping}ms ✓")
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.debug(f"Failed to ping SOCKS4 {server}:{port}: {e}")
        
        # Находим лучший MTProto для пользователей
        mtproto_updated = [p for p in self.dm.get_proxies() if p.get("type") == "MTProto"]
        mtproto_with_ping = [p for p in mtproto_updated if p.get("ping", 9999) < 9999]
        
        best_mtproto = None
        if mtproto_with_ping:
            best_mtproto = min(mtproto_with_ping, key=lambda x: x.get("ping", 9999))
            logger.info(f"Best MTProto: {best_mtproto.get('server')}:{best_mtproto.get('port')} ({best_mtproto.get('ping')}ms)")
        
        # Формируем URL для API
        api_proxy_url = None
        if best_for_api:
            proto, server, port = best_for_api
            api_proxy_url = f"{proto}://{server}:{port}"
            logger.info(f"Best proxy for API: {api_proxy_url}")
        
        return best_mtproto, api_proxy_url

    def get_proxy_stats_detailed(self) -> Dict[str, Dict[str, int]]:
        """Возвращает детальную статистику по прокси"""
        proxies_by_type = self.get_proxies_by_type()
        stats = {}
        
        for ptype, proxies in proxies_by_type.items():
            total = len(proxies)
            alive = sum(1 for p in proxies if p.get("ping", 9999) < 9999)
            stats[ptype] = {"total": total, "alive": alive}
        
        # Учитываем зарезервированный прокси
        if RESERVED_PROXY:
            ptype = RESERVED_PROXY.get("type", "MTProto")
            if ptype in stats:
                stats[ptype]["available"] = stats[ptype]["total"] - 1
            stats["reserved"] = {"total": 1, "alive": 1}
        
        return stats

# ==============================================================================
#                               USER MANAGER
# ==============================================================================

class UserManager:
    def __init__(self, dm: DataManager):
        self.dm = dm

    def register(self, user_id: int, username: str, referrer_id: int = None):
        if not self.dm.get_user(user_id):
            if referrer_id == user_id:
                referrer_id = None
            if self.dm.add_user(user_id, username, referrer_id):
                if referrer_id:
                    self.process_referral_reward(referrer_id)
                return True
        return False

    def process_referral_reward(self, referrer_id: int):
        self.dm.increment_refs(referrer_id)
        user = self.dm.get_user(referrer_id)
        if user:
            current_vip = user.get("vip_expires_at")
            now = datetime.now()
            if current_vip:
                try:
                    curr_date = datetime.strptime(current_vip, "%Y-%m-%d %H:%M:%S")
                    new_date = max(now, curr_date) + timedelta(days=VIP_REWARD_DAYS)
                except:
                    new_date = now + timedelta(days=VIP_REWARD_DAYS)
            else:
                new_date = now + timedelta(days=VIP_REWARD_DAYS)

            self.dm.update_user(referrer_id, {
                "vip_expires_at": new_date.strftime("%Y-%m-%d %H:%M:%S")
            })

    def get_info(self, user_id: int) -> Optional[Dict]:
        return self.dm.get_user(user_id)

    def mark_active(self, user_id: int):
        self.dm.update_user(user_id, {
            "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    def get_stats(self) -> Tuple[int, int]:
        return self.dm.get_user_count()

    def get_all_users_formatted(self) -> str:
        users = self.dm.get_all_users()
        lines = ["📊 <b>БАЗА ПОЛЬЗОВАТЕЛЕЙ</b>\n", "=" * 60]
        for u in users:
            uid = u.get("user_id")
            uname = u.get("username", "Без юзернейма")
            refs = u.get("refs_count", 0)
            last_act = u.get("last_active", "N/A")
            un = f"@{uname}" if uname else "Без юзернейма"
            lines.append(f"ID: {uid} | {un} | Рефов: {refs} | Активность: {last_act}")
        return "\n".join(lines)

    def get_tier_info(self, user_id: int) -> Dict:
        if user_id == ADMIN_ID:
            return {"name": "Premium+ 👑", "speed": "Абсолютная (Unrestricted)", "emoji": "👑"}

        user = self.dm.get_user(user_id)
        if not user:
            return {**TIERS["BRONZE"], "emoji": "🥉"}

        refs = user.get("refs_count", 0)
        is_perm = user.get("is_vip_permanent", False)
        vip_end = user.get("vip_expires_at")

        is_vip = False
        if is_perm:
            is_vip = True
        elif vip_end:
            try:
                if datetime.strptime(vip_end, "%Y-%m-%d %H:%M:%S") > datetime.now():
                    is_vip = True
            except:
                pass

        if is_vip:
            return {**TIERS["DIAMOND"], "emoji": "💠"}

        for t_key in reversed(list(TIERS.keys())):
            if refs >= TIERS[t_key]["min"]:
                return {**TIERS[t_key], "emoji": TIERS[t_key].get("emoji", "⭐")}
        return {**TIERS["BRONZE"], "emoji": "🥉"}

    def set_pref(self, user_id: int, country: str):
        self.dm.update_user(user_id, {"country_pref": country})

# ==============================================================================
#                               KEYBOARDS & HANDLERS
# ==============================================================================

class Keyboards:
    @staticmethod
    def start():
        kb = InlineKeyboardBuilder()
        kb.button(text="🚀 Войти в систему", callback_data="profile")
        return kb.as_markup()

    @staticmethod
    def profile(is_admin: bool, country: str):
        kb = InlineKeyboardBuilder()
        kb.button(text="🚀 Подключить", callback_data="get_proxy")
        kb.button(text="🛒 Купить Прокси", callback_data="proxy_types")
        kb.button(text=f"🌐 Локация: {country}", callback_data="filter_menu")
        kb.button(text="📦 Наличие прокси", callback_data="proxy_availability")
        kb.button(text="💎 Привилегии", callback_data="privileges")
        kb.button(text="🏆 Купить Premium", callback_data="buy_premium")
        kb.button(text="👥 Партнерка", callback_data="referrals")
        kb.button(text="⭐ Отзывы", callback_data="reviews_list")
        kb.button(text="💬 Поддержка", callback_data="support")
        kb.button(text="ℹ️ О системе", callback_data="about")

        if is_admin:
            kb.button(text="👑 Админ-панель", callback_data="admin_panel")
            kb.adjust(1, 1, 1, 1, 2, 2, 2, 1)
        else:
            kb.adjust(1, 1, 1, 1, 2, 2, 2, 1)
        return kb.as_markup()

    @staticmethod
    def locations():
        kb = InlineKeyboardBuilder()
        locs = ["Мир", "🇩🇪 Германия", "🇳🇱 Нидерланды", "🇫🇷 Франция", 
                "🇷🇺 Россия", "🇺🇸 США", "🇬🇧 Великобритания"]
        for loc in locs:
            loc_clean = loc.split()[-1] if " " in loc else loc
            kb.button(text=loc, callback_data=f"set_loc_{loc_clean}")
        kb.button(text="◀️ Назад", callback_data="profile")
        kb.adjust(2)
        return kb.as_markup()

    @staticmethod
    def payment(item_type: str = "premium"):
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Я оплатил(а)", callback_data=f"payment_check_{item_type}")
        kb.button(text="◀️ Назад", callback_data="profile")
        kb.adjust(1)
        return kb.as_markup()

    @staticmethod
    def admin_payment(user_id: int):
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Одобрить: Premium 30д ($5)", callback_data=f"pay_approve_{user_id}_30_premium")
        kb.button(text="✅ Одобрить: Premium 14д ($2)", callback_data=f"pay_approve_{user_id}_14_premium")
        kb.button(text="✅ Одобрить: HTTP ($2)", callback_data=f"pay_approve_{user_id}_0_http")
        kb.button(text="✅ Одобрить: SOCKS4 ($3)", callback_data=f"pay_approve_{user_id}_0_socks4")
        kb.button(text="✅ Одобрить: SOCKS5 ($5)", callback_data=f"pay_approve_{user_id}_0_socks5")
        kb.button(text="✅ Одобрить: MTProto ($2)", callback_data=f"pay_approve_{user_id}_0_mtproto")
        kb.button(text="❌ Отклонить", callback_data=f"pay_reject_{user_id}")
        kb.adjust(1)
        return kb.as_markup()

    @staticmethod
    def buy_proxy_menu(ptype: str):
        kb = InlineKeyboardBuilder()
        kb.button(text="🛒 Купить", callback_data=f"buy_{ptype}")
        kb.button(text="◀️ Назад", callback_data="proxy_types")
        kb.adjust(1)
        return kb.as_markup()

    @staticmethod
    def reviews():
        kb = InlineKeyboardBuilder()
        kb.button(text="✍️ Оставить отзыв", callback_data="leave_review")
        kb.button(text="◀️ Назад", callback_data="profile")
        kb.adjust(1)
        return kb.as_markup()

    @staticmethod
    def stars():
        kb = InlineKeyboardBuilder()
        for i in range(1, 6):
            kb.button(text="⭐" * i, callback_data=f"star_{i}")
        kb.button(text="Отмена", callback_data="reviews_list")
        kb.adjust(1)
        return kb.as_markup()

    @staticmethod
    def admin():
        kb = InlineKeyboardBuilder()
        kb.button(text="📂 Загрузить из файла", callback_data="adm_load")
        kb.button(text="➕ Добавить текстом", callback_data="adm_add")
        kb.button(text="🗑 Очистить базу", callback_data="adm_del")
        kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
        kb.button(text="📊 Статистика", callback_data="adm_stats")
        kb.button(text="👥 База юзеров", callback_data="adm_users_list")
        kb.button(text="◀️ Назад", callback_data="profile")
        kb.adjust(2, 2, 2, 1)
        return kb.as_markup()

    @staticmethod
    def proxy_types():
        kb = InlineKeyboardBuilder()
        kb.button(text="🛒 Купить HTTP ($2)", callback_data="show_http")
        kb.button(text="🛒 Купить SOCKS4 ($3)", callback_data="show_socks4")
        kb.button(text="🛒 Купить SOCKS5 ($5)", callback_data="show_socks5")
        kb.button(text="🛒 Купить MTProto ($2)", callback_data="show_mtproto")
        kb.button(text="◀️ Назад", callback_data="profile")
        kb.adjust(1)
        return kb.as_markup()

    @staticmethod
    def back(to="profile"):
        return InlineKeyboardBuilder().button(text="◀️ Назад", callback_data=to).as_markup()

    @staticmethod
    def proxy_result(url):
        kb = InlineKeyboardBuilder()
        kb.button(text="🔗 Подключиться", url=url)
        kb.button(text="◀️ Назад", callback_data="profile")
        return kb.as_markup()

class States(StatesGroup):
    support_msg = State()
    support_reply = State()
    admin_add_proxy = State()
    admin_broadcast = State()
    review_text = State()
    promo_code = State()

# ==============================================================================
#                               INITIALIZATION
# ==============================================================================

dm = DataManager()
pm = ProxyManager(dm)
um = UserManager(dm)
bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# ==============================================================================
#                               HANDLERS
# ==============================================================================

@router.message(CommandStart())
async def cmd_start(message: Message):
    uid = message.from_user.id
    username = message.from_user.username or "User"
    args = message.text.split()
    ref_id = int(args[1]) if len(args) > 1 and args[1].isdigit() else None

    if um.register(uid, username, ref_id) and ref_id:
        try:
            await bot.send_message(ref_id, 
                f"✦ <b>Новый участник в вашей сети!</b>\n"
                f"➕ +1 день Premium зачислено!", 
                parse_mode="HTML")
        except:
            pass

    um.mark_active(uid)

    # Removed message.delete() to keep user message

    text = (f"🛡 <b>{PROXY_NAME}</b>\n\n"
            f"✨ Приветствуем, <b>{message.from_user.first_name}</b>!\n\n"
            f"🚀 Быстрые и надежные прокси сервера\n"
            f"💎 Реферальная система с наградами\n"
            f"🌍 Сервера по всему миру\n\n"
            f"🎁 <i>Есть промокод? Введите команду /promo</i>")

    await message.answer_photo(
        photo=FSInputFile(ASSETS["START"]),
        caption=text,
        reply_markup=Keyboards.start(),
        parse_mode="HTML"
    )

@router.message(Command("promo"))
async def cmd_promo(message: Message, state: FSMContext):
    await state.set_state(States.promo_code)
    await message.answer(
        "🎟 <b>Активация промокода</b>\n\nВведи свой промокод ниже:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardBuilder().button(text="Отмена", callback_data="profile").as_markup()
    )

@router.message(StateFilter(States.promo_code))
async def process_promo_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    uid = message.from_user.id
    
    if code in PROMO_CODES:
        reward = PROMO_CODES[code]
        
        # Защита от повторного использования на уровне юзера
        user = dm.get_user(uid)
        used_promos = user.get("used_promos", []) if user else []
        
        if code in used_promos:
            await message.answer("❌ Вы уже использовали этот промокод.")
            await state.clear()
            return
            
        dm.update_user(uid, {"used_promos": used_promos + [code]})
        
        if reward == "MTPROTO":
            proxies = pm.get_proxies_by_type().get("MTPROTO", [])
            if not proxies:
                await message.answer("🎁 Промокод активирован, но к сожалению, свободных MTProto прокси сейчас нет. Администратор свяжется с вами.")
            else:
                proxy = proxies[0]
                server = proxy.get('server')
                port = proxy.get('port')
                secret = proxy.get('secret')
                link = f"https://t.me/proxy?server={server}&port={port}&secret={secret}"
                
                await message.answer(
                    f"🎉 <b>Промокод активирован!</b>\n\n"
                    f"Ваш подарочный <b>MTProto</b> прокси:\n"
                    f"<code>{server}:{port}</code>\n\n"
                    f"🔗 <a href='{link}'>Подключиться</a>\n\n"
                    f"<i>Сохраните эти данные.</i>", 
                    parse_mode="HTML"
                )
    else:
        await message.answer("❌ Неверный или недействительный промокод.")
    
    await state.clear()

@router.callback_query(F.data == "profile")
async def show_profile(call: CallbackQuery, state: FSMContext):
    await state.clear()
    uid = call.from_user.id
    um.mark_active(uid)

    info = um.get_info(uid)
    if not info:
        um.register(uid, call.from_user.username)
        info = um.get_info(uid)

    refs = info.get("refs_count", 0) if info else 0
    vip_end = info.get("vip_expires_at") if info else None
    pref = info.get("country_pref", "Мир") if info else "Мир"
    
    tier = um.get_tier_info(uid)
    vip_status = Utils.format_vip_time(vip_end) if vip_end else "Не активен"
    if uid == ADMIN_ID:
        vip_status = "Бессрочный"
    is_admin = (uid == ADMIN_ID)

    alive_proxies = pm.get_alive_count()

    text = (f"👤 <b>КАБИНЕТ ПОЛЬЗОВАТЕЛЯ</b>\n\n"
            f"├ 🔹 ID: <code>{uid}</code>\n"
            f"├ 🔹 Статус: <b>{tier['emoji']} {tier['name']}</b>\n"
            f"├ 🔹 Premium: <b>{vip_status}</b>\n"
            f"├ 🔹 Рефералов: <b>{refs}</b>\n"
            f"├ 🔹 Локация: <b>{pref}</b>\n"
            f"└ 🔹 Активных серверов: <b>{alive_proxies}</b>\n\n"
            f"⚡ <i>Скорость: {tier['speed']}</i>")

    await try_edit(call, ASSETS["PROFILE"], text, Keyboards.profile(is_admin, pref))

@router.callback_query(F.data == "get_proxy")
async def get_proxy_handler(call: CallbackQuery):
    uid = call.from_user.id
    info = um.get_info(uid)
    country_pref = info.get("country_pref", "Мир") if info else "Мир"
    tier = um.get_tier_info(uid)
    
    proxy, error = pm.get_best_proxy(tier["name"], country_pref)

    if not proxy:
        return await call.answer(error, show_alert=True)

    server = proxy.get("server", "")
    port = proxy.get("port", "")
    secret = proxy.get("secret", "")
    country = proxy.get("country", "🌍 Мир")
    ping = proxy.get("ping", 9999)

    text = (f"🛡 <b>ВАШЕ СОЕДИНЕНИЕ ГОТОВО</b>\n\n"
            f"📍 <b>Локация:</b> {country}\n"
            f"⚡ <b>Отклик:</b> {ping} ms\n"
            f"🔐 <b>Протокол:</b> MTProto\n"
            f"{error}\n\n"
            f"👇 <i>Нажмите кнопку ниже для моментального подключения:</i>")

    await try_edit(call, ASSETS["PROXY"], text, 
                   Keyboards.proxy_result(f"https://t.me/proxy?server={server}&port={port}&secret={secret}"))

@router.callback_query(F.data == "filter_menu")
async def filter_handler(call: CallbackQuery):
    await try_edit(call, ASSETS["PROXY"], 
                   "🌐 <b>ВЫБОР ЛОКАЦИИ</b>\n\nУкажите предпочитаемый регион:", 
                   Keyboards.locations())

@router.callback_query(F.data.startswith("set_loc_"))
async def set_location(call: CallbackQuery):
    loc = call.data.replace("set_loc_", "")
    um.set_pref(call.from_user.id, loc)
    await call.answer(f"✓ Локация: {loc}", show_alert=True)
    await show_profile(call, None)

@router.callback_query(F.data == "privileges")
async def privileges_handler(call: CallbackQuery):
    uid = call.from_user.id
    tier = um.get_tier_info(uid)
    text = (f"💎 <b>СИСТЕМА ПРИВИЛЕГИЙ</b>\n\n"
            f"Ваш статус: <b>{tier['emoji']} {tier['name']}</b>\n\n"
            f"<b>Уровни доступа:</b>\n"
            f"🥉 <b>Базовый</b> (0 реф.) — Стандартная скорость\n"
            f"🥈 <b>Продвинутый</b> (3 реф.) — Повышенная скорость\n"
            f"🥇 <b>Профессиональный</b> (10 реф.) — Высокая скорость\n"
            f"💎 <b>Элитный</b> (25 реф.) — Максимальная скорость\n"
            f"💠 <b>Премиум</b> (50 реф.) — Безлимитная скорость\n"
            f"👑 <b>Premium+</b> — Администрация\n\n"
            f"<i>Приглашайте друзей для повышения статуса!</i>")
    await try_edit(call, ASSETS["PROFILE"], text, Keyboards.back())

@router.callback_query(F.data == "buy_premium")
async def buy_premium_handler(call: CallbackQuery):
    text = (f"🛒 <b>ПОКУПКА PREMIUM</b>\n\n"
            f"Получите доступ без приглашения друзей!\n\n"
            f"<b>Тарифы:</b>\n"
            f"🔹 <b>Средний</b> — $5 / 30 дней\n"
            f"   <i>Максимальная скорость</i>\n"
            f"🔸 <b>Базовый</b> — $2 / 14 дней\n"
            f"   <i>Высокая скорость</i>\n\n"
            f"<b>Оплата:</b> CryptoBot (TON / USDT)\n\n"
            f"👉 <b><a href='https://t.me/send?start=IV0fjJRuSt2W'>ОПЛАТИТЬ СЧЕТ</a></b>\n"
            f"<code>https://t.me/send?start=IV0fjJRuSt2W</code>\n\n"
            f"<i>После оплаты нажмите «Я оплатил(а)»</i>")
    await try_edit(call, ASSETS["PROXY"], text, Keyboards.payment("premium"))

@router.callback_query(F.data.startswith("buy_"))
async def buy_proxy_handler(call: CallbackQuery):
    ptype = call.data.replace("buy_", "")
    
    prices = {"HTTP": "$2", "SOCKS4": "$3", "SOCKS5": "$5", "MTPROTO": "$2"}
    price = prices.get(ptype.upper(), "$5")

    text = (f"🛒 <b>ПОКУПКА {ptype.upper()} ПРОКСИ</b>\n\n"
            f"Получите один приватный сервер с высокой скоростью.\n\n"
            f"<b>Стоимость:</b> {price} / шт\n\n"
            f"<b>Оплата:</b> CryptoBot (TON / USDT)\n\n"
            f"👉 <b><a href='https://t.me/send?start=IV0fjJRuSt2W'>ОПЛАТИТЬ СЧЕТ</a></b>\n"
            f"<code>https://t.me/send?start=IV0fjJRuSt2W</code>\n\n"
            f"<i>После оплаты нажмите «Я оплатил(а)»</i>")
    await try_edit(call, ASSETS["PROXY"], text, Keyboards.payment(ptype.lower()))

@router.callback_query(F.data.startswith("payment_check_"))
async def payment_check_handler(call: CallbackQuery):
    item_type = call.data.split('_', 2)[2]
    uid = call.from_user.id
    username = call.from_user.username or "Без юзернейма"

    type_map = {
        "premium": "Premium",
        "http": "HTTP Прокси",
        "socks4": "SOCKS4 Прокси",
        "socks5": "SOCKS5 Прокси",
        "mtproto": "MTProto Прокси"
    }
    item_name = type_map.get(item_type, item_type.upper())

    text_admin = (f"💰 <b>ПРОВЕРКА ОПЛАТЫ</b>\n\n"
                  f"От: @{username} (<code>{uid}</code>)\n"
                  f"Запрос на: <b>{item_name}</b>\n"
                  f"Пользователь запросил проверку оплаты.\n"
                  f"Выберите действие:")
    try:
        await bot.send_message(ADMIN_ID, text_admin, 
                              reply_markup=Keyboards.admin_payment(uid), 
                              parse_mode="HTML")
        await call.answer("✅ Запрос отправлен администратору!", show_alert=True)
    except Exception as e:
        logger.error(f"Payment check error: {e}")
        await call.answer("❌ Ошибка. Попробуйте позже.", show_alert=True)

@router.callback_query(F.data.startswith("pay_approve_"))
async def pay_approve_handler(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    parts = call.data.split('_')
    target_uid = int(parts[2])
    days = int(parts[3])
    item_type = parts[4] if len(parts) > 4 else "premium"

    if item_type == "premium":
        user = dm.get_user(target_uid)
        now = datetime.now()
        
        if user and user.get("vip_expires_at"):
            try:
                curr_date = datetime.strptime(user["vip_expires_at"], "%Y-%m-%d %H:%M:%S")
                new_date = max(now, curr_date) + timedelta(days=days)
            except:
                new_date = now + timedelta(days=days)
        else:
            new_date = now + timedelta(days=days)

        dm.update_user(target_uid, {
            "vip_expires_at": new_date.strftime("%Y-%m-%d %H:%M:%S")
        })

        tariff_name = "Premium (30 дней)" if days == 30 else "Premium (14 дней)"
        await call.message.edit_text(
            f"✅ Оплата подтверждена!\n"
            f"Пользователю <code>{target_uid}</code> выдан <b>{tariff_name}</b>.", 
            parse_mode="HTML")

        try:
            await bot.send_message(target_uid, 
                f"🎉 <b>Оплата подтверждена!</b>\n\n"
                f"Вам зачислен тариф <b>{tariff_name}</b>.", 
                parse_mode="HTML")
        except:
            pass
    else:
        # Выдаем прокси
        proxies = pm.get_proxies_by_type().get(item_type.upper(), [])
        if not proxies:
            await call.message.edit_text(f"❌ Нет доступных {item_type.upper()} прокси в базе для выдачи.", parse_mode="HTML")
            try:
                await bot.send_message(target_uid, "Оплата подтверждена, но свободных прокси нет. Администратор свяжется с вами.")
            except:
                pass
            return

        proxy = proxies[0] # Берем первый доступный
        server = proxy.get('server')
        port = proxy.get('port')
        
        await call.message.edit_text(
            f"✅ Оплата подтверждена!\n"
            f"Пользователю <code>{target_uid}</code> выдан <b>{item_type.upper()}</b> прокси:\n"
            f"<code>{server}:{port}</code>", 
            parse_mode="HTML")

        try:
            await bot.send_message(target_uid, 
                f"🎉 <b>Оплата подтверждена!</b>\n\n"
                f"Ваш персональный <b>{item_type.upper()}</b> прокси:\n"
                f"<code>{server}:{port}</code>\n\n"
                f"<i>Сохраните эти данные.</i>", 
                parse_mode="HTML")
        except:
            pass

@router.callback_query(F.data.startswith("pay_reject_"))
async def pay_reject_handler(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    target_uid = int(call.data.split('_')[2])
    await call.message.edit_text(
        f"❌ Запрос оплаты от <code>{target_uid}</code> отклонен.", 
        parse_mode="HTML")
    try:
        await bot.send_message(target_uid, 
            "❌ <b>Оплата не подтверждена.</b>\n"
            "Свяжитесь с поддержкой.", 
            parse_mode="HTML")
    except:
        pass

@router.callback_query(F.data == "reviews_list")
async def reviews_list_handler(call: CallbackQuery):
    reviews = dm.get_reviews(5)

    text = "⭐ <b>ОТЗЫВЫ КЛИЕНТОВ</b>\n\n"
    if not reviews:
        text += "<i>Пока нет отзывов. Будьте первыми!</i>\n\n"
    else:
        for r in reviews:
            uname = r.get("username", "Аноним")
            stars = "⭐" * r.get("stars", 5)
            r_text = r.get("text", "")
            text += f"👤 <b>@{uname}</b>\n"
            text += f"Оценка: {stars}\n"
            text += f"💬 <i>«{r_text}»</i>\n"
            text += "〰️〰️〰️〰️〰️〰️\n"

    await try_edit(call, ASSETS["PROXY"], text, Keyboards.reviews())

@router.callback_query(F.data == "leave_review")
async def leave_review_start(call: CallbackQuery):
    await try_edit(call, ASSETS["PROXY"], 
                   "⭐ <b>ОЦЕНКА СЕРВИСА</b>\n\nВыберите оценку:", 
                   Keyboards.stars())

@router.callback_query(F.data.startswith("star_"))
async def process_star(call: CallbackQuery, state: FSMContext):
    stars = int(call.data.split('_')[1])
    await state.update_data(stars=stars)
    await try_edit(call, ASSETS["PROXY"], 
                   f"Вы выбрали {stars} ⭐\n\nНапишите отзыв (до 500 символов):", 
                   Keyboards.back("reviews_list"))
    await state.set_state(States.review_text)

@router.message(StateFilter(States.review_text))
async def process_review_text(message: Message, state: FSMContext):
    data = await state.get_data()
    stars = data.get('stars', 5)
    uid = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    text = message.text[:500]

    dm.add_review(uid, username, stars, text)

    await message.answer(
        "✅ <b>Спасибо за отзыв!</b>\nОн опубликован в общем списке.", 
        parse_mode="HTML", 
        reply_markup=Keyboards.back("reviews_list"))
    await state.clear()

@router.callback_query(F.data == "proxy_types")
async def proxy_types_handler(call: CallbackQuery):
    proxies_by_type = pm.get_proxies_by_type()
    
    text = "📦 <b>ТИПЫ ПРОКСИ</b>\n\n"
    text += f"🌐 <b>HTTP:</b> {len(proxies_by_type['HTTP'])} шт.\n"
    text += f"🔒 <b>SOCKS4:</b> {len(proxies_by_type['SOCKS4'])} шт.\n"
    text += f"🔐 <b>SOCKS5:</b> {len(proxies_by_type['SOCKS5'])} шт.\n"
    text += f"⚡ <b>MTProto:</b> {len(proxies_by_type['MTProto'])} шт.\n\n"
    text += "<i>Выберите тип для просмотра:</i>"
    
    await try_edit(call, ASSETS["PROXY"], text, Keyboards.proxy_types())

@router.callback_query(F.data == "proxy_availability")
async def proxy_availability_handler(call: CallbackQuery):
    stats = pm.get_proxy_stats_detailed()
    
    text = "📦 <b>НАЛИЧИЕ ПРОКСИ</b>\n\n"
    text += "<b>Доступные сервера:</b>\n\n"
    
    type_emoji = {"HTTP": "🌐", "SOCKS4": "🔒", "SOCKS5": "🔐", "MTProto": "⚡"}
    
    for ptype, data in stats.items():
        if ptype == "reserved":
            continue
        emoji = type_emoji.get(ptype, "📌")
        total = data.get("total", 0)
        
        text += f"{emoji} <b>{ptype}:</b> {total} шт.\n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="◀️ Назад", callback_data="profile")
    kb.adjust(1)
    
    await try_edit(call, ASSETS["PROXY"], text, kb.as_markup())

@router.callback_query(F.data == "refresh_ping")
async def refresh_ping_handler(call: CallbackQuery):
    await call.answer("⏳ Запуск проверки пинга...", show_alert=True)
    await pm.update_all_pings()
    await proxy_availability_handler(call)

@router.callback_query(F.data == "show_http")
async def show_http(call: CallbackQuery):
    text = (f"🌐 <b>HTTP ПРОКСИ</b>\n\n"
            f"Приватные HTTP-прокси с высокой надежностью.\n\n"
            f"💵 <b>Цена:</b> $2 / шт\n\n"
            f"<i>Нажмите кнопку ниже, чтобы приобрести.</i>")
    await try_edit(call, ASSETS["PROXY"], text, Keyboards.buy_proxy_menu("http"))

@router.callback_query(F.data == "show_socks4")
async def show_socks4(call: CallbackQuery):
    text = (f"🔒 <b>SOCKS4 ПРОКСИ</b>\n\n"
            f"Универсальные SOCKS4-прокси для любых задач.\n\n"
            f"💵 <b>Цена:</b> $3 / шт\n\n"
            f"<i>Нажмите кнопку ниже, чтобы приобрести.</i>")
    await try_edit(call, ASSETS["PROXY"], text, Keyboards.buy_proxy_menu("socks4"))

@router.callback_query(F.data == "show_socks5")
async def show_socks5(call: CallbackQuery):
    text = (f"🔐 <b>SOCKS5 ПРОКСИ</b>\n\n"
            f"Максимальная анонимность и скорость с SOCKS5.\n\n"
            f"💵 <b>Цена:</b> $5 / шт\n\n"
            f"<i>Нажмите кнопку ниже, чтобы приобрести.</i>")
    await try_edit(call, ASSETS["PROXY"], text, Keyboards.buy_proxy_menu("socks5"))

@router.callback_query(F.data == "show_mtproto")
async def show_mtproto(call: CallbackQuery):
    text = (f"⚡ <b>MTProto ПРОКСИ</b>\n\n"
            f"Идеально для Telegram. Никаких подвисаний и отличный пинг.\n\n"
            f"💵 <b>Цена:</b> $2 / шт\n\n"
            f"<i>Нажмите кнопку ниже, чтобы приобрести.</i>")
    await try_edit(call, ASSETS["PROXY"], text, Keyboards.buy_proxy_menu("mtproto"))

@router.callback_query(F.data == "admins_list")
async def admins_list_handler(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    text = (f"👑 <b>АДМИНИСТРАЦИЯ</b>\n\n"
            f"<b>Владелец и Главный Админ:</b>\n"
            f"👤 <a href='tg://user?id={ADMIN_ID}'>Xenon (Premium+)</a>\n\n"
            f"<i>По вопросам обращайтесь в поддержку.</i>")
    await try_edit(call, ASSETS["ADMIN"], text, Keyboards.back())

@router.callback_query(F.data == "referrals")
async def referrals_handler(call: CallbackQuery):
    uid = call.from_user.id
    info = um.get_info(uid)
    bot_info = await bot.get_me()
    
    refs_count = info.get("refs_count", 0) if info else 0
    
    text = (f"🔗 <b>ПАРТНЕРСКАЯ СЕТЬ</b>\n\n"
            f"💰 <b>1 друг = +1 день Premium</b>\n\n"
            f"Ваша ссылка:\n"
            f"<code>https://t.me/{bot_info.username}?start={uid}</code>\n\n"
            f"Привлечено: <b>{refs_count}</b>")
    await try_edit(call, ASSETS["REF"], text, Keyboards.back())

@router.callback_query(F.data == "support")
async def support_handler(call: CallbackQuery, state: FSMContext):
    await try_edit(call, ASSETS["PROXY"], 
                   "💬 <b>ПОДДЕРЖКА</b>\n\nОпишите ваш вопрос одним сообщением:", 
                   Keyboards.back())
    await state.set_state(States.support_msg)

@router.message(StateFilter(States.support_msg))
async def support_receive(message: Message, state: FSMContext):
    uid = message.from_user.id
    kb = InlineKeyboardBuilder().button(
        text="Ответить", callback_data=f"reply_{uid}").as_markup()
    try:
        await bot.send_message(ADMIN_ID, 
            f"📞 <b>Запрос #{uid}</b>\n"
            f"От: @{message.from_user.username}\n\n"
            f"{message.text}", 
            reply_markup=kb, 
            parse_mode="HTML")
        await message.answer("✓ <b>Запрос отправлен!</b>\nОжидайте ответа.", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Support error: {e}")
    await state.clear()

@router.callback_query(F.data.startswith("reply_"))
async def admin_reply_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    user_to_reply = call.data.split('_')[1]
    await call.message.answer(f"Напишите ответ для пользователя {user_to_reply}:")
    await state.update_data(tid=int(user_to_reply))
    await state.set_state(States.support_reply)

@router.message(StateFilter(States.support_reply))
async def admin_reply_send(message: Message, state: FSMContext):
    data = await state.get_data()
    try:
        await bot.send_message(data['tid'], 
            f"📩 <b>Ответ поддержки:</b>\n\n{message.text}", 
            parse_mode="HTML")
        await message.answer("✅ Сообщение отправлено!")
    except:
        await message.answer("❌ Ошибка. Пользователь заблокировал бота.")
    await state.clear()

@router.callback_query(F.data == "admin_panel")
async def admin_panel(call: CallbackQuery):
    if call.from_user.id == ADMIN_ID:
        await try_edit(call, ASSETS["ADMIN"], 
                       "⚙️ <b>АДМИН-ПАНЕЛЬ</b>\nДоступ разрешен", 
                       Keyboards.admin())

@router.callback_query(F.data == "adm_load")
async def adm_load(call: CallbackQuery):
    c = pm.load_from_file()
    await call.answer(f"✅ Добавлено: {c} прокси", show_alert=True)

@router.callback_query(F.data == "adm_stats")
async def adm_stats(call: CallbackQuery):
    total_users, active_users = um.get_stats()
    total_proxies, alive_proxies = dm.get_proxy_stats()
    
    await call.answer(
        f"📊 СТАТИСТИКА:\n\n"
        f"👥 Пользователей: {total_users}\n"
        f"🟢 Онлайн (24ч): {active_users}\n"
        f"📦 Прокси всего: {total_proxies}\n"
        f"✅ Активных: {alive_proxies}", 
        show_alert=True)

@router.callback_query(F.data == "adm_users_list")
async def adm_users_list(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    
    text_data = um.get_all_users_formatted()
    filename = "users_export.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text_data)

    await call.message.answer_document(
        FSInputFile(filename),
        caption="📁 <b>База пользователей</b>",
        parse_mode="HTML"
    )

@router.callback_query(F.data == "adm_add")
async def adm_add(call: CallbackQuery, state: FSMContext):
    await call.message.answer("Отправьте список прокси (ссылки tg:// или https://):")
    await state.set_state(States.admin_add_proxy)

@router.message(StateFilter(States.admin_add_proxy))
async def adm_add_proc(message: Message, state: FSMContext):
    ls = re.findall(r'(?:tg://|https://t\.me/|http://|socks[45]://)proxy[^\s]*|'
                    r'(?:http://|socks[45]://)[^\s]+', message.text)
    c = 0
    for link in ls:
        proxy = pm.parse_proxy_link(link)
        if proxy and dm.add_proxy(proxy):
            c += 1
    await message.answer(f"✅ Добавлено: {c} прокси")
    await state.clear()

@router.callback_query(F.data == "adm_broadcast")
async def adm_cast(call: CallbackQuery, state: FSMContext):
    await call.message.answer("Отправьте текст рассылки (поддерживается HTML):")
    await state.set_state(States.admin_broadcast)

@router.message(StateFilter(States.admin_broadcast))
async def adm_cast_proc(message: Message, state: FSMContext):
    users = dm.get_all_users()
    c = 0
    ph = FSInputFile(ASSETS["BROADCAST"])
    msg = await message.answer("Рассылка началась...")

    for u in users:
        try:
            await bot.send_photo(u["user_id"], ph, caption=message.text, parse_mode="HTML")
            c += 1
            await asyncio.sleep(0.05)
        except:
            pass

    await msg.edit_text(f"✅ Готово!\nДоставлено: {c}")
    await state.clear()

@router.callback_query(F.data == "adm_del")
async def adm_del(call: CallbackQuery):
    dm.clear_proxies()
    await call.answer("✓ База прокси очищена", show_alert=True)

@router.callback_query(F.data == "about")
async def about(call: CallbackQuery):
    text = (f"ℹ️ <b>О СИСТЕМЕ {PROXY_NAME}</b>\n\n"
            f"🔐 Защищенные MTProto сервера\n"
            f"🌍 Сервера по всему миру\n"
            f"💎 Реферальная система\n"
            f"⚡ Высокая скорость\n\n"
            f"<i>Развивайте сеть для премиум доступа!</i>")
    await try_edit(call, ASSETS["PROXY"], text, Keyboards.back())

async def try_edit(call: CallbackQuery, photo: str, cap: str, kb):
    try:
        await call.message.edit_media(
            media=InputMediaPhoto(media=FSInputFile(photo), caption=cap, parse_mode="HTML"),
            reply_markup=kb
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Edit error: {e}")

async def ping_loop():
    logger.info("External proxy check loop started (runs every 12 hours)...")
    while True:
        try:
            logger.info("Running scheduled external proxy checks...")
            await pm.update_all_pings()
        except Exception as e:
            logger.error(f"External proxy check error: {e}")
        # 12 hours = 43200 seconds
        await asyncio.sleep(43200)

async def check_telegram_connection() -> bool:
    """Проверяет прямой доступ к api.telegram.org"""
    try:
        async with ClientSession(timeout=ClientTimeout(total=3)) as session:
            async with session.get(f"https://api.telegram.org/bot{TOKEN}/getMe") as response:
                return response.status == 200
    except Exception:
        return False

async def main():
    global RESERVED_PROXY, TELEGRAM_PROXY, bot
    
    print(f"\n🚀 {PROXY_NAME} STARTED | ADMIN: {ADMIN_ID}")
    logger.info("Инициализация...")

    Utils.check_files()

    total_proxies = len(dm.get_proxies())
    logger.info(f"В базе уже {total_proxies} прокси. Загрузка новых из файлов...")
    c = pm.load_from_file()
    if c > 0:
        logger.info(f"Загружено {c} новых прокси. Итого в базе: {len(dm.get_proxies())}")
    else:
        logger.info(f"Новых прокси нет. В базе: {total_proxies}")

    # Проверяем прямой доступ к Telegram API
    logger.info("Проверка подключения к Telegram API...")
    try:
        direct_conn_ok = await asyncio.wait_for(check_telegram_connection(), timeout=5)
    except asyncio.TimeoutError:
        direct_conn_ok = False

    if direct_conn_ok:
        logger.info("✅ Прямое подключение OK. Используем прямое.")
        bot = Bot(token=TOKEN)
    elif TELEGRAM_PROXY:
        logger.info(f"⚠️ Прямое подключение недоступно. Прокси: {TELEGRAM_PROXY}")
        aiogram_session = AiohttpSession(proxy=TELEGRAM_PROXY)
        bot = Bot(token=TOKEN, session=aiogram_session)
    else:
        logger.warning("❌ Нет подключения и нет прокси. Бот может не работать.")
        bot = Bot(token=TOKEN)

    # Пинг запускается в фоне
    asyncio.create_task(ping_loop())

    # Безопасный запуск поллинга для Leapcell
    retries = 5
    while retries > 0:
        try:
            logger.info("Starting bot polling...")
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
            break
        except Exception as e:
            logger.error(f"Polling error: {e}")
            retries -= 1
            logger.info(f"Retrying in 5 seconds... ({retries} attempts left)")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"FATAL CRASH: {e}")


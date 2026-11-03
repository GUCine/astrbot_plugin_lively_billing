"""機器人的飯錢：DeepSeek 餘額和每天花了多少，每把密鑰分開記。

- 每把密鑰各算各的：
  - 聊天回覆：provider_stats 裡每次對話的 token（AstrBot 自己記的，按模型 ID 分開）
  - 後台活（記憶總結、人物卡、覆盤、日報、說說……）：本插件給每個模型的 text_chat 套一層，記下每次呼叫的 token
    和是誰呼叫的（usage.jsonl），聊天回覆那部分已經在 provider_stats 裡，不重複記
  - 都按配置裡的單價、DeepSeek 峰谷時段折算成錢
- 餘額：每小時用每把密鑰查一次 /user/balance。DeepSeek 沒有按密鑰的賬單介面，幾把密鑰屬於同一個帳戶時餘額是共用的，
  這時帳戶的「真實花費」只有一個總數，減去各密鑰算出來的部分就是「沒記到的」（比如別的程式用了這個帳戶）
- 餘額不降反升說明充了錢，那一段按算出來的花費補上
- 畫圖的中轉站沒有賬單介面，按「當天畫了幾張 × 單價」估（單價在插件配置 image_price）
- 語音（11-03 起 BreezeBlue 雲端）另算，不扣 DeepSeek：每小時用語音插件設定裡的密鑰查一次 BreezeBlue 的點數餘額（/v1/balance）
  和最近 VOICE_DAYS 天每次請求實際扣的點數（/v1/usage），存在 voice.json；按天記條數、點數，再按 voice_price_10k（元/1 萬點）
  估成錢。點數是 BreezeBlue 算的；錢是估的，實際以 BreezeBlue 的 Subscription 頁、生成前的確認為準
- 每個帳戶餘額低於 alert_at 私信主人提醒充錢，低於一半再提醒一次；充回去之後重新計
- 每小時把彙總寫進 summary.json，面板和日報讀它
- 按人記：每次聊天回覆是誰觸發的（群裡說話的人、私聊的人）、用了多少 token，記在 users.jsonl；
  面板「饭钱」頁按人匯總（花費、次數、什麼時候來、在哪聊）。開始記之前的私聊從 provider_stats 補，群聊的舊記錄對不上人
- 提示詞留檔（預設關，除錯用）：打開後把〈主人〉私聊每次真的送給模型的整份內容（system、歷史、這一輪、工具定義）
  和模型的回覆、思考存在 prompts/<日期>.jsonl，同一輪調了工具的每一步各一行，AstrBot 壓縮這個私聊的歷史那次也存。
  **會把對話全文存在伺服器上**；system 和工具定義按內容去重存在 prompts/blobs/，圖片只記大小。
  打開幾小時後自動關（prompt_dump_hours），只留最近幾天（prompt_dump_days）
- 按天的帳（ledger）：每天餘額實際掉了多少（兩次查餘額之間掉的按時間攤到每個鐘點）、記到的花在哪幾類、每個鐘點、
  誰、最貴的幾次、快取省了多少、高峰多花多少、降智版要是走 DeepSeek 得花多少；面板「饭钱」頁按天看、點進某一天
- 別的插件在自己開的任務裡調模型時看不出是誰（呼叫棧斷在任務邊界，只看得到 AstrBot），按設定 pid_who
  「模型設定ID=插件名」認（例如群分析插件取話題用的模型設定）
- 錢花在哪（spend）：按做的事分成人看得懂的幾類（群裡聊天、私聊、看圖、整理太長的聊天、回味、記住聊過的事、看手機、
  寫東西、調試），餘額實際掉得比記到的多的那部分單列「沒記到的」（在 AstrBot 外面直接用這個帳戶的，比如伺服器上跑的測試）
- 投餵記錄（feeds.json）：群友給〈我〉的飯錢，只是帳本，不經手錢。〈我〉聊天時聽到了自己記（工具 lively_feed_note，
  記下是誰說的；記錯了用 lively_feed_drop 劃掉自己記的），〈主人〉也能在面板上記、刪
主人私信指令：飯錢 / 餘額；飯錢 留檔 開|關|狀態
"""
import asyncio
import contextvars
import hashlib
import inspect
import itertools
import json
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.message.message_event_result import MessageChain

from .lively_common import CONFIG_DIR, DATA, GATE_PLUGIN, Settings, plugin_data_dir, tell_owner

TZ = timezone(timedelta(hours=8))
CMD_CONFIG = DATA / "cmd_config.json"
STATS_DB = DATA / "data_v4.db"
BALANCE_URL = "https://api.deepseek.com/user/balance"
# 她電腦上做長任務（DSH）單獨的一把密鑰（10-30 主人用 server/mac/set_study_key.py 放的）：不在模型設定裡，也查餘額、算進帳戶
STUDY_KEY = plugin_data_dir("astrbot_plugin_lively_pc") / "study_key"
STUDY_ID = "study"
EVERY = 3600
VOICE_CONF = CONFIG_DIR / "astrbot_plugin_lively_voice_config.json"
VOICE_DAYS = 31
VOICE_PAGES = 20  # 用量一頁 100 條，最多翻這麼多頁
BILL_FRESH = 600  # 她查飯錢時，上次查餘額超過這麼久就先再查一次
WRAP_EVERY = 300  # 模型重載後是新實例，隔一會兒檢查一遍有沒有沒套上的
KEEP_USAGE_DAYS = 62
# 這一輪聊天回覆是誰觸發的：在 on_llm_request 裡設，模型呼叫時讀（同一個任務裡，Agent 的工具輪也算這個人）
_WHO: contextvars.ContextVar[dict | None] = contextvars.ContextVar("lively_billing_who", default=None)
_RID = itertools.count(1)
WHO_TTL = 900  # 一輪最多算 15 分鐘，過了就不算這個人的
CALLER_NAMES = {
    "chat": "聊天回复", "astrbot": "AstrBot 自带", "livingmemory": "记忆插件",
    "lively_bond": "情感", "lively_evolve": "自我成长", "lively_memtier": "记忆分层",
    "lively_report": "日报", "lively_qzone": "说说", "lively_proactive": "主动聊天",
    "qq_group_daily_analysis": "群分析/群漫画", "lively_mind": "自主", "lively_gate": "门禁", "lively_comic": "群漫画",
    "lively_social": "社交",
}
# 錢花在哪：按做的事分類（面板「饭钱」頁用），順序就是顯示的順序
SPEND_CATS = [
    ("group", "群里聊天", "群里有人叫〈我〉、〈我〉接话"),
    ("private", "私聊", "私聊里回话"),
    ("look", "看群里的图", "群里每张图先看一眼，写一句描述当背景"),
    ("squeeze", "整理太长的聊天", "一个聊天的记录太长时，压成一段总结接着聊"),
    ("digest", "回味", "每晚 4～9 点把记忆拆成和每个人的回忆，整理心里的 TA"),
    ("remember", "记住聊过的事", "聊完写成长期记忆，每晚再按天、周、月压短"),
    ("phone", "看 QQ", "自主：隔一阵看一眼 QQ 想做什么，发完再看一眼"),
    ("write", "写东西", "日报、说说、成长复盘、群漫画（取话题、分镜、看图）、主动找人"),
    ("test", "调试", "在面板或服务器里试功能"),
    ("other", "其他", "认不出是哪个插件"),
]
SPEND_WHO = {"lively_bond": "digest", "livingmemory": "remember", "lively_memtier": "remember", "lively_mind": "phone",
             "lively_report": "write", "lively_qzone": "write", "lively_evolve": "write", "lively_comic": "write",
             "lively_proactive": "write", "lively_social": "write", "qq_group_daily_analysis": "write"}
LOOK_MAX_INPUT = 3000  # 舊記錄沒標做什麼：AstrBot 自己的呼叫輸入這麼短的是看圖，長的是壓縮歷史
FEED_MAX = 100000
# 提示詞留檔：AstrBot 壓縮歷史時最後一條 user 的開頭（compressor.py），用來認出壓縮那次
COMPRESS_MARK = "Generate a summary of our previous conversation history"
DUMP_WORDS = {"开": "on", "開": "on", "关": "off", "關": "off", "状态": "status", "狀態": "status", "": "status"}
BLOB_GRACE = 3600  # 剛寫的 blob 可能還沒被那一行引用到，清理時不動一小時內的


def _peak(ts: float) -> bool:
    d = datetime.fromtimestamp(ts, TZ)
    hm = d.strftime("%H:%M")
    return d.weekday() < 5 and ("09:00" <= hm < "12:00" or "14:00" <= hm < "18:00")


def _day(ts: float) -> str:
    return datetime.fromtimestamp(ts, TZ).strftime("%Y-%m-%d")


def _ts(line: str) -> float:
    try:
        return float(json.loads(line).get("ts", 0))
    except Exception:
        return 0.0


def _no_b64(s: str) -> str:
    """圖片的 base64 換成大小，網址和本機路徑照留"""
    head = s[:80]
    if (head.startswith("data:") and ";base64," in head) or head.startswith("base64://"):
        return f"[图片 {len(s) * 3 // 4 // 1024} KB]"
    return s


def _plain(x, depth: int = 0):
    """模型參數轉成能存 JSON 的（pydantic 用 model_dump），圖片的 base64 換掉"""
    if depth > 30:
        return str(x)[:200]
    if hasattr(x, "model_dump"):
        try:
            x = x.model_dump(exclude_none=True)
        except Exception:
            return str(x)[:2000]
    if isinstance(x, dict):
        return {str(k): _plain(v, depth + 1) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_plain(v, depth + 1) for v in x]
    if isinstance(x, str):
        return _no_b64(x)
    if x is None or isinstance(x, (int, float, bool)):
        return x
    return str(x)[:2000]


def _text_of(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(str(p.get("text", "")) for p in content if isinstance(p, dict))
    return ""


def _sha(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _refs(x, out: set):
    """一行留檔裡引用到的 blob"""
    if isinstance(x, dict):
        for k, v in x.items():
            if k in ("blob", "tools_blob") and isinstance(v, str):
                out.add(v)
            else:
                _refs(v, out)
    elif isinstance(x, list):
        for v in x:
            _refs(v, out)


def _caller() -> str:
    """看呼叫棧是誰在調模型：聊天回覆（AstrBot 的對話流程）、某個插件、還是 AstrBot 自己"""
    f = sys._getframe(2)  # 0 是這裡，1 是套的那層，2 起是呼叫方
    seen_core = False
    for _ in range(60):
        if f is None:
            break
        mod = f.f_globals.get("__name__", "") or ""
        if mod.startswith("astrbot.core.agent.runners"):
            return "chat"
        for part in mod.split("."):
            if part.startswith("astrbot_plugin_") and part != "astrbot_plugin_lively_billing":
                return part.removeprefix("astrbot_plugin_")
        seen_core = seen_core or mod.startswith("astrbot.")
        f = f.f_back
    return "astrbot" if seen_core else "其他"



def _pid_prices(lines) -> dict[str, tuple[float, float, float]]:
    """「模型設定ID=未命中,命中,輸出」（元/百萬 tokens），只寫一個數就三項都用它（0 = 不算錢）"""
    out = {}
    for line in lines or []:
        pid, _, val = str(line).partition("=")
        try:
            nums = [float(x) for x in val.replace("，", ",").split(",") if x.strip()]
        except ValueError:
            continue
        if pid.strip() and nums:
            out[pid.strip()] = tuple((nums * 3)[:3]) if len(nums) == 1 else tuple((nums + [nums[-1]] * 3)[:3])
    return out


def _pid_who(lines) -> dict[str, str]:
    """「模型設定ID=插件名」：AstrBot 看不出是誰調的呼叫，用這個模型設定的就記成這個插件（插件名寫不寫 astrbot_plugin_ 都行）"""
    out = {}
    for line in lines or []:
        pid, _, who = str(line).replace("：", "=").partition("=")
        if pid.strip() and who.strip():
            out[pid.strip()] = who.strip().removeprefix("astrbot_plugin_")
    return out


def _what(names: list, args: tuple, kwargs: dict) -> str:
    """AstrBot 自己的呼叫是在做什麼：看圖（帶圖片、沒有歷史）或壓縮歷史；認不出回空字串"""
    try:
        params = dict(zip(names, args))
        params.update(kwargs)
        ctx = params.get("contexts") or []
        last = ctx[-1] if isinstance(ctx, list) and ctx else None
        last = last.model_dump() if hasattr(last, "model_dump") else last
        if isinstance(last, dict) and last.get("role") == "user" and _text_of(last.get("content")).startswith(COMPRESS_MARK):
            return "compress"
        if params.get("image_urls") and not ctx:
            return "caption"
    except Exception:
        pass
    return ""


def _spend_cat(u: dict) -> str:
    """usage.jsonl 的一行算哪一類（SPEND_CATS）"""
    who, pid = str(u.get("who", "")), str(u.get("pid", ""))
    if who.endswith("_test"):
        return "test"
    if who in SPEND_WHO:
        return SPEND_WHO[who]
    if who == "lively_gate":  # 門禁包了群聊背景的看圖（加逾時）；其他是起草稿重寫，很少
        return "look" if "caption" in pid else "other"
    if who == "astrbot":
        what = u.get("what")
        if what:
            return "look" if what == "caption" else "squeeze" if what == "compress" else "other"
        return "look" if "caption" in pid or (u.get("miss", 0) + u.get("hit", 0)) <= LOOK_MAX_INPUT else "squeeze"
    return "other"


@register(
    "astrbot_plugin_lively_billing",
    "GUCine",
    "DeepSeek 餘額每小時記帳：每天真實花費、聊天/後臺/畫圖分項、餘額不足私信主人",
    "2.5.0",
)
class LivelyBilling(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.s = Settings(context, config)
        self.alert_at = float(self.s.get("alert_at", 10.0))
        self.image_price = float(self.s.get("image_price", 0.3))
        self.voice_price = float(self.s.get("voice_price_10k", 21.6))  # 元/1 萬點：BreezeBlue 充值 $3＝1 萬點
        # 元/百萬 tokens：未命中快取的輸入、命中快取的輸入、輸出；DeepSeek 尖峰時段（工作日 9-12、14-18 點）翻倍
        self.prices = (float(self.s.get("price_input_miss", 1.0)), float(self.s.get("price_input_hit", 0.02)),
                       float(self.s.get("price_output", 4.0)))
        self.peak_double = bool(self.s.get("peak_double", True))
        # 不扣 DeepSeek 餘額的模型設定（例如降智版走別家帳號）各自的單價；寫 0 就是不算錢
        self.pid_prices = _pid_prices(self.s.get("provider_prices", []))
        self.dir = Path(StarTools.get_data_dir("astrbot_plugin_lively_billing"))
        self.snap_file = self.dir / "balance.jsonl"
        self.usage_file = self.dir / "usage.jsonl"
        self.users_file = self.dir / "users.jsonl"
        self.track_users = bool(self.s.get("track_users", True))
        self.state_file = self.dir / "state.json"
        self.summary_file = self.dir / "summary.json"
        self.feeds_file = self.dir / "feeds.json"
        self.voice_file = self.dir / "voice.json"
        self._feeds_lock = threading.Lock()
        try:
            self.state = json.loads(self.state_file.read_text("utf-8"))
        except Exception:
            self.state = {}
        if not isinstance(self.state.get("alerted"), dict):
            self.state["alerted"] = {}  # 帳戶 -> 已經按哪個門檻提醒過
        self._wrapped: list[tuple] = []  # (模型, 原本的 text_chat, 原本的 text_chat_stream)，卸載時還原
        self.dump_dir = self.dir / "prompts"
        self._steps: dict[str, int] = {}  # rid -> 這一輪已經請求了幾次模型
        self._dump_lock = threading.Lock()
        self._dump_tasks: set = set()
        self._tasks = [asyncio.create_task(self._loop())]
        if bool(self.s.get("track_all_calls", True)):
            self._tasks.append(asyncio.create_task(self._wrap_loop()))

    async def terminate(self):
        for t in self._tasks:
            t.cancel()
        # 套在模型上的包裝拿掉：不然停用後還在記，重載後新實例看到標記也不會重套，一直用舊實例（舊價格）
        for prov, chat, stream in self._wrapped:
            if getattr(prov.text_chat, "_lively_billing", False):
                prov.text_chat = chat
            if stream is not None and getattr(getattr(prov, "text_chat_stream", None), "_lively_billing", False):
                prov.text_chat_stream = stream
        self._wrapped.clear()

    def _save(self):
        try:
            self.state_file.write_text(json.dumps(self.state, ensure_ascii=False), "utf-8")
        except Exception as e:
            logger.warning(f"[lively_billing] 保存状态失败: {e}")

    def _cost(self, ts: float, miss: int, hit: int, out: int, pid: str | None = None) -> float:
        own = self.pid_prices.get(str(pid or ""))
        p_miss, p_hit, p_out = own or self.prices
        k = 2 if self.peak_double and not own and _peak(ts) else 1
        return ((miss or 0) * p_miss + (hit or 0) * p_hit + (out or 0) * p_out) * k / 1e6

    # ---------- 記下每次模型呼叫 ----------
    def _record(self, pid: str, usage, who: str, by: dict | None = None, what: str = ""):
        if usage is None:
            return
        if who == "chat":  # 聊天回覆的錢 AstrBot 自己記在 provider_stats 裡，這裡只另記是誰觸發的
            if by and self.track_users and time.time() - by["t0"] < WHO_TTL:
                self._record_user(pid, usage, by)
            return
        try:
            row = {"ts": int(time.time()), "pid": pid, "who": who, "miss": int(getattr(usage, "input_other", 0) or 0),
                   "hit": int(getattr(usage, "input_cached", 0) or 0), "out": int(getattr(usage, "output", 0) or 0)}
            if what:
                row["what"] = what
            with self.usage_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.debug(f"[lively_billing] 记用量失败: {e}")

    def _record_user(self, pid: str, usage, by: dict):
        try:
            row = {"ts": int(time.time()), "pid": pid, "rid": by["rid"], "qq": by["qq"], "name": by["name"], "gid": by["gid"],
                   "called": by["called"], "miss": int(getattr(usage, "input_other", 0) or 0),
                   "hit": int(getattr(usage, "input_cached", 0) or 0), "out": int(getattr(usage, "output", 0) or 0)}
            with self.users_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.debug(f"[lively_billing] 记谁用的失败: {e}")

    # 優先級排在所有改請求的插件後面：被別的插件攔下（比如自主插件讓掉的主動插話）就輪不到這裡，不會記錯人
    @filter.on_llm_request(priority=-100000)
    async def who_asked(self, event: AstrMessageEvent, req=None):
        if not self.track_users:
            return
        try:
            qq = str(event.get_sender_id() or "")
            if not qq or qq == str(self.s.bot_qq() or ""):
                return
            _WHO.set({"rid": f"{int(time.time())}-{next(_RID)}", "t0": time.time(), "qq": qq, "name": str(event.get_sender_name() or "")[:40],
                      "gid": str(event.get_group_id() or ""), "called": bool(getattr(event, "is_at_or_wake_command", False)) or not event.get_group_id()})
        except Exception as e:
            logger.debug(f"[lively_billing] 认不出是谁: {e}")

    def _wrap(self, prov):
        pid = str(prov.provider_config.get("id", ""))
        orig = prov.text_chat
        if getattr(orig, "_lively_billing", False):
            return False
        bill = self

        names = list(inspect.signature(orig).parameters)

        # 普通函數而不是 async：呼叫的那一刻就看呼叫棧（之後可能被放進別的 task 裡跑，就看不出是誰調的了）
        def text_chat(*args, **kwargs):
            who, by = _caller(), _WHO.get()
            req = bill._dump_req(prov, pid, who, by, names, args, kwargs)
            what = _what(names, args, kwargs) if who == "astrbot" else ""
            who = bill._who_of(who, what, pid)
            return bill._run(orig(*args, **kwargs), pid, who, by, req, what)

        text_chat._lively_billing = True
        prov.text_chat = text_chat
        orig_stream = getattr(prov, "text_chat_stream", None)
        if orig_stream is not None and not getattr(orig_stream, "_lively_billing", False):
            snames = list(inspect.signature(orig_stream).parameters)

            def text_chat_stream(*args, **kwargs):
                who, by = _caller(), _WHO.get()
                req = bill._dump_req(prov, pid, who, by, snames, args, kwargs)
                what = _what(snames, args, kwargs) if who == "astrbot" else ""
                who = bill._who_of(who, what, pid)
                return bill._run_stream(orig_stream(*args, **kwargs), pid, who, by, req, what)

            text_chat_stream._lively_billing = True
            prov.text_chat_stream = text_chat_stream
        else:
            orig_stream = None
        self._wrapped.append((prov, orig, orig_stream))
        return True

    @property
    def pid_who(self) -> dict[str, str]:
        """設定 pid_who 熱讀：改了不用重啟"""
        return _pid_who(self.s.get("pid_who", []))

    def _who_of(self, who: str, what: str, pid: str) -> str:
        """呼叫棧只看得到 AstrBot（別的插件在自己開的任務裡調的）、又不是 AstrBot 自己看圖或壓縮歷史：按 pid_who 認是哪個插件"""
        if who in ("astrbot", "其他") and not what:
            return self.pid_who.get(pid, who)
        return who

    async def _run(self, coro, pid: str, who: str, by: dict | None = None, req: dict | None = None, what: str = ""):
        try:
            resp = await coro
        except BaseException as e:
            self._dump_done(req, None, e)
            raise
        self._record(pid, getattr(resp, "usage", None), who, by, what)
        self._dump_done(req, resp)
        return resp

    async def _run_stream(self, gen, pid: str, who: str, by: dict | None = None, req: dict | None = None, what: str = ""):
        last, final = None, None
        try:
            async for chunk in gen:
                if getattr(chunk, "usage", None) is not None:
                    last = chunk.usage
                if not getattr(chunk, "is_chunk", False):
                    final = chunk
                yield chunk
        except BaseException as e:
            self._dump_done(req, final, e)
            raise
        self._record(pid, last, who, by, what)
        if final is not None and last is not None and getattr(final, "usage", None) is None:
            final.usage = last
        self._dump_done(req, final)

    # ---------- 提示詞留檔 ----------
    def _dump_since(self) -> float | None:
        """留檔開著就回傳從什麼時候開的。設定 prompt_dump 改過以設定為準，主人指令開關蓋過設定；開滿 prompt_dump_hours 自動關"""
        st = self.state.get("dump")
        if not isinstance(st, dict):
            st = self.state["dump"] = {}
        cfg = bool(self.s.get("prompt_dump", False))
        if st.get("cfg") != cfg:
            st.update(cfg=cfg, on=cfg, since=time.time() if cfg else None)
            self._save()
        if not st.get("on"):
            return None
        hours = float(self.s.get("prompt_dump_hours", 48) or 0)
        if hours > 0 and time.time() - float(st.get("since") or 0) > hours * 3600:
            st.update(on=False, since=None)
            self._save()
            logger.info(f"[lively_billing] 提示词留档开满 {hours:g} 小时，自动关了")
            return None
        return float(st.get("since") or time.time())

    def _dump_qqs(self) -> set[str]:
        raw = self.s.get("prompt_dump_qq", []) or []
        if isinstance(raw, str):
            raw = raw.replace("，", ",").replace("\n", ",").split(",")
        qqs = {str(x).strip() for x in raw if str(x).strip()}
        return qqs or {self.s.owner()} - {""}

    def _dump_kind(self, who: str, by: dict | None, params: dict) -> str | None:
        """這次呼叫要不要存：chat（指定的人的私聊回覆）、compress（同一個私聊壓縮歷史）、mind（心跳，另開）"""
        if who == "lively_mind":
            return "mind" if self.s.get("prompt_dump_heartbeat", False) else None
        if who not in ("chat", "astrbot") or not by or by.get("gid") or time.time() - by.get("t0", 0) >= WHO_TTL:
            return None
        if str(by.get("qq")) not in self._dump_qqs():
            return None
        ctx = params.get("contexts") or []
        last = ctx[-1] if isinstance(ctx, list) and ctx else None
        last = last.model_dump() if hasattr(last, "model_dump") else last
        if isinstance(last, dict) and last.get("role") == "user" and _text_of(last.get("content")).startswith(COMPRESS_MARK):
            return "compress"
        return "chat" if who == "chat" else None

    def _dump_req(self, prov, pid: str, who: str, by: dict | None, names: list, args: tuple, kwargs: dict) -> dict | None:
        """呼叫的那一刻把送出去的參數抄下來（之後清單可能被改）；不用存就回 None。出錯只記 debug，不影響回話"""
        try:
            if self._dump_since() is None:
                return None
            params = dict(zip(names, args))
            params.update(kwargs)
            kind = self._dump_kind(who, by, params)
            if not kind:
                return None
            blobs: dict[str, str] = {}
            msgs = []
            if params.get("system_prompt"):
                msgs.append({"role": "system", "content": str(params["system_prompt"])})
            for m in params.get("contexts") or []:
                msgs.append(_plain(m))
            if params.get("prompt") or params.get("image_urls"):
                msgs.append({"role": "user", "content": _no_b64(str(params.get("prompt") or "")),
                             "image_urls": [_no_b64(str(u)) for u in params.get("image_urls") or []]})
            for m in msgs:  # system 每輪大半一樣，正文另存一份
                if isinstance(m, dict) and m.get("role") == "system" and isinstance(m.get("content"), str):
                    h = _sha(m["content"])
                    blobs[h] = m["content"]
                    m["content"] = {"blob": h, "chars": len(blobs[h])}
            row = {"ts": round(time.time(), 3), "kind": kind, "who": who, "pid": pid, "model": "", "messages": msgs}
            try:
                row["model"] = str(params.get("model") or prov.get_model() or "")
            except Exception:
                pass
            if by:
                row.update(rid=by.get("rid"), qq=by.get("qq"), umo=self.s.private_umo(by.get("qq")))
                rid = str(by.get("rid"))
                self._steps[rid] = self._steps.get(rid, 0) + 1
                row["step"] = self._steps[rid]
                if len(self._steps) > 500:
                    for k in list(self._steps)[:250]:
                        self._steps.pop(k, None)
            tools = params.get("func_tool")
            if tools is not None and not isinstance(tools, list):
                try:  # 工具定義抄不下來只少這一塊
                    schema = tools.openai_schema() if hasattr(tools, "openai_schema") else _plain(tools)
                    text = json.dumps(schema, ensure_ascii=False, sort_keys=True)
                    h = _sha(text)
                    blobs[h] = text
                    row["tools"] = [str(t.get("function", {}).get("name", "")) for t in schema if isinstance(t, dict)]
                    row["tools_blob"] = h
                except Exception as e:
                    row["tools_error"] = str(e)[:200]
            for k in ("extra_user_content_parts", "tool_calls_result"):
                if params.get(k):
                    row[k] = _plain(params[k])
            return {"row": row, "blobs": blobs}
        except Exception as e:
            logger.debug(f"[lively_billing] 提示词留档抄参数失败: {e}")
            return None

    def _dump_done(self, req: dict | None, resp, err: BaseException | None = None):
        """補上回覆，丟到別的執行緒寫檔，不等它"""
        if not req:
            return
        try:
            row = req["row"]
            row["secs"] = round(time.time() - row["ts"], 2)
            if err is not None:
                row["error"] = f"{type(err).__name__}: {err}"[:500]
            if resp is not None:
                u = getattr(resp, "usage", None)
                row["reply"] = {
                    "text": _plain(getattr(resp, "completion_text", "") or ""),
                    "think": _plain(getattr(resp, "reasoning_content", None) or ""),
                    "tools": [{"name": n, "args": _plain(a)} for n, a in
                              zip(getattr(resp, "tools_call_name", None) or [], getattr(resp, "tools_call_args", None) or [])],
                    "usage": {"miss": int(getattr(u, "input_other", 0) or 0), "hit": int(getattr(u, "input_cached", 0) or 0),
                              "out": int(getattr(u, "output", 0) or 0)} if u is not None else None,
                }
            t = asyncio.get_running_loop().create_task(asyncio.to_thread(self._dump_write, row, req["blobs"]))
            self._dump_tasks.add(t)
            t.add_done_callback(self._dump_tasks.discard)
        except Exception as e:
            logger.debug(f"[lively_billing] 提示词留档失败: {e}")

    def _dump_write(self, row: dict, blobs: dict):
        try:
            with self._dump_lock:
                bdir = self.dump_dir / "blobs"
                bdir.mkdir(parents=True, exist_ok=True)
                for h, text in blobs.items():
                    f = bdir / f"{h}.txt"
                    if f.exists():
                        continue
                    tmp = bdir / f"{h}.tmp"
                    tmp.write_text(text, "utf-8")
                    tmp.replace(f)
                with (self.dump_dir / f"{_day(row['ts'])}.jsonl").open("a", encoding="utf-8") as f:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.debug(f"[lively_billing] 提示词留档写文件失败: {e}")

    def _prune_dump(self):
        """日檔只留 prompt_dump_days 天，blobs 裡沒人引用的刪掉"""
        if not self.dump_dir.exists():
            return
        days = max(1, int(self.s.get("prompt_dump_days", 3) or 3))
        keep_from = _day(time.time() - (days - 1) * 86400)
        with self._dump_lock:
            used: set = set()
            for f in sorted(self.dump_dir.glob("*.jsonl")):
                if f.stem < keep_from:
                    f.unlink(missing_ok=True)
                    continue
                with f.open(encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            _refs(json.loads(line), used)
                        except Exception:
                            continue
            now = time.time()
            for b in (self.dump_dir / "blobs").glob("*"):
                if b.stem not in used and now - b.stat().st_mtime > BLOB_GRACE:
                    b.unlink(missing_ok=True)

    def dump_status(self) -> dict:
        since = self._dump_since()
        hours = float(self.s.get("prompt_dump_hours", 48) or 0)
        n, size = 0, 0
        today = self.dump_dir / f"{_day(time.time())}.jsonl"
        try:
            with today.open(encoding="utf-8") as f:
                n = sum(1 for _ in f)
        except FileNotFoundError:
            pass
        if self.dump_dir.exists():
            size = sum(f.stat().st_size for f in self.dump_dir.rglob("*") if f.is_file())
        return {"on": since is not None, "since": since, "until": since + hours * 3600 if since and hours > 0 else None,
                "today": n, "bytes": size, "qq": sorted(self._dump_qqs()),
                "heartbeat": bool(self.s.get("prompt_dump_heartbeat", False)), "days": int(self.s.get("prompt_dump_days", 3) or 3)}

    def dump_switch(self, on: bool):
        st = self.state.get("dump")
        if not isinstance(st, dict):
            st = self.state["dump"] = {}
        st.setdefault("cfg", bool(self.s.get("prompt_dump", False)))
        st.update(on=on, since=time.time() if on else None)
        self._save()

    async def _wrap_loop(self):
        await asyncio.sleep(5)
        while True:
            try:
                n = sum(self._wrap(p) for p in self.context.get_all_providers())
                if n:
                    logger.info(f"[lively_billing] 开始记 {n} 个模型的调用用量")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[lively_billing] 套用量记录失败: {e}")
            await asyncio.sleep(WRAP_EVERY)

    # ---------- 密鑰和用途 ----------
    def _providers(self) -> list[dict]:
        """要記帳的 DeepSeek 模型：[{id, key, label, uses}]。key 只在內存裡用，不寫文件、不打印"""
        d = json.loads(CMD_CONFIG.read_text("utf-8-sig"))
        srcs = {s.get("id"): s for s in d.get("provider_sources", [])}
        want = [str(x) for x in (self.s.get("providers", []) or [])]
        out = []
        for p in d.get("provider", []):
            if p.get("provider_type", "chat_completion") != "chat_completion" or p.get("enable") is False:
                continue
            src = srcs.get(p.get("provider_source_id"), {})
            base = str(p.get("api_base") or src.get("api_base") or "")
            if want and p.get("id") not in want or not want and "deepseek.com" not in base:
                continue
            key = p.get("key") or src.get("key") or []
            key = key[0] if isinstance(key, list) and key else key
            if key:
                out.append({"id": p["id"], "key": str(key)})
        try:
            if key := STUDY_KEY.read_text("utf-8").strip():
                out.append({"id": STUDY_ID, "key": key})
        except OSError:
            pass
        uses = self._uses()
        uses.setdefault(STUDY_ID, ["电脑上做长任务（DSH）"])
        labels = {}
        for line in self.s.get("labels", []) or []:
            pid, _, name = str(line).replace("：", "=").replace(":", "=").partition("=")
            if pid.strip() and name.strip():
                labels[pid.strip()] = name.strip()
        for p in out:
            p["uses"] = uses.get(p["id"], [])
            p["label"] = labels.get(p["id"]) or ("、".join(p["uses"]) if p["uses"] else p["id"])
        return out

    def _group_names(self) -> dict:
        names = {}
        try:
            con = sqlite3.connect(f"file:{STATS_DB}?mode=ro", uri=True)
            for umo, name in con.execute("select umo, auto_name from umo_aliases"):
                if ":GroupMessage:" in umo and name:
                    names[umo.rsplit(":", 1)[-1]] = name
            con.close()
        except Exception:
            pass
        return names

    def _uses(self) -> dict[str, list[str]]:
        """每個模型 ID 被拿去做什麼：從門禁插件的配置看哪些群、私聊用它"""
        meta = self.context.get_registered_star(GATE_PLUGIN)
        acc = (meta.config if meta and meta.config is not None else {}) or {}
        names = self._group_names()
        per_group = {}
        for line in acc.get("group_providers", []) or []:
            gid, _, pid = str(line).replace("：", "=").replace(":", "=").partition("=")
            if gid.strip().isdigit() and pid.strip():
                per_group[gid.strip()] = pid.strip()
        uses: dict[str, list[str]] = {}
        default = acc.get("group_provider_id") or ""
        for gid in self.s.groups():
            pid = per_group.get(gid) or default
            if pid:
                uses.setdefault(pid, []).append(f"群「{names.get(gid, gid)}」")
        if acc.get("dm_provider_id"):
            uses.setdefault(acc["dm_provider_id"], []).append("私聊")
        # LivingMemory 的總結/提取模型
        try:
            lm = json.loads((CONFIG_DIR / "astrbot_plugin_livingmemory_config.json").read_text("utf-8-sig"))
            pid = (lm.get("provider_settings") or {}).get("llm_provider_id")
            if pid:
                uses.setdefault(pid, []).append("记忆插件")
        except Exception:
            pass
        return uses

    # ---------- 查餘額 ----------
    async def fetch(self) -> dict:
        """每把不同的密鑰查一次餘額。返回 {"ts", "keys": {模型ID: 餘額}}，同一把密鑰的幾個模型只查一次"""
        provs = self._providers()
        by_key: dict[str, list[str]] = {}
        for p in provs:
            by_key.setdefault(p["key"], []).append(p["id"])
        snap = {"ts": int(time.time()), "keys": {}}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as s:
            for key, pids in by_key.items():
                try:
                    async with s.get(BALANCE_URL, headers={"Authorization": f"Bearer {key}"}) as r:
                        data = await r.json(content_type=None)
                        if r.status != 200:
                            raise RuntimeError(f"HTTP {r.status}")
                    info = next((b for b in data.get("balance_infos", []) if b.get("currency") == "CNY"), None)
                    if info:
                        for pid in pids:
                            snap["keys"][pid] = float(info["total_balance"])
                except Exception as e:
                    logger.warning(f"[lively_billing] 查 {pids[0]} 的余额失败: {e}")
        if not snap["keys"]:
            raise RuntimeError("一个余额都没查到")
        with self.snap_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(snap) + "\n")
        return snap

    def _snaps(self, since: float) -> list[dict]:
        out = []
        fallback = (self._providers() or [{"id": "deepseek"}])[0]["id"]
        try:
            for line in self.snap_file.read_text("utf-8").splitlines():
                try:
                    s = json.loads(line)
                except Exception:
                    continue
                if s["ts"] < since:
                    continue
                if "keys" not in s and "total" in s:  # 舊格式：只查了一把密鑰
                    s = {"ts": s["ts"], "keys": {fallback: s["total"]}}
                out.append(s)
        except FileNotFoundError:
            pass
        return out

    @staticmethod
    def _accounts(snap: dict) -> list[list[str]]:
        """把餘額完全一樣的密鑰歸成一個帳戶（DeepSeek 同一個帳戶下的密鑰共用餘額）"""
        by_bal: dict[float, list[str]] = {}
        for pid, bal in snap.get("keys", {}).items():
            by_bal.setdefault(round(bal, 2), []).append(pid)
        return sorted(by_bal.values(), key=lambda x: x[0])

    # ---------- 算賬 ----------
    def _chat_costs(self, t0: float, t1: float) -> dict[str, float]:
        """聊天回覆：模型ID -> 元"""
        out: dict[str, float] = {}
        try:
            con = sqlite3.connect(f"file:{STATS_DB}?mode=ro", uri=True)
            rows = con.execute(
                "SELECT provider_id, start_time, token_input_other, token_input_cached, token_output FROM provider_stats "
                "WHERE start_time >= ? AND start_time < ?", (t0, t1)).fetchall()
            con.close()
        except Exception:
            return out
        for pid, ts, m, h, o in rows:
            out[pid or "?"] = out.get(pid or "?", 0.0) + self._cost(ts, m, h, o, pid)
        return out

    def _bg_costs(self, t0: float, t1: float) -> dict[str, dict[str, float]]:
        """後台活：模型ID -> {誰: 元}"""
        out: dict[str, dict[str, float]] = {}
        try:
            for line in self.usage_file.read_text("utf-8").splitlines():
                try:
                    u = json.loads(line)
                except Exception:
                    continue
                if t0 <= u["ts"] < t1:
                    by = out.setdefault(u["pid"], {})
                    by[u["who"]] = by.get(u["who"], 0.0) + self._cost(u["ts"], u["miss"], u["hit"], u["out"], u["pid"])
        except FileNotFoundError:
            pass
        return out

    def _tracking_since(self) -> float | None:
        try:
            with self.usage_file.open(encoding="utf-8") as f:
                return float(json.loads(f.readline())["ts"])
        except Exception:
            return None

    def _prune_usage(self):
        cutoff = time.time() - KEEP_USAGE_DAYS * 86400
        for file in (self.usage_file, self.users_file):
            try:
                lines = file.read_text("utf-8").splitlines()
                keep = [x for x in lines if _ts(x) >= cutoff]  # 壞掉的行（寫到一半被重啟）丟掉，不讓整個清理放棄
                if len(keep) < len(lines):
                    tmp = file.with_suffix(".tmp")
                    tmp.write_text("\n".join(keep) + ("\n" if keep else ""), "utf-8")
                    tmp.replace(file)
            except FileNotFoundError:
                pass
            except Exception as e:
                logger.warning(f"[lively_billing] 清理旧用量失败: {e}")

    # ---------- 語音（BreezeBlue 點數）----------
    async def fetch_voice(self) -> dict | None:
        """查 BreezeBlue 的點數餘額和最近 VOICE_DAYS 天每次請求扣的點數，存 voice.json；沒配密鑰回 None"""
        try:
            cfg = json.loads(VOICE_CONF.read_text("utf-8-sig"))
        except Exception:
            return None
        key = str(cfg.get("breeze_key") or "")
        if not key:
            return None
        base = str(cfg.get("breeze_base") or "https://api.breeze.blue/v1").rstrip("/")
        head = {"xi-api-key": key, "User-Agent": "lively-billing"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), headers=head) as s:
            async with s.get(base + "/balance") as r:
                if r.status != 200:
                    raise RuntimeError(f"BreezeBlue 余额回了 {r.status}")
                bal = await r.json(content_type=None)
            rows, page = [], 1
            while page <= VOICE_PAGES:
                async with s.get(base + "/usage", params={"days": VOICE_DAYS, "page": page, "page_size": 100}) as r:
                    if r.status != 200:
                        raise RuntimeError(f"BreezeBlue 用量回了 {r.status}")
                    u = await r.json(content_type=None)
                items = u.get("items") or []
                for it in items:
                    try:
                        ts = datetime.fromisoformat(str(it["created_at"]).replace("Z", "+00:00")).timestamp()
                    except Exception:
                        continue
                    rows.append({"ts": int(ts), "credits": float(it.get("cost") or 0), "units": int(it.get("billable_units") or 0),
                                 "status": it.get("status"), "what": "tts" if "/text-to-speech/" in str(it.get("endpoint")) else str(it.get("action_code") or "")})
                if len(items) < 100 or page * 100 >= int(u.get("total") or 0):
                    break
                page += 1
        out = {"ts": int(time.time()), "balance": float(bal.get("balance") or 0),
               "topups": [{"ts": t.get("created_at"), "amount": t.get("amount"), "what": t.get("description")} for t in bal.get("topups") or []][:10],
               "rows": sorted(rows, key=lambda x: x["ts"])}
        try:
            self.voice_file.write_text(json.dumps(out, ensure_ascii=False), "utf-8")
        except Exception as e:
            logger.warning(f"[lively_billing] 存语音用量失败: {e}")
        return out

    def voice_info(self) -> dict | None:
        try:
            return json.loads(self.voice_file.read_text("utf-8"))
        except Exception:
            return None

    def voice(self, t0: float, t1: float, info: dict | None = None) -> dict:
        """這段時間 BreezeBlue 扣的：條數（合成成功的語音）、點數（所有扣點的請求）、估的錢"""
        info = info if info is not None else self.voice_info()
        n, cr = 0, 0.0
        for r in (info or {}).get("rows") or []:
            if t0 <= r["ts"] < t1:
                cr += r["credits"]
                n += r["what"] == "tts" and r.get("status") == 200
        return {"n": n, "credits": round(cr, 1), "cost": round(cr / 10000 * self.voice_price, 4)}

    @staticmethod
    def images(t0: float, t1: float) -> int:
        n = 0
        f = plugin_data_dir("astrbot_plugin_lively_comic") / "events.jsonl"
        try:
            for line in f.read_text("utf-8").splitlines():
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if t0 <= e.get("ts", 0) < t1 and e.get("kind") in ("draw", "draft"):
                    n += 1
        except FileNotFoundError:
            pass
        return n

    def users(self, days: int = 30) -> dict:
        """按人匯總聊天回覆的用量：誰觸發的、花了多少、什麼時候來、在哪聊。給面板「饭钱」頁用。
        開始按人記之前的私聊從 provider_stats 補（私聊的 umo 就是對方 QQ）；群聊的舊記錄對不上人，只算進「沒對上人」"""
        now = time.time()
        today0 = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        t0 = (today0 - timedelta(days=days - 1)).timestamp()
        bot = str(self.s.bot_qq() or "")
        ppl: dict[str, dict] = {}
        heat = [[0] * 24 for _ in range(7)]  # 星期 × 小時的對話輪數
        daily: dict[str, dict] = {}  # 日期 -> {"user": 元, "chat": 元, "bg": 元}

        def person(qq):
            return ppl.setdefault(qq, {"qq": qq, "name": "", "cost": 0.0, "miss": 0, "hit": 0, "out": 0, "turns": 0, "called": 0,
                                       "private": 0, "groups": {}, "hours": [0] * 24, "wdays": [0] * 7, "days": {}, "first": None, "last": 0,
                                       "_rids": set()})

        def add(qq, name, gid, ts, miss, hit, out, rid, called, pid=None):
            u = person(qq)
            if name:
                u["name"] = name
            c = self._cost(ts, miss, hit, out, pid)
            d = datetime.fromtimestamp(ts, TZ)
            key = d.strftime("%Y-%m-%d")
            u["cost"] += c
            u["miss"] += miss
            u["hit"] += hit
            u["out"] += out
            u["days"][key] = u["days"].get(key, 0.0) + c
            daily.setdefault(key, {"user": 0.0, "chat": 0.0, "bg": 0.0})["user"] += c
            u["first"] = ts if u["first"] is None else min(u["first"], ts)
            u["last"] = max(u["last"], ts)
            if rid in u["_rids"]:  # 同一輪的工具調用不重複算次數
                return
            u["_rids"].add(rid)
            u["turns"] += 1
            u["called"] += 1 if called else 0
            if gid:
                u["groups"][gid] = u["groups"].get(gid, 0) + 1
            else:
                u["private"] += 1
            u["hours"][d.hour] += 1
            u["wdays"][d.weekday()] += 1
            heat[d.weekday()][d.hour] += 1

        since = None
        try:
            for line in self.users_file.read_text("utf-8").splitlines():
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                since = r["ts"] if since is None else min(since, r["ts"])
                if r["ts"] < t0 or str(r.get("qq")) in ("", bot):
                    continue
                add(str(r["qq"]), r.get("name", ""), str(r.get("gid") or ""), r["ts"], r.get("miss", 0), r.get("hit", 0), r.get("out", 0),
                    r.get("rid") or f"{r['ts']}", r.get("called", True), r.get("pid"))
        except FileNotFoundError:
            pass
        # 聊天回覆一共花的（provider_stats），和開始按人記之前的私聊
        chat_total = 0.0
        try:
            con = sqlite3.connect(f"file:{STATS_DB}?mode=ro", uri=True)
            rows = con.execute("SELECT id, umo, start_time, token_input_other, token_input_cached, token_output, provider_id FROM provider_stats "
                               "WHERE start_time >= ? AND start_time < ?", (t0, now)).fetchall()
            con.close()
        except Exception:
            rows = []
        for rid, umo, ts, m, h, o, pid in rows:
            c = self._cost(ts, m, h, o, pid)
            chat_total += c
            daily.setdefault(_day(ts), {"user": 0.0, "chat": 0.0, "bg": 0.0})["chat"] += c
            if since is not None and ts >= since:
                continue
            parts = str(umo or "").split(":")
            if len(parts) == 3 and parts[1] == "FriendMessage" and parts[2] != bot:
                add(parts[2], "", "", ts, m or 0, h or 0, o or 0, f"ps{rid}", True, pid)
        bg = {}
        for by in self._bg_costs(t0, now).values():
            for w, v in by.items():
                bg[CALLER_NAMES.get(w, w)] = bg.get(CALLER_NAMES.get(w, w), 0.0) + v
        try:
            for line in self.usage_file.read_text("utf-8").splitlines():
                try:
                    u = json.loads(line)
                except Exception:
                    continue
                if u["ts"] >= t0:
                    daily.setdefault(_day(u["ts"]), {"user": 0.0, "chat": 0.0, "bg": 0.0})["bg"] += self._cost(u["ts"], u["miss"], u["hit"], u["out"], u["pid"])
        except FileNotFoundError:
            pass
        names = self._group_names()
        out = []
        for u in ppl.values():
            u.pop("_rids")
            u["cost"] = round(u["cost"], 4)
            u["days"] = {k: round(v, 4) for k, v in sorted(u["days"].items())}
            u["active_days"] = len(u["days"])
            out.append(u)
        out.sort(key=lambda u: -u["cost"])
        user_cost = sum(u["cost"] for u in out)
        return {
            "days": days, "from": t0, "tracking_since": since, "users": out,
            "group_names": names, "heat": heat,
            "daily": [{"day": k, **{x: round(y, 4) for x, y in v.items()}} for k, v in sorted(daily.items())],
            "total": {"chat": round(chat_total, 2), "users": round(user_cost, 2), "unmatched": round(max(0.0, chat_total - user_cost), 2),
                      "background": {k: round(v, 3) for k, v in sorted(bg.items(), key=lambda x: -x[1])}},
        }

    def _usage_rows(self, t0: float, t1: float) -> list[dict]:
        """usage.jsonl 裡這段時間的行。AstrBot 自己看圖、壓縮歷史開始標 what 以後，沒標的就不是它自己的事：
        按 pid_who 認成插件，認不出記「其他」（以前的舊記錄照舊猜）"""
        rows = []
        try:
            for line in self.usage_file.read_text("utf-8").splitlines():
                try:
                    u = json.loads(line)
                    float(u["ts"])
                except Exception:
                    continue
                rows.append(u)
        except FileNotFoundError:
            return []
        since = min((u["ts"] for u in rows if u.get("what")), default=None)
        out, pid_who = [], self.pid_who
        for u in rows:
            if not t0 <= u["ts"] < t1:
                continue
            if u.get("who") == "astrbot" and not u.get("what") and since is not None and u["ts"] >= since:
                u = dict(u, who=pid_who.get(str(u.get("pid", "")), "其他"))
            out.append(u)
        return out

    def spend(self, days: int = 30) -> dict:
        """錢花在哪：按做的事分類（SPEND_CATS），每類花了多少、做了幾次；不算錢的（降智版）只記次數。
        餘額實際掉的比記到的多，多出來的單列「沒記到的」（只算有餘額記錄、而且開始記後台之後的整天）"""
        now = time.time()
        today0 = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        t0 = (today0 - timedelta(days=days - 1)).timestamp()
        cats = {k: {"id": k, "name": n, "desc": d, "cost": 0.0, "n": 0} for k, n, d in SPEND_CATS}
        free = {"n": 0, "chat": 0}
        daily: dict[str, dict] = {}
        day_of = lambda ts: daily.setdefault(_day(ts), {"group": 0.0, "private": 0.0, "bg": 0.0, "unrecorded": 0.0})

        is_free = lambda pid: self.pid_prices.get(str(pid or "")) == (0.0, 0.0, 0.0)

        def add(cat, ts, c, pid):
            cats[cat]["cost"] += c
            cats[cat]["n"] += 1
            if is_free(pid):
                free["n"] += 1
                if cat in ("group", "private"):
                    free["chat"] += 1
            day_of(ts)[cat if cat in ("group", "private") else "bg"] += c

        try:
            con = sqlite3.connect(f"file:{STATS_DB}?mode=ro", uri=True)
            rows = con.execute("SELECT umo, provider_id, start_time, token_input_other, token_input_cached, token_output FROM provider_stats "
                               "WHERE start_time >= ? AND start_time < ?", (t0, now)).fetchall()
            con.close()
        except Exception:
            rows = []
        for umo, pid, ts, m, h, o in rows:
            add("private" if ":FriendMessage:" in str(umo or "") else "group", ts, self._cost(ts, m, h, o, pid), pid)
        for u in self._usage_rows(t0, now):
            add(_spend_cat(u), u["ts"], self._cost(u["ts"], u["miss"], u["hit"], u["out"], u["pid"]), u["pid"])
        # 餘額掉的比記到的多：從每天的帳（summary）拿
        unrec, actual, actual_days = 0.0, 0.0, 0
        try:
            for d in self.summary(days)["days"]:
                if d.get("actual") is not None and not d.get("partial"):
                    actual += d["actual"]
                    actual_days += 1
                if d.get("unattributed"):
                    unrec += d["unattributed"]
                    daily.setdefault(d["day"], {"group": 0.0, "private": 0.0, "bg": 0.0, "unrecorded": 0.0})["unrecorded"] = d["unattributed"]
        except Exception as e:
            logger.debug(f"[lively_billing] 算没记到的失败: {e}")
        imgs = self.images(t0, now)
        out = [dict(c, cost=round(c["cost"], 3)) for c in cats.values() if c["n"]]
        return {
            "days": days, "from": t0, "cats": out, "free": free,
            "recorded": round(sum(c["cost"] for c in cats.values()), 2), "unrecorded": round(unrec, 2),
            "actual": round(actual, 2) if actual_days else None, "actual_days": actual_days,
            "images": {"n": imgs, "cost": round(imgs * self.image_price, 2), "price": self.image_price},
            "voice": self.voice(t0, now),
            "daily": [{"day": k, **{x: round(y, 3) for x, y in v.items()}} for k, v in sorted(daily.items())],
        }

    # ---------- 按天的帳 ----------
    def ledger(self, days: int = 30) -> dict:
        """按天的帳（面板「饭钱」頁）：每天餘額實際掉了多少、記到的花在哪幾類（SPEND_CATS）、每個鐘點、誰、最貴的幾次、
        快取省了多少、高峰多花了多少、降智版要是走 DeepSeek 得花多少、充值和投餵。
        餘額大約每小時查一次，兩次之間掉的按時間攤到經過的每個鐘點；比同一段時間記到的多出來的是「沒記到的」"""
        now = time.time()
        today0 = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        t0 = (today0 - timedelta(days=days - 1)).timestamp()
        nh = days * 24
        hour = lambda ts: int((ts - t0) // 3600)
        dp = self.prices
        bot = str(self.s.bot_qq() or "")
        group_names = self._group_names()
        try:
            labels = {p["id"]: p["label"] for p in self._providers()}
        except Exception:
            labels = {}
        D: dict[str, dict] = {}

        def day(ts):
            k = _day(ts)
            if k not in D:
                D[k] = {"cats": {}, "tok": [0, 0, 0], "peak_extra": 0.0, "cache_saved": 0.0, "free": [0, 0.0], "keys": {},
                        "people": {}, "top": [], "img": 0, "voice": [0, 0.0], "feeds": []}
            return D[k]

        rec = [[0.0, 0.0, 0.0] for _ in range(nh)]  # 每個鐘點記到的：群裡聊天、私聊、後台
        calls: list[tuple[float, str, float]] = []  # (時間, 模型設定, 元)：充值那段、算沒記到的時候用

        def add(ts, pid, m, h, o, cat, what):
            pid, m, h, o = str(pid or "?"), int(m or 0), int(h or 0), int(o or 0)
            own = self.pid_prices.get(pid)
            peak = self.peak_double and _peak(ts)
            pm, ph, po = own or dp
            k = 2 if peak and not own else 1
            base = (m * pm + h * ph + o * po) / 1e6
            cost = base * k
            d = day(ts)
            c = d["cats"].setdefault(cat, [0.0, 0])
            c[0] += cost
            c[1] += 1
            if own == (0.0, 0.0, 0.0):  # 不扣 DeepSeek 的（降智版）：記次數，和照 DeepSeek 的價要花多少
                d["free"][0] += 1
                d["free"][1] += (m * dp[0] + h * dp[1] + o * dp[2]) * (2 if peak else 1) / 1e6
            else:
                d["tok"][0] += m
                d["tok"][1] += h
                d["tok"][2] += o
                d["peak_extra"] += base * (k - 1)
                d["cache_saved"] += h * (pm - ph) * k / 1e6
            if cost > 0:
                d["keys"][pid] = d["keys"].get(pid, 0.0) + cost
                top = d["top"]
                top.append({"ts": int(ts), "cat": cat, "what": what, "pid": labels.get(pid, pid), "in": m + h, "hit": h, "out": o,
                            "peak": k == 2, "cost": cost})
                if len(top) > 40:
                    top.sort(key=lambda x: -x["cost"])
                    del top[8:]
            i = hour(ts)
            if 0 <= i < nh:
                rec[i][0 if cat == "group" else 1 if cat == "private" else 2] += cost
            calls.append((ts, pid, cost))

        # 按人：聊天回覆是誰觸發的（users.jsonl）；順手攢一份 QQ → 名字
        names: dict[str, str] = {}
        users_since = None
        try:
            for line in self.users_file.read_text("utf-8").splitlines():
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                qq = str(r.get("qq") or "")
                if r.get("name"):
                    names[qq] = str(r["name"])
                users_since = r["ts"] if users_since is None else min(users_since, r["ts"])
                if not t0 <= r["ts"] < now or qq in ("", bot):
                    continue
                p = day(r["ts"])["people"].setdefault(qq, [0.0, set()])
                p[0] += self._cost(r["ts"], r.get("miss", 0), r.get("hit", 0), r.get("out", 0), r.get("pid"))
                p[1].add(r.get("rid") or r["ts"])
        except FileNotFoundError:
            pass
        # users.jsonl 記的是那時的群名片，同一個人在不同群不一樣；有情感插件記的名字（QQ 暱稱）就用它
        names.update(self.bond_names())
        # 聊天回覆（provider_stats）
        try:
            con = sqlite3.connect(f"file:{STATS_DB}?mode=ro", uri=True)
            rows = con.execute("SELECT umo, provider_id, start_time, token_input_other, token_input_cached, token_output FROM provider_stats "
                               "WHERE start_time >= ? AND start_time < ?", (t0 - 2 * EVERY, now)).fetchall()
            con.close()
        except Exception:
            rows = []
        for umo, pid, ts, m, h, o in rows:
            parts = str(umo or "").split(":")
            private = len(parts) == 3 and parts[1] == "FriendMessage"
            who = parts[-1] if parts else ""
            what = f"私聊 {names.get(who) or who}" if private else f"群「{group_names.get(who, who)}」"
            add(ts, pid, m, h, o, "private" if private else "group", what)
        # 後台
        for u in self._usage_rows(t0 - 2 * EVERY, now):
            who = str(u.get("who", ""))
            what = CALLER_NAMES.get(who, who)
            if who == "astrbot" and u.get("what"):
                what = {"caption": "AstrBot 看图", "compress": "AstrBot 压缩聊天"}.get(u["what"], what)
            add(u["ts"], u["pid"], u.get("miss"), u.get("hit"), u.get("out"), _spend_cat(u), what)
        # 畫圖（中轉站另算，按張數估）
        try:
            for line in (plugin_data_dir("astrbot_plugin_lively_comic") / "events.jsonl").read_text("utf-8").splitlines():
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if t0 <= e.get("ts", 0) < now and e.get("kind") in ("draw", "draft"):
                    day(e["ts"])["img"] += 1
        except FileNotFoundError:
            pass
        # 語音（BreezeBlue 點數，另算）
        vinfo = self.voice_info()
        for r in (vinfo or {}).get("rows") or []:
            if t0 <= r["ts"] < now:
                v = day(r["ts"])["voice"]
                v[0] += r["what"] == "tts" and r.get("status") == 200
                v[1] += r["credits"]
        for f in self.feeds():
            if t0 <= f.get("ts", 0) < now:
                day(f["ts"])["feeds"].append({"qq": f.get("qq"), "name": f.get("name") or f.get("qq"), "amount": f.get("amount", 0)})

        # 餘額：兩次之間掉的按時間攤到每個鐘點；漲了是充值，那一段按記到的算
        snaps = self._snaps(t0 - 2 * EVERY)
        accounts = self._accounts(snaps[-1]) if snaps else []
        act, cov = [0.0] * nh, [0.0] * nh
        topups, series = [], []
        for s_ in snaps:
            bals = [next((s_["keys"][p] for p in acct if p in s_["keys"]), None) for acct in accounts]
            if bals and None not in bals and s_["ts"] >= t0 - EVERY:
                series.append([int(s_["ts"]), round(sum(bals), 2)])
        for a, b in zip(snaps, snaps[1:]):
            if b["ts"] <= t0 or b["ts"] <= a["ts"]:
                continue
            drop, seen = 0.0, False
            for acct in accounts:
                rep = next((p for p in acct if p in a["keys"] and p in b["keys"]), None)
                if rep is None:
                    continue
                seen = True
                x = a["keys"][rep] - b["keys"][rep]
                if x < -0.005:
                    est = sum(c for ts, pid, c in calls if a["ts"] <= ts < b["ts"] and pid in acct)
                    topups.append({"ts": int(b["ts"]), "amount": round(-x + est, 2)})
                    x = est
                drop += max(0.0, x)
            if not seen:
                continue
            span, s0 = b["ts"] - a["ts"], max(a["ts"], t0)
            i = hour(s0)
            while s0 < b["ts"] and i < nh:
                e = min(b["ts"], t0 + (i + 1) * 3600)
                act[i] += drop * (e - s0) / span
                cov[i] += e - s0
                s0, i = e, i + 1
        first_snap = snaps[0]["ts"] if snaps else None
        last_snap = snaps[-1]["ts"] if snaps else None
        track_since = self._tracking_since()
        topup_day: dict[str, float] = {}
        for t in topups:
            if t["ts"] >= t0:
                topup_day[_day(t["ts"])] = topup_day.get(_day(t["ts"]), 0.0) + t["amount"]

        out = []
        r2, r4 = (lambda v: round(v, 2)), (lambda v: round(v, 4))
        for n in range(days):
            d0 = today0 + timedelta(days=n - days + 1)
            key = d0.strftime("%Y-%m-%d")
            t_a, t_b = d0.timestamp(), d0.timestamp() + 86400
            d = D.get(key) or day(t_a)
            hs = range(n * 24, n * 24 + 24)
            seen = any(cov[i] > 0 for i in hs)
            actual = sum(act[i] for i in hs) if seen else None
            recorded = sum(c[0] for c in d["cats"].values())
            # 和餘額比的只算查過餘額的那段時間裡記到的
            lo, hi = max(t_a, first_snap or t_b), min(t_b, last_snap or t_a)
            rec_cov = sum(c for ts, _, c in calls if lo <= ts < hi)
            unrec = max(0.0, actual - rec_cov) if actual is not None and track_since and track_since <= t_a else None
            is_today = key == _day(now)
            people = sorted(({"qq": q, "name": names.get(q, q), "cost": r4(v[0]), "n": len(v[1])} for q, v in d["people"].items()),
                            key=lambda x: -x["cost"])
            top = sorted(d["top"], key=lambda x: -x["cost"])[:8]
            for x in top:
                x["cost"] = r4(x["cost"])
            miss, hit, outp = d["tok"]
            bal_in = [v for ts, v in series if t_a - EVERY <= ts < t_b]
            out.append({
                "day": key, "wd": d0.weekday(), "today": is_today,
                "partial": is_today or first_snap is None or first_snap > t_a + EVERY,
                "actual": r2(actual) if actual is not None else None, "recorded": r2(recorded),
                "unrec": r2(unrec) if unrec is not None else None,
                "cats": {k: [r4(v[0]), v[1]] for k, v in d["cats"].items()},
                "hours": [[r4(rec[i][0]), r4(rec[i][1]), r4(rec[i][2]), r4(act[i]) if cov[i] > 0 else None] for i in hs],
                "img": d["img"], "img_cost": r2(d["img"] * self.image_price),
                "voice": [d["voice"][0], round(d["voice"][1], 1), r4(d["voice"][1] / 10000 * self.voice_price)],
                "tok": [miss, hit, outp], "hit_rate": round(hit / (miss + hit), 3) if miss + hit else None,
                "peak_extra": r2(d["peak_extra"]), "cache_saved": r2(d["cache_saved"]),
                "free": [d["free"][0], r2(d["free"][1])],
                "keys": sorted(({"id": k, "label": labels.get(k, k), "cost": r4(v)} for k, v in d["keys"].items()), key=lambda x: -x["cost"]),
                "people": people[:10], "people_n": len(people), "top": top,
                "topup": r2(topup_day.get(key, 0.0)), "feeds": d["feeds"],
                "bal": [bal_in[0], bal_in[-1]] if bal_in else None,
            })
        full = [x["actual"] for x in out if x["actual"] is not None and not x["partial"]][-3:]
        bal = series[-1][1] if series else None
        per_day = r2(sum(full) / len(full)) if full else None
        runway = round(bal / per_day, 1) if bal is not None and per_day else None
        if len(series) > 500:  # 一小時留一個點就夠畫線
            keep, last_h = [], None
            for ts, v in series:
                h = int(ts // 3600)
                if h == last_h:
                    keep[-1] = [ts, v]
                else:
                    keep.append([ts, v])
                last_h = h
            series = keep
        return {
            "days": days, "from": t0, "now": int(now), "updated": last_snap, "first_snap": first_snap,
            "tracking_since": track_since, "users_since": users_since,
            "cats": [{"id": k, "name": n, "desc": d} for k, n, d in SPEND_CATS],
            "list": out, "balance": series, "topups": [t for t in topups if t["ts"] >= t0],
            "bal": bal, "per_day": per_day, "runway": runway, "alert_at": self.alert_at,
            "image_price": self.image_price, "prices": list(dp), "peak_double": self.peak_double,
            "voice": ({"bal": vinfo.get("balance"), "updated": vinfo.get("ts"), "price_10k": self.voice_price,
                       "topups": vinfo.get("topups") or []} if vinfo else None),
        }

    # ---------- 投餵記錄：群友給〈我〉的飯錢（帳本，不經手錢）----------
    def feeds(self) -> list[dict]:
        try:
            rows = json.loads(self.feeds_file.read_text("utf-8"))
            return sorted([r for r in rows if isinstance(r, dict)], key=lambda r: -r.get("ts", 0))
        except FileNotFoundError:
            return []
        except Exception as e:
            logger.warning(f"[lively_billing] 读投喂记录失败: {e}")
            return []

    def _save_feeds(self, rows: list[dict]):
        tmp = self.feeds_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=1), "utf-8")
        tmp.replace(self.feeds_file)

    def add_feed(self, qq: str, amount: float, ts: float | None = None, note: str = "", name: str = "", via: str = "",
                 told_by: str = "", told_name: str = "", told_owner: bool = False) -> dict:
        qq = str(qq).strip()
        if not qq.isdigit() or not 5 <= len(qq) <= 12:
            raise ValueError("QQ 号不对")
        amount = round(float(amount), 2)
        if not 0 < amount <= FEED_MAX:
            raise ValueError("金额不对")
        now = time.time()
        ts = float(ts or now)
        if ts > now + 86400 or ts < now - 3 * 365 * 86400:
            raise ValueError("日期不对")
        row = {"id": "", "ts": int(ts), "qq": qq, "name": str(name or "")[:40], "amount": amount,
               "note": str(note or "").strip()[:60], "added": int(now), "via": via}
        if told_by:  # 〈我〉在聊天裡聽誰說的
            row.update(told_by=str(told_by), told_name=str(told_name or "")[:40], told_owner=bool(told_owner))
        with self._feeds_lock:
            rows = self.feeds()
            ids, n = {r.get("id") for r in rows}, int(now * 1000)
            while str(n) in ids:  # 同一毫秒記了兩筆
                n += 1
            row["id"] = str(n)
            rows.append(row)
            self._save_feeds(rows)
        logger.info(f"[lively_billing] 记了一笔投喂：{qq} ¥{amount:g}")
        return row

    def del_feed(self, fid: str) -> dict | None:
        with self._feeds_lock:
            rows = self.feeds()
            hit = next((r for r in rows if r.get("id") == str(fid)), None)
            if hit:
                self._save_feeds([r for r in rows if r is not hit])
                logger.info(f"[lively_billing] 删了一笔投喂：{hit['qq']} ¥{hit['amount']:g}")
        return hit

    @staticmethod
    def bond_names() -> dict[str, str]:
        """情感插件記的全部名字 {QQ: 名字}（10-27 起是 QQ 暱稱，不跟著群名片變）"""
        try:
            con = sqlite3.connect(f"file:{plugin_data_dir('astrbot_plugin_lively_bond') / 'bond.db'}?mode=ro", uri=True)
            rows = con.execute("select qq, name from people").fetchall()
            con.close()
            return {str(q): str(n) for q, n in rows if n}
        except Exception:
            return {}

    @staticmethod
    def bond_name(qq: str) -> str:
        """情感插件記的名字（10-27 起是 QQ 暱稱），記投餵時順手存一份，查不到就空著"""
        try:
            con = sqlite3.connect(f"file:{plugin_data_dir('astrbot_plugin_lively_bond') / 'bond.db'}?mode=ro", uri=True)
            row = con.execute("select name from people where qq=?", (str(qq),)).fetchone()
            con.close()
            return str(row[0] or "") if row else ""
        except Exception:
            return ""

    @staticmethod
    def bond_find(name: str) -> list[tuple[str, str]]:
        """按名字在情感插件記的人裡找（先找一模一樣的，沒有再找包含的），返回 [(QQ, 名字)]"""
        name = str(name or "").strip()
        if not name:
            return []
        try:
            con = sqlite3.connect(f"file:{plugin_data_dir('astrbot_plugin_lively_bond') / 'bond.db'}?mode=ro", uri=True)
            rows = [(str(q), str(n or "")) for q, n in con.execute("select qq, name from people where name != ''")]
            con.close()
        except Exception:
            return []
        low = name.lower()
        same = [r for r in rows if r[1].lower() == low]
        return same or [r for r in rows if low in r[1].lower()]

    @staticmethod
    def feed_day(when: str, now: float | None = None) -> float | None:
        """「今天」「昨天」「前天」「3天前」「10-26」「10月26日」→ 那天的時間；不寫是現在；認不出是 None"""
        now = now or time.time()
        w = re.sub(r"\s+", "", str(when or ""))
        if w in ("", "今天", "刚才", "刚刚", "今早", "今晚", "早上", "中午", "下午", "晚上"):
            return now
        back = {"昨天": 1, "昨晚": 1, "昨日": 1, "前天": 2, "大前天": 3}.get(w)
        if back is None and (m := re.fullmatch(r"(\d{1,2})天(前|之前)", w)):
            back = int(m.group(1))
        if back is not None:
            return now - back * 86400
        m = re.fullmatch(r"(?:(\d{4})[-/.年])?(\d{1,2})[-/.月](\d{1,2})[日号]?", w)
        if not m:
            return None
        today = datetime.fromtimestamp(now, TZ)
        try:
            d = today.replace(year=int(m.group(1) or today.year), month=int(m.group(2)), day=int(m.group(3)),
                              hour=12, minute=0, second=0, microsecond=0)
        except ValueError:
            return None
        if not m.group(1) and d.timestamp() > now + 86400:  # 沒寫年、日子在後面：是去年的
            d = d.replace(year=d.year - 1)
        return d.timestamp()

    @staticmethod
    def feed_from(r: dict) -> str:
        """這筆是怎麼記下的（〈我〉看的，帶〈主人〉〈我〉）"""
        via = r.get("via")
        if via == "chat":
            if r.get("told_owner"):
                return "〈主人〉跟〈我〉说的"
            if str(r.get("told_by") or "") == str(r.get("qq")):
                return "TA 自己跟〈我〉说的"
            return f"{r.get('told_name') or r.get('told_by') or '有人'}跟〈我〉说的"
        return "〈主人〉在后台记的" if via == "panel" else "〈主人〉记的"

    def _feed_who(self, event: AstrMessageEvent, who: str) -> tuple[str, str, str]:
        """工具裡的「誰」：QQ 號、名字，不寫是正在說話的人。返回 (QQ, 名字, 認不出時的說明)"""
        who = str(who or "").strip()
        sender = str(event.get_sender_id() or "")
        if not who or who in ("我", "TA", "ta", "他", "她"):
            return sender, self.bond_name(sender) or str(event.get_sender_name() or ""), ""
        digits = re.sub(r"\D", "", who)
        if digits and 5 <= len(digits) <= 12 and len(digits) >= len(re.sub(r"\s", "", who)) - 4:
            return digits, self.bond_name(digits), ""
        if who in ("〈主人〉", self.s.owner_call(), "主人") and self.s.owners():
            q = self.s.owners()[0]
            return q, self.bond_name(q), ""
        hits = self.bond_find(who)
        if len(hits) == 1:
            return hits[0][0], hits[0][1], ""
        if not hits:
            return "", "", f"〈我〉认识的人里没有叫「{who}」的，写 TA 的 QQ 号就能记"
        listed = "、".join(f"{n}（QQ {q}）" for q, n in hits[:6])
        return "", "", f"「{who}」对得上好几个人：{listed}，写 QQ 号就能记"

    # ---------- 〈我〉查飯錢（10-27）：主人在群裡問「咱家还剩多少钱」，她看不到餘額，答成「你自己翻微信去」----------
    # 只有主人在的聊天裡有（說話的人是主人；和 comic 的補畫一樣）。數字和面板飯錢頁同一套（ledger）：餘額、今天實際扣了多少、
    # 最近幾天平均一天多少、還夠吃幾天。上次查餘額超過 BILL_FRESH 秒就先再查一次
    @filter.on_llm_request(priority=100)
    async def hide_bill_tool(self, event: AstrMessageEvent, req=None):
        tools = getattr(req, "func_tool", None)
        if not tools or self.s.is_owner(event.get_sender_id()):
            return
        try:
            if tools.get_tool("lively_bill_look"):
                tools.remove_tool("lively_bill_look")
        except Exception:
            pass

    def bill_text(self, led: dict, fresh_err: str = "") -> str:
        """給〈我〉看的飯錢事實（工具回傳）"""
        def hm(ts):
            return datetime.fromtimestamp(ts, TZ).strftime("%H:%M") if ts else "?"
        if led.get("bal") is None:
            return "饭钱查不到：还没查到过 DeepSeek 的余额" + (f"（{fresh_err}）" if fresh_err else "")
        rows = led.get("list") or []
        today = rows[-1] if rows else {}
        yday = rows[-2] if len(rows) > 1 else {}
        lines = [f"这是 DeepSeek 账户的余额，就是〈我〉调模型用的饭钱，不是〈主人〉自己的钱：现在 ¥{led['bal']:.2f}（{hm(led.get('updated'))} 查的"
                 + (f"；刚才想再查一次没查到：{fresh_err}" if fresh_err else "") + "）"]
        if today.get("actual") is not None:
            lines.append(f"今天到现在实际扣了 ¥{today['actual']:.2f}（按余额掉的算，记没记到都在里面）")
        if yday.get("actual") is not None and not yday.get("partial"):
            lines.append(f"昨天一共 ¥{yday['actual']:.2f}")
        if led.get("per_day") and led.get("runway") is not None:
            end = (datetime.now(TZ) + timedelta(days=led["runway"])).strftime("%m-%d")
            lines.append(f"最近几天平均一天 ¥{led['per_day']:.2f}，照这个速度还够吃 {led['runway']:.1f} 天（大约到 {end}）")
        else:
            lines.append("最近还没有完整几天的账，算不出还够吃几天")
        if self.alert_at > 0:
            lines.append(f"余额低于 ¥{self.alert_at:g} 时〈我〉会提醒〈主人〉")
        lines.append("群聊走的降智版不花这笔钱；画图走另一个中转站，也不算在这里")
        v = led.get("voice")
        if v and v.get("bal") is not None:
            vt = (today.get("voice") or [0, 0, 0])
            lines.append(f"发语音用的是另一个账户（BreezeBlue，按点数扣，不是 DeepSeek 的钱）：还剩 {v['bal']:g} 点（{hm(v.get('updated'))} 查的），"
                         f"今天发了 {vt[0]} 条、扣了 {vt[1]:g} 点")
        return self.s.fill("\n".join(lines))

    @filter.llm_tool("lively_bill_look")
    async def tool_bill_look(self, event: AstrMessageEvent):
        """看你的饭钱：DeepSeek 账户的余额（你调模型就花这里的钱）、今天到现在花了多少、照最近几天的速度还够吃几天。
        只有主人在的聊天里、做你自己记下的事时有这个工具。"""
        if not self.s.is_owner(event.get_sender_id()) and not event.get_extra("lively_task_own"):  # 自己記的事：mind tasks.OWN_EXTRA
            return self.s.fill("没查：这个工具只在〈主人〉在的聊天里能用")
        err = ""
        if time.time() - self._last_snap_ts() > BILL_FRESH:
            try:
                await self.fetch()
            except Exception as e:
                err = str(e)[:60]
        try:
            led = await asyncio.to_thread(self.ledger, 4)
        except Exception as e:
            logger.warning(f"[lively_billing] 她查饭钱时算账出错: {e}")
            return f"饭钱查不到：算账时出错了（{str(e)[:60]}）"
        logger.info(f"[lively_billing] 她查了饭钱：余额 {led.get('bal')}，一天 {led.get('per_day')}，还够 {led.get('runway')} 天")
        return self.bill_text(led, err)

    def _last_snap_ts(self) -> float:
        try:
            last = (self.snap_file.read_text("utf-8").rstrip().rsplit("\n", 1) or [""])[-1]
            return float(json.loads(last).get("ts") or 0)
        except Exception:
            return 0.0

    @filter.llm_tool("lively_feed_note")
    async def tool_feed_note(self, event: AstrMessageEvent, amount: str, who: str = "", when: str = "", note: str = ""):
        """在饭钱本上记一笔投喂：有人给你饭钱（投喂、打赏、赞助）。你自己收不了钱，钱是给主人的，这里只是记账。
        记的时候会写上是谁跟你说的；记下来主人在后台看得到，以后跟 TA 聊天、有人提到 TA 时你也会看到这笔。
        每次调用都记一笔；记完会列出饭钱本上 TA 之前的几笔（哪天的、谁说的、什么时候记下的），记重了可以用 lively_feed_drop 划掉。

        Args:
            amount(string): 多少钱，写数字（元）
            who(string): 谁投喂的：QQ 号或名字；就是正在跟你说话的人可以不写
            when(string): 什么时候投喂的：今天、昨天、10-26 这种，不写就是今天
            note(string): 备注，比如 TA 投喂时说的话，可以不写
        """
        qq, name, err = self._feed_who(event, who)
        if err:
            return self.s.fill(f"没记成：{err}")
        try:
            amt = float(re.sub(r"[¥￥元块圆\s,，]", "", str(amount)))
        except ValueError:
            return f"没记成：金额「{amount}」不是数字"
        ts = self.feed_day(when)
        guessed = ts is None
        ts = time.time() if guessed else ts
        teller = str(event.get_sender_id() or "")
        before = [x for x in self.feeds() if str(x.get("qq")) == qq]
        try:
            r = self.add_feed(qq, amt, ts=ts, note=note, name=name, via="chat", told_by=teller,
                              told_name=str(event.get_sender_name() or ""), told_owner=self.s.is_owner(teller))
        except ValueError as e:
            return f"没记成：{e}"
        mine = [x for x in self.feeds() if str(x.get("qq")) == qq]
        day = datetime.fromtimestamp(r["ts"], TZ)
        text = (f"记下了：{r['name'] or qq}（QQ {qq}）{day.month}月{day.day}日投喂 ¥{r['amount']:g}"
                + (f"（{r['note']}）" if r["note"] else "") + f"，{self.feed_from(r)}")
        if guessed:
            text += f"。「{when}」没认出是哪天，按今天记了"
        if before:  # 照實列出 TA 之前的幾筆，是不是同一筆、要不要劃掉由〈我〉看
            text += "。饭钱本上 TA 之前还有：" + "；".join(self.feed_line(x) for x in before[:5])
            if len(before) > 5:
                text += f"；还有更早的 {len(before) - 5} 笔"
        if len(mine) > 1:
            text += f"。现在 TA 一共 {len(mine)} 笔 ¥{sum(float(x['amount']) for x in mine):g}"
        return self.s.fill(text)

    def feed_line(self, r: dict) -> str:
        """一筆投喂的一行字（〈我〉看的）：哪天、多少、備註、怎麼記下的、什麼時候記下的"""
        day = datetime.fromtimestamp(r.get("ts", 0), TZ)
        added = datetime.fromtimestamp(r.get("added") or r.get("ts", 0), TZ)
        return (f"{day.month}月{day.day}日 ¥{float(r.get('amount') or 0):g}" + (f"（{r['note']}）" if r.get("note") else "")
                + f"，{self.feed_from(r)}，{added.month}-{added.day} {added:%H:%M} 记下")

    @filter.llm_tool("lively_feed_drop")
    async def tool_feed_drop(self, event: AstrMessageEvent, who: str = "", amount: str = ""):
        """从饭钱本上划掉一笔你自己在聊天里记错的投喂（主人在后台记的划不掉）。

        Args:
            who(string): 那笔是谁的：QQ 号或名字；就是正在跟你说话的人可以不写
            amount(string): 那笔的金额，写数字；不写就是 TA 最近记的那笔
        """
        qq, _, err = self._feed_who(event, who)
        if err:
            return self.s.fill(f"没划掉：{err}")
        try:
            amt = float(re.sub(r"[¥￥元块圆\s,，]", "", str(amount))) if str(amount or "").strip() else None
        except ValueError:
            return f"没划掉：金额「{amount}」不是数字"
        rows = [r for r in self.feeds() if str(r.get("qq")) == qq and (amt is None or abs(float(r.get("amount") or 0) - amt) < 0.005)]
        if not rows:
            return f"饭钱本上没有 QQ {qq}" + (f" ¥{amt:g}" if amt is not None else "") + " 这笔"
        r = next((x for x in rows if x.get("via") == "chat"), rows[0])  # 同樣金額有好幾筆時劃〈我〉自己記的
        if r.get("via") != "chat":
            return self.s.fill(f"那笔（¥{r['amount']:g}）是{self.feed_from(r)}，〈我〉划不掉；记错了得〈主人〉在后台删")
        self.del_feed(r["id"])
        day = datetime.fromtimestamp(r["ts"], TZ)
        return f"划掉了：{r.get('name') or qq}（QQ {qq}）{day.month}月{day.day}日 ¥{r['amount']:g}"

    def summary(self, days: int = 7) -> dict:
        now = time.time()
        today0 = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        start = (today0 - timedelta(days=days - 1)).timestamp()
        provs = self._providers()
        snaps = self._snaps(start - 2 * EVERY)
        last = snaps[-1] if snaps else None
        accounts = self._accounts(last) if last else [[p["id"]] for p in provs]
        track_since = self._tracking_since()

        def acct_of(pid):
            return next((i for i, a in enumerate(accounts) if pid in a), None)

        # 每個帳戶：按天的真實花費（餘額下降），充值那段用算出來的補
        actual = [dict() for _ in accounts]
        topups = []
        for a, b in zip(snaps, snaps[1:]):
            for i, acct in enumerate(accounts):
                rep = next((p for p in acct if p in a["keys"] and p in b["keys"]), None)
                if rep is None:
                    continue
                drop = a["keys"][rep] - b["keys"][rep]
                if drop < -0.005:
                    est = sum(v for pid, v in self._chat_costs(a["ts"], b["ts"]).items() if pid in acct) + sum(
                        sum(by.values()) for pid, by in self._bg_costs(a["ts"], b["ts"]).items() if pid in acct)
                    topups.append({"ts": b["ts"], "account": i, "amount": round(-drop + est, 2)})
                    drop = est
                if b["ts"] >= start:
                    actual[i][_day(b["ts"])] = actual[i].get(_day(b["ts"]), 0.0) + max(0.0, drop)
        first_ts = snaps[0]["ts"] if snaps else now

        out_days = []
        for i in range(days):
            d0 = today0 + timedelta(days=i - days + 1)
            t0, t1 = d0.timestamp(), (d0 + timedelta(days=1)).timestamp()
            key = d0.strftime("%Y-%m-%d")
            is_today = key == _day(now)
            tracked = first_ts <= t0 + EVERY
            chat = self._chat_costs(t0, min(t1, now))
            bg = self._bg_costs(t0, min(t1, now))
            per_key = {}
            for p in provs:
                c, by = chat.get(p["id"], 0.0), bg.get(p["id"], {})
                per_key[p["id"]] = {"chat": round(c, 2), "background": round(sum(by.values()), 2),
                                    "total": round(c + sum(by.values()), 2),
                                    "by": {CALLER_NAMES.get(w, w): round(v, 3) for w, v in sorted(by.items(), key=lambda x: -x[1])}}
            acts = [round(actual[j][key], 2) if key in actual[j] and (tracked or is_today) else None for j in range(len(accounts))]
            act = round(sum(a for a in acts if a is not None), 2) if any(a is not None for a in acts) else None
            est = round(sum(v["total"] for v in per_key.values()), 2)
            img = self.images(t0, t1)
            vc = self.voice(t0, t1)
            out_days.append({
                "day": key, "actual": act, "accounts_actual": acts, "estimated": est, "keys": per_key,
                "unattributed": round(max(0.0, act - est), 2) if act is not None and track_since and track_since <= t0 else None,
                "images": img, "image_cost": round(img * self.image_price, 2), "partial": is_today or not tracked,
                "voice_n": vc["n"], "voice_credits": vc["credits"], "voice_cost": round(vc["cost"], 2),
                # 兼容舊面板
                "chat": round(sum(v["chat"] for v in per_key.values()), 2),
                "background": round(max(0.0, act - sum(v["chat"] for v in per_key.values())), 2) if act is not None else None,
            })

        acct_out = []
        for i, acct in enumerate(accounts):
            bal = next((last["keys"][p] for p in acct if last and p in last["keys"]), None)
            full = [d["accounts_actual"][i] for d in out_days[:-1] if d["accounts_actual"][i] is not None and not d["partial"]][-3:]
            if full:
                rate = sum(full) / len(full)
            else:  # 還沒有完整的一天：用今天到現在的速度，或者算出來的花費 × 1.5
                today = out_days[-1]
                hours = max(1.0, (now - max(today0.timestamp(), first_ts)) / 3600)
                frac = max(0.1, (now - today0.timestamp()) / 86400)
                a = today["accounts_actual"][i]
                rate = a / hours * 24 if a and hours >= 3 else sum(today["keys"][p]["total"] for p in acct if p in today["keys"]) * 1.5 / frac
            acct_out.append({"keys": acct, "balance": bal, "per_day": round(rate, 2),
                             "runway_days": round(bal / rate, 1) if bal is not None and rate > 0 else None,
                             "low": bal is not None and self.alert_at > 0 and bal < self.alert_at})
        keys_out = [{"id": p["id"], "label": p["label"], "uses": p["uses"], "account": acct_of(p["id"])} for p in provs]
        first = acct_out[0] if acct_out else {}
        return {
            "updated": last["ts"] if last else None, "accounts": acct_out, "keys": keys_out,
            "tracking_since": track_since, "alert_at": self.alert_at, "image_price": self.image_price,
            "days": out_days, "topups": topups[-5:],
            # 兼容舊面板：只有一個帳戶時就是它
            "balance": round(sum(a["balance"] for a in acct_out if a["balance"] is not None), 2) if acct_out else None,
            "per_day": round(sum(a["per_day"] for a in acct_out), 2),
            "runway_days": first.get("runway_days") if len(acct_out) == 1 else min(
                (a["runway_days"] for a in acct_out if a["runway_days"] is not None), default=None),
        }

    def _write_summary(self) -> dict:
        s = self.summary()
        try:
            self.summary_file.write_text(json.dumps(s, ensure_ascii=False), "utf-8")
        except Exception as e:
            logger.warning(f"[lively_billing] 写汇总失败: {e}")
        return s

    # ---------- 提醒 ----------
    async def _check_alert(self, s: dict):
        if self.alert_at <= 0:
            return
        labels = {k["id"]: k["label"] for k in s["keys"]}
        for acct in s["accounts"]:
            total, aid = acct["balance"], ",".join(acct["keys"])
            if total is None:
                continue
            alerted = self.state["alerted"].get(aid, 0.0)
            if total >= self.alert_at:
                if alerted:
                    self.state["alerted"].pop(aid, None)
                    self._save()
                continue
            level = self.alert_at if total >= self.alert_at / 2 else self.alert_at / 2
            if alerted and alerted <= level:
                continue
            who = "、".join(labels.get(p, p) for p in acct["keys"])
            runway = f"，照最近一天 ¥{acct['per_day']:.1f} 的速度大概还能撑 {acct['runway_days']:.1f} 天" if acct.get("runway_days") else ""
            if not self.s.owner():
                return
            # 插件不寫冒充她口吻的句子（10-26 用戶定）：事實交給她寫一句，後面一行【后台】餘額
            facts = f"DeepSeek 的饭钱（{who}用的这个账户）只剩 ¥{total:.2f} 了{runway}；提醒线是 ¥{self.alert_at:g}，要〈主人〉去充值"
            card = f"饭钱快没了：DeepSeek 余额 ¥{total:.2f}（{who}用的这个账户）{runway}"
            self.state["alerted"][aid] = level
            self._save()
            try:
                said = await tell_owner(self.s, facts, "饭钱快没了", must=True, card=card, kind="饭钱提醒")
                logger.info(f"[lively_billing] 余额 ¥{total:.2f}，已提醒主人" + (f"：{said[:60]}" if said else "（只发了后台那行）"))
            except Exception as e:
                logger.error(f"[lively_billing] 提醒主人失败: {e}")

    async def _loop(self):
        await asyncio.sleep(60)
        pruned = ""
        while True:
            try:
                if pruned != _day(time.time()):
                    pruned = _day(time.time())
                    self._prune_usage()
                try:
                    await asyncio.to_thread(self._prune_dump)
                except Exception as e:
                    logger.warning(f"[lively_billing] 清理提示词留档失败: {e}")
                await self.fetch()
                try:
                    await self.fetch_voice()
                except Exception as e:
                    logger.warning(f"[lively_billing] 查 BreezeBlue 失败: {e}")
                await self._check_alert(self._write_summary())
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[lively_billing] 查余额失败: {e}")
            # 對齊到整點，賬按小時算比較整齊
            await asyncio.sleep(EVERY - time.time() % EVERY + 5)

    # ---------- 主人指令 ----------
    def report_text(self, s: dict) -> str:
        t, y = s["days"][-1], s["days"][-2]
        labels = {k["id"]: k["label"] for k in s["keys"]}
        lines = []
        for a in s["accounts"]:
            shared = f"（{len(a['keys'])} 把密钥共用这个账户）" if len(a["keys"]) > 1 else f"（{labels.get(a['keys'][0], a['keys'][0])}）"
            lines.append(f"DeepSeek 余额 ¥{a['balance']:.2f}{shared}" if a["balance"] is not None else f"{shared} 余额查不到")
        if t["actual"] is not None:
            lines.append(f"今天到现在实际扣了 ¥{t['actual']:.2f}")
        lines.append("每把密钥今天花的（按 token 算）：")
        for k in s["keys"]:
            v = t["keys"].get(k["id"], {})
            by = "，".join(f"{w} ¥{c:.2f}" for w, c in v.get("by", {}).items() if c >= 0.005)
            lines.append(f"· {k['label']}：¥{v.get('total', 0):.2f}（聊天 ¥{v.get('chat', 0):.2f}"
                         + (f"，{by}" if by else "") + "）")
        if t.get("unattributed"):
            lines.append(f"· 没记到的：¥{t['unattributed']:.2f}")
        if y["actual"] is not None and not y["partial"]:
            lines.append(f"昨天一共 ¥{y['actual']:.2f}")
        lines.append(f"画图今天 {t['images']} 张，约 ¥{t['image_cost']:.2f}（估的）")
        if t.get("voice_n") or t.get("voice_credits"):
            lines.append(f"语音今天 {t['voice_n']} 条，BreezeBlue 扣了 {t['voice_credits']:g} 点，约 ¥{t['voice_cost']:.2f}（估的，以 BreezeBlue 账户页为准）")
        for a in s["accounts"]:
            if a.get("runway_days"):
                lines.append(f"照一天 ¥{a['per_day']:.1f} 的速度，还能撑 {a['runway_days']:.1f} 天；低于 ¥{self.alert_at:g} 〈我〉会提醒〈主人〉")
        return "\n".join(lines)

    def dump_text(self, st: dict) -> str:
        def hm(ts):
            return datetime.fromtimestamp(ts, TZ).strftime("%m-%d %H:%M")
        who = "、".join("〈主人〉" if self.s.is_owner(q) else q for q in st["qq"]) or "（没人）"
        lines = [f"提示词留档：{'开着' if st['on'] else '关着'}"]
        if st["on"]:
            lines.append(f"从 {hm(st['since'])} 开始，" + (f"{hm(st['until'])} 自动关" if st["until"] else "不会自动关"))
        lines.append(f"存的是 {who} 私聊的每次模型请求和压缩历史那次" + ("，还有心跳" if st["heartbeat"] else ""))
        lines.append(f"今天存了 {st['today']} 笔，一共占 {st['bytes'] / 1048576:.1f} MB（留最近 {st['days']} 天）")
        return "\n".join(lines)

    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE, priority=1330)
    async def owner(self, event: AstrMessageEvent):
        if not self.s.is_owner(event.get_sender_id()):
            return
        msg = (event.message_str or "").strip().lstrip("/").strip()
        head, _, rest = msg.partition(" ")
        if head in ("饭钱", "飯錢") and rest.strip().split(" ")[0] in ("留档", "留檔"):
            word = rest.strip().partition(" ")[2].strip()
            if word not in DUMP_WORDS:
                return
            event.stop_event()
            act = DUMP_WORDS[word]
            if act != "status":
                self.dump_switch(act == "on")
                logger.info(f"[lively_billing] 主人把提示词留档{'打开' if act == 'on' else '关掉'}了")
            await event.send(MessageChain().message(self.s.admin_text(self.dump_text(self.dump_status()))))
            return
        if msg not in ("饭钱", "余额", "飯錢", "餘額"):
            return
        event.stop_event()
        try:
            await self.fetch()
            s = self._write_summary()
        except Exception as e:
            await event.send(MessageChain().message(self.s.admin_text(f"查不到余额：{e}")))
            return
        await event.send(MessageChain().message(self.s.admin_text(self.report_text(s))))

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
- 每個帳戶餘額低於 alert_at 私信主人提醒充錢，低於一半再提醒一次；充回去之後重新計
- 每小時把彙總寫進 summary.json，面板和日報讀它
主人私信指令：飯錢 / 餘額
"""
import asyncio
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.message.message_event_result import MessageChain

from .lively_common import CONFIG_DIR, DATA, GATE_PLUGIN, Settings, plugin_data_dir

TZ = timezone(timedelta(hours=8))
CMD_CONFIG = DATA / "cmd_config.json"
STATS_DB = DATA / "data_v4.db"
BALANCE_URL = "https://api.deepseek.com/user/balance"
EVERY = 3600
WRAP_EVERY = 300  # 模型重載後是新實例，隔一會兒檢查一遍有沒有沒套上的
KEEP_USAGE_DAYS = 62
CALLER_NAMES = {
    "chat": "聊天回复", "astrbot": "AstrBot 自带", "livingmemory": "记忆插件",
    "lively_bond": "人物卡", "lively_evolve": "自我成长", "lively_memtier": "记忆分层",
    "lively_report": "日报", "lively_qzone": "说说", "lively_proactive": "主动聊天",
    "qq_group_daily_analysis": "群分析/群漫画",
}


def _peak(ts: float) -> bool:
    d = datetime.fromtimestamp(ts, TZ)
    hm = d.strftime("%H:%M")
    return d.weekday() < 5 and ("09:00" <= hm < "12:00" or "14:00" <= hm < "18:00")


def _day(ts: float) -> str:
    return datetime.fromtimestamp(ts, TZ).strftime("%Y-%m-%d")


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


@register(
    "astrbot_plugin_lively_billing",
    "GUCine",
    "DeepSeek 餘額每小時記帳：每天真實花費、聊天/後臺/畫圖分項、餘額不足私信主人",
    "2.0.0",
)
class LivelyBilling(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.s = Settings(context, config)
        self.alert_at = float(self.s.get("alert_at", 10.0))
        self.image_price = float(self.s.get("image_price", 0.3))
        # 元/百萬 tokens：未命中快取的輸入、命中快取的輸入、輸出；DeepSeek 尖峰時段（工作日 9-12、14-18 點）翻倍
        self.prices = (float(self.s.get("price_input_miss", 1.0)), float(self.s.get("price_input_hit", 0.02)),
                       float(self.s.get("price_output", 4.0)))
        self.peak_double = bool(self.s.get("peak_double", True))
        self.dir = Path(StarTools.get_data_dir("astrbot_plugin_lively_billing"))
        self.snap_file = self.dir / "balance.jsonl"
        self.usage_file = self.dir / "usage.jsonl"
        self.state_file = self.dir / "state.json"
        self.summary_file = self.dir / "summary.json"
        try:
            self.state = json.loads(self.state_file.read_text("utf-8"))
        except Exception:
            self.state = {}
        if not isinstance(self.state.get("alerted"), dict):
            self.state["alerted"] = {}  # 帳戶 -> 已經按哪個門檻提醒過
        self._tasks = [asyncio.create_task(self._loop())]
        if bool(self.s.get("track_all_calls", True)):
            self._tasks.append(asyncio.create_task(self._wrap_loop()))

    async def terminate(self):
        for t in self._tasks:
            t.cancel()

    def _save(self):
        try:
            self.state_file.write_text(json.dumps(self.state, ensure_ascii=False), "utf-8")
        except Exception as e:
            logger.warning(f"[lively_billing] 保存状态失败: {e}")

    def _cost(self, ts: float, miss: int, hit: int, out: int) -> float:
        p_miss, p_hit, p_out = self.prices
        k = 2 if self.peak_double and _peak(ts) else 1
        return ((miss or 0) * p_miss + (hit or 0) * p_hit + (out or 0) * p_out) * k / 1e6

    # ---------- 記下每次模型呼叫 ----------
    def _record(self, pid: str, usage, who: str):
        if who == "chat" or usage is None:  # 聊天回覆 AstrBot 自己記在 provider_stats 裡
            return
        try:
            row = {"ts": int(time.time()), "pid": pid, "who": who, "miss": int(getattr(usage, "input_other", 0) or 0),
                   "hit": int(getattr(usage, "input_cached", 0) or 0), "out": int(getattr(usage, "output", 0) or 0)}
            with self.usage_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.debug(f"[lively_billing] 记用量失败: {e}")

    def _wrap(self, prov):
        pid = str(prov.provider_config.get("id", ""))
        orig = prov.text_chat
        if getattr(orig, "_lively_billing", False):
            return False
        bill = self

        # 普通函數而不是 async：呼叫的那一刻就看呼叫棧（之後可能被放進別的 task 裡跑，就看不出是誰調的了）
        def text_chat(*args, **kwargs):
            return bill._run(orig(*args, **kwargs), pid, _caller())

        text_chat._lively_billing = True
        prov.text_chat = text_chat
        orig_stream = getattr(prov, "text_chat_stream", None)
        if orig_stream is not None and not getattr(orig_stream, "_lively_billing", False):
            def text_chat_stream(*args, **kwargs):
                return bill._run_stream(orig_stream(*args, **kwargs), pid, _caller())

            text_chat_stream._lively_billing = True
            prov.text_chat_stream = text_chat_stream
        return True

    async def _run(self, coro, pid: str, who: str):
        resp = await coro
        self._record(pid, getattr(resp, "usage", None), who)
        return resp

    async def _run_stream(self, gen, pid: str, who: str):
        last = None
        async for chunk in gen:
            if getattr(chunk, "usage", None) is not None:
                last = chunk.usage
            yield chunk
        self._record(pid, last, who)

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
        uses = self._uses()
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
            out[pid or "?"] = out.get(pid or "?", 0.0) + self._cost(ts, m, h, o)
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
                    by[u["who"]] = by.get(u["who"], 0.0) + self._cost(u["ts"], u["miss"], u["hit"], u["out"])
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
        try:
            lines = self.usage_file.read_text("utf-8").splitlines()
            keep = [x for x in lines if json.loads(x).get("ts", 0) >= cutoff]
            if len(keep) < len(lines):
                tmp = self.usage_file.with_suffix(".tmp")
                tmp.write_text("\n".join(keep) + ("\n" if keep else ""), "utf-8")
                tmp.replace(self.usage_file)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"[lively_billing] 清理旧用量失败: {e}")

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
            out_days.append({
                "day": key, "actual": act, "accounts_actual": acts, "estimated": est, "keys": per_key,
                "unattributed": round(max(0.0, act - est), 2) if act is not None and track_since and track_since <= t0 else None,
                "images": img, "image_cost": round(img * self.image_price, 2), "partial": is_today or not tracked,
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
            text = f"〈主人〉，DeepSeek 的饭钱只剩 ¥{total:.2f} 了（{who}用的这个账户）{runway}，记得给〈我〉充饭钱呀～"
            owner = self.s.owner()
            if not owner:
                return
            try:
                await self.context.send_message(self.s.private_umo(owner), MessageChain().message(self.s.owner_text(text)))
                self.state["alerted"][aid] = level
                self._save()
                logger.info(f"[lively_billing] 余额 ¥{total:.2f}，已提醒主人")
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
                await self.fetch()
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
        for a in s["accounts"]:
            if a.get("runway_days"):
                lines.append(f"照一天 ¥{a['per_day']:.1f} 的速度，还能撑 {a['runway_days']:.1f} 天；低于 ¥{self.alert_at:g} 〈我〉会提醒〈主人〉")
        return "\n".join(lines)

    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE, priority=1330)
    async def owner(self, event: AstrMessageEvent):
        if not self.s.is_owner(event.get_sender_id()):
            return
        if (event.message_str or "").strip() not in ("饭钱", "余额", "飯錢", "餘額"):
            return
        event.stop_event()
        try:
            await self.fetch()
            s = self._write_summary()
        except Exception as e:
            await event.send(MessageChain().message(self.s.owner_text(f"查不到余额：{e}")))
            return
        await event.send(MessageChain().message(self.s.owner_text(self.report_text(s))))

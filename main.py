"""
雀魂大会室管理插件 (AstrBot, 纯 Python REST 版)
================================================
通过纯 Python 调用雀魂大会室 REST API（contest-gate-202411.maj-soul.com），
实现群内管理雀魂赛事功能。无需 Node.js / mjsoul / WebSocket。

架构:
    QQ群 → AstrBot 插件 (main.py + mjsoul_client.py) → REST API

指令 (以 % 前缀):
    %绑定 赛事id   绑定赛事（自动获取 unique_id）
    %解绑          解绑
    %规则          查看赛事信息
    %名单          查看参赛名单
    %公告          查看公告
    %大厅          查看对局中/准备中
    %待机          查看准备中玩家
    %排名          查看对局记录
    %添加 id1,id2  添加选手
    %删除 id1,id2  删除选手
    %开赛 [...]    开赛（支持ID/昵称、标签、固定座位、指定点数）
    %暂停 [uuid] / %恢复 [uuid] / %终止 [uuid]  对局控制
    %帮助          帮助
    %查看配置 / %配置 / %重置配置  管理雀魂账号

对局播报（默认开启，绑定后自动推送，风格同 mjdhs）:
    对局开始: 名字1,名字2,电脑,电脑
    对局结束: /?paipu=游戏编号
             H:mm:ss - H:mm:ss
             名字 点数
"""
import asyncio
import json
import threading
import time
from pathlib import Path

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.message.components import Plain

from .mjsoul_client import ApiError, MjsoulClient

PLUGIN_DIR = Path(__file__).resolve().parent
CONFIG_FILE = PLUGIN_DIR / "mjsoul_config.json"  # 雀魂账号配置
DB_FILE = PLUGIN_DIR / "db.json"  # QQ群 ↔ 赛事绑定
BCAST_FILE = PLUGIN_DIR / "bcast_state.json"  # 播报状态(进行中对局/已播报牌谱)，防重启重复播报

# 默认配置仅为占位；真实凭据请在「插件配置页」填写，或通过 %配置 指令修改。
# 配置优先从 AstrBot 插件配置页(_conf_schema.json)读取；首次升级时会自动从旧的
# mjsoul_config.json 迁移账号/密码/白名单（该文件仍向后兼容）。
DEFAULT_CONFIG = {
    "account": "",
    "password": "",
    # 可修改本插件的管理员白名单。
    #   aiocqhttp(OneBot/Go-CQ) 填 QQ 号；qq_official / qq_official_webhook 只能填 openid（拿不到QQ号）。
    # 空列表表示"不单独限制"，此时回到原有逻辑（QQ群主/群管理 / AstrBot全局 admins_id）。
    "admin_ids": [],
    # "any"   = 有任一人即算管理员（群管理 或 AstrBot admins_id 或 上面白名单）
    # "strict" = 只认 admin_ids 白名单；空列表则任何人都不算管理员（可先用白名单再改回来）
    "admin_mode": "any",
    # 对局播报开关与轮询间隔（绑定赛事的群自动推送对局开始/结束，风格同 mjdhs）
    "broadcast_enabled": True,
    "poll_interval": 10,
}

ALL_CMDS = {"帮助", "绑定", "解绑", "规则", "名单", "公告", "大厅", "待机",
            "排名", "查看配置", "配置", "重置配置", "添加", "删除", "开赛", "暂停", "恢复", "终止"}


def _pick_text(v, lang="zh"):
    """从雀魂接口的多语言字段中提取指定语言文本。
    兼容: str / {"content":...} / [{"lang":"zh","content":...}] / {"zh":...} / None"""
    if not v:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        # 形如 {"zh": "...", "ja": "..."} 或 {"content": "..."}
        if lang in v and v[lang]:
            return _pick_text(v[lang])
        for key in ("content", "text", "zh", "zh-CN", "zh_CN", "ja", "en"):
            if v.get(key):
                return _pick_text(v[key])
        return ""
    if isinstance(v, list):
        # 形如 [{"lang":"zh","content":"..."}, {"lang":"ja","content":"..."}]
        for item in v:
            if isinstance(item, dict) and item.get("lang") == lang:
                t = _pick_text(item.get("content") or item.get("text"))
                if t:
                    return t
        for item in v:
            t = _pick_text(item)
            if t:
                return t
    return ""


@register("astrbot_plugin_mjdhs", "SR3MP", "雀魂大会室管理", "1.5.2")
class MjdhsPlugin(Star):
    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self.config = config
        self._client = None
        self._lock = threading.Lock()
        # 是否使用 AstrBot 插件配置页（_conf_schema.json）作为配置来源
        self._config_is_astrbot = isinstance(config, dict)
        self._load_config()
        self._load_db()
        # ---- 对局开始/结束播报（风格同 mjdhs） ----
        self._uid_cache = {}        # contest_id -> unique_id 缓存，避免每次轮询都拉赛事列表
        self._bcast_state = {}      # str(cid) -> {running:[], ended:[], seeded:bool}
        self._bcast_stop = threading.Event()
        self._bcast_thread = None
        self._main_loop = None
        try:
            self._main_loop = asyncio.get_event_loop()
        except Exception:
            pass
        self._load_bcast()
        self._ensure_bcast()

    # ---------- 配置/数据库 ----------
    def _load_config(self):
        """加载配置：优先用 AstrBot 插件配置页；缺失时回退旧的 mjsoul_config.json。"""
        cfg = self.config
        if not isinstance(cfg, dict):
            # 没有插件配置 schema（或未传入）→ 走旧的 mjsoul_config.json
            cfg = dict(DEFAULT_CONFIG)
            if CONFIG_FILE.exists():
                try:
                    cfg.update(json.loads(CONFIG_FILE.read_text(encoding="utf-8")))
                except Exception:
                    pass
            self.account_cfg = cfg
            self._config_is_astrbot = False
        else:
            self._config_is_astrbot = True
            # 一次性迁移：插件配置页账号为空但旧文件有账号时，把旧值搬过来
            if not cfg.get("account") and CONFIG_FILE.exists():
                try:
                    legacy = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
                    if legacy.get("account"):
                        for k in ("account", "password", "admin_ids", "admin_mode"):
                            if k in legacy:
                                cfg[k] = legacy[k]
                        self._save_config()
                        logger.info("[mjdhs] 已从 mjsoul_config.json 迁移配置到插件配置页")
                except Exception:
                    pass
            self.account_cfg = cfg

        # 归一化（兼容旧配置文件中缺字段/类型不对的情况）
        ids = self.account_cfg.get("admin_ids") or []
        self.account_cfg["admin_ids"] = [str(x).strip() for x in ids if str(x).strip()]
        if self.account_cfg.get("admin_mode") not in ("any", "strict"):
            self.account_cfg["admin_mode"] = "any"
        if not isinstance(self.account_cfg.get("broadcast_enabled"), bool):
            self.account_cfg["broadcast_enabled"] = DEFAULT_CONFIG["broadcast_enabled"]
        try:
            iv = int(self.account_cfg.get("poll_interval", 10))
        except (TypeError, ValueError):
            iv = 10
        self.account_cfg["poll_interval"] = max(iv, 3)

    def _admin_ids(self) -> list:
        return list(self.account_cfg.get("admin_ids") or [])

    def _save_config(self):
        """保存配置：写入 AstrBot 插件配置页；不可用时回退 mjsoul_config.json。"""
        if self._config_is_astrbot and isinstance(self.config, dict):
            save = getattr(self.config, "save_config", None)
            if callable(save):
                try:
                    save()
                    return
                except Exception as e:
                    logger.warning(f"[mjdhs] 保存插件配置失败，回退到 mjsoul_config.json: {e}")
        CONFIG_FILE.write_text(json.dumps(self.account_cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_db(self):
        self.db = {}
        if DB_FILE.exists():
            try:
                self.db = json.loads(DB_FILE.read_text(encoding="utf-8"))
            except Exception:
                self.db = {}
        # 迁移旧格式: db[gid] = cid(int) -> {"cid": cid, "session": ""}
        for gid, v in list(self.db.items()):
            if isinstance(v, int) or isinstance(v, str):
                self.db[gid] = {"cid": int(v), "session": ""}
                self._save_db()

    def _save_db(self):
        DB_FILE.write_text(json.dumps(self.db, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _entry_cid(ent) -> int:
        try:
            if isinstance(ent, dict):
                return int(ent.get("cid") or 0)
            return int(ent or 0)
        except (TypeError, ValueError):
            return 0

    # ---------- 雀魂客户端 ----------
    def _get_client(self):
        """获取（必要时新建）已登录的雀魂客户端"""
        if self._client is None:
            self._client = MjsoulClient(self.account_cfg.get("account", ""),
                                        self.account_cfg.get("password", ""))
            self._client.login()
            logger.info("[mjdhs] 雀魂登录成功")
        return self._client

    def _resolve_unique_id(self, contest_id) -> int:
        """把 contest_id 转 unique_id（直接从赛事列表取）"""
        client = self._get_client()
        try:
            contests = client.fetch_contest_list()
            for c in contests:
                if c.get("contest_id") == int(contest_id):
                    return c.get("unique_id") or None
        except Exception:
            pass
        return None

    # ---------- 指令 ----------
    @filter.command("helpmjdhs", alias={"mjdhs帮助", "雀魂帮助", "dhs帮助"})
    async def cmd_help(self, event: AstrMessageEvent):
        yield event.plain_result(self._get_help())

    def _get_help(self) -> str:
        return """-----大会室指令说明-----
● 查询类指令(%为前缀)
%绑定 赛事id / %解绑
%规则 / %名单 / %公告
%大厅 / %待机 / %排名
● 系统类指令
%帮助 / %查看配置 / %配置 账号 xxx 密码 xxx
%配置 管理 123 456(可修改的人, 可用 清空 移除)
%配置 模式 any/strict (strict=只认白名单)"""

    @filter.regex(r"^%")
    async def on_message(self, event: AstrMessageEvent):
        message_str = getattr(event, "message_str", "") or ""
        if not message_str:
            return
        message = message_str.strip()

        if message.startswith("%"):
            body = message[1:].strip()
        elif message[:3].lower() == "dhs":
            body = message[3:].strip()
        else:
            return

        # 匹配指令（长指令优先）
        cmd = ""
        param = body
        for c in sorted(ALL_CMDS, key=len, reverse=True):
            if body.startswith(c):
                cmd = c
                param = body[len(c):].strip()
                break
        if not cmd:
            return

        # 帮助
        if cmd == "帮助":
            yield event.plain_result(self._get_help())
            return

        gid = event.get_group_id() or event.session_id
        is_admin = self._is_admin(event)

        # 配置类指令（任何群管理员）
        if cmd == "查看配置":
            if not is_admin:
                yield event.plain_result("需要管理员权限")
                return
            yield event.plain_result(self._format_config())
            return
        if cmd == "配置":
            if not is_admin:
                yield event.plain_result("需要管理员权限")
                return
            yield event.plain_result(self._do_config(param))
            return
        if cmd == "重置配置":
            if not is_admin:
                yield event.plain_result("需要管理员权限")
                return
            self.account_cfg = dict(DEFAULT_CONFIG)
            self._client = None
            self._save_config()
            yield event.plain_result("✅ 配置已重置")
            return

        # 绑定类（需要管理员权限）
        if cmd == "绑定":
            if not is_admin:
                yield event.plain_result("❌ 绑定需要管理员权限")
                return
            yield event.plain_result(self._do_bind(gid, param, str(event.session)))
            return
        if cmd == "解绑":
            if not is_admin:
                yield event.plain_result("❌ 解绑需要管理员权限")
                return
            self.db.pop(gid, None)
            self._save_db()
            yield event.plain_result("✅ 已解绑")
            return

        # 写操作（添加/删除选手、开赛、对局控制）需管理员权限
        if cmd in {"添加", "删除", "开赛", "暂停", "恢复", "终止"} and not is_admin:
            yield event.plain_result("❌ 该指令需要管理员权限")
            return

        # 需要绑定的查询类
        ent = self.db.get(gid)
        cid = self._entry_cid(ent)
        if not cid:
            yield event.plain_result("尚未绑定赛事。请先使用 %绑定 赛事id")
            return
        # 自我修复：老绑定记录缺 session 时顺手补录（对局播报需要推送地址）
        if isinstance(ent, dict) and not ent.get("session"):
            ent["session"] = str(event.session)
            self._save_db()
        yield event.plain_result(self._do_query(cmd, cid, param))

    def _is_admin(self, event) -> bool:
        mode = self.account_cfg.get("admin_mode", "any")
        admins = self._admin_ids()

        # 1) 配置白名单（最高优先级）。aiocqhttp 填 QQ 号；qq_official/qq_official_webhook 填 openid
        sid = str(event.get_sender_id() or "")
        if sid and sid in admins:
            return True

        if mode == "strict":
            return False  # 严格模式只认白名单

        # 2) OneBot v11 (aiocqhttp)：原始事件里有群角色 sender.role (owner/admin/member)
        if event.get_platform_name() == "aiocqhttp":
            raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
            if isinstance(raw, dict):
                role = (raw.get("sender") or {}).get("role", "")
                if role in ("owner", "admin"):
                    return True
        # 3) 兜底：AstrBot 全局配置 admins_id。qq_official 事件里没有群角色，只能靠它/白名单
        return event.is_admin()

    # ---------- 业务 ----------
    def _do_bind(self, gid: str, param: str, session_str: str = "") -> str:
        if not param.isdigit():
            return "请输入正确的赛事id（数字）"
        cid = int(param)
        try:
            client = self._get_client()
            contests = client.fetch_contest_list()
            for c in contests:
                if c.get("contest_id") == cid:
                    self.db[gid] = {"cid": cid, "session": session_str}
                    self._save_db()
                    self._uid_cache[cid] = c.get("unique_id")
                    self._ensure_bcast()
                    name = _pick_text(c.get("contest_name")) or "?"
                    return f"\u2705 绑定成功: {cid} {name}\n已开启对局播报（风格同 mjdhs）"
            return f"❌ 未找到赛事 {cid}（请确认该账号有管理权限）"
        except ApiError as e:
            return f"❌ 绑定失败: {e}"
        except Exception as e:
            return f"❌ 绑定异常: {e}"

    def _do_query(self, cmd: str, cid: int, param: str = "") -> str:
        try:
            client = self._get_client()
            uid = self._resolve_unique_id(cid)
            if not uid:
                return f"❌ 无法解析赛事 {cid} 的 unique_id"
            season = self._get_active_season(uid)

            if cmd == "名单":
                players = client.fetch_season_player_list(uid, season)
                res = f"[参赛名单({len(players)}人)]\n"
                res += ",".join(p.get("nickname", "?") for p in players)
                return res
            if cmd == "公告":
                notices = client.fetch_announcement(uid)
                res = "[公告]\n"
                if not notices:
                    res += "(无)"
                else:
                    for n in notices:
                        if isinstance(n, dict):
                            title = _pick_text(n.get("title"))
                            body = _pick_text(n.get("content")) or _pick_text(n.get("content_list"))
                            res += ((title + "\n") if title else "") + (body or "") + "\n"
                        else:
                            res += str(n) + "\n"
                return res
            if cmd == "大厅":
                ready = client.fetch_ready_player_list(uid, season)
                running = client.fetch_running_game_list(uid, season)
                res = f"[对局中({len(running)}局)]\n"
                res += "(无)" if not running else "\n".join(str(g.get("game_uuid", "?")) for g in running)
                res += f"\n[准备中({len(ready)}人)]\n"
                res += "(无)" if not ready else ",".join(p.get("nickname", "?") for p in ready)
                return res
            if cmd == "待机":
                ready = client.fetch_ready_player_list(uid, season)
                res = f"[准备中({len(ready)}人)]\n"
                res += "(无)" if not ready else ",".join(p.get("nickname", "?") for p in ready)
                return res
            if cmd == "排名":
                records = client.fetch_game_records(uid, season, limit=50)
                res = f"[对局记录({len(records)}条)]\n"
                if not records:
                    res += "(无)"
                else:
                    for i, r in enumerate(records[:10], 1):
                        accs = r.get("accounts", [])
                        if accs:
                            nicks = ",".join(a.get("nickname", "?") for a in accs)
                        else:
                            nicks = r.get("uuid", "?")[:16]
                        res += f"{i}. {nicks}\n"
                return res
            if cmd == "规则":
                return self._format_rules(cid)
            if cmd == "添加":
                if not param:
                    return "请加上选手ID, 半角逗号分隔\n例: %添加 144687,13286280"
                return self._do_add(client, uid, season, param)
            if cmd == "删除":
                if not param:
                    return "请加上选手ID, 半角逗号分隔\n例: %删除 144687"
                return self._do_remove(client, uid, season, param)
            if cmd == "开赛":
                return self._do_start(client, uid, season, param)
            if cmd in ("暂停", "恢复", "终止"):
                return self._do_game_control(client, uid, cmd, param)
            return "暂未支持该指令"
        except ApiError as e:
            return f"❌ 查询失败: {e}"
        except Exception as e:
            return f"❌ 异常: {e}"

    def _get_active_season(self, uid) -> int:
        try:
            seasons = self._client.fetch_contest_season_list(uid)
            if not seasons:
                return 0
            # 优先返回当前进行中的赛季(state==2)，否则取最新创建的赛季
            for s in seasons:
                if s.get("state") == 2:
                    return s.get("season_id", 0)
            return seasons[-1].get("season_id", 0)
        except Exception:
            pass
        return 0

    # ---------- 选手添加/删除 ----------
    def _do_add(self, client, uid: int, season: int, param: str) -> str:
        # 添加选手: %添加 id1,id2
        ids = [x.strip() for x in param.replace("\n", ",").split(",") if x.strip()]
        if not ids:
            return "未识别到选手ID"
        accounts = client.search_accounts(ids)
        found = {str(a.get("account_id")): a.get("nickname", "?") for a in accounts}
        missing = [i for i in ids if i not in found]
        if missing:
            return "以下ID无效或不存在: " + ",".join(missing)
        account_list = [{"account_id": int(i), "nickname": found[i]} for i in ids]
        result = client.add_contest_season_player(uid, season, account_list)
        success = result.get("success", [])
        failed = result.get("failed", [])
        parts = []
        if success:
            names = [found.get(str(s), str(s)) for s in success]
            parts.append("成功添加: " + ",".join(names))
        if failed:
            fnames = [(f.get("account_id"), f.get("error", "")) for f in failed]
            parts.append("失败: " + "; ".join(str(fid) + "(" + str(err) + ")" for fid, err in fnames))
        return "\n".join(parts) if parts else "无结果"
    def _format_rules(self, cid) -> str:
        """把 game_mode.detail_rule 的原始开关翻译成可读规则说明"""
        client = self._get_client()
        cid = int(cid)
        uid = self._resolve_unique_id(cid)
        if not uid:
            return f"❌ 无法解析赛事 {cid} 的 unique_id"
        detail = client.fetch_contest_detail(uid)
        name = detail.get("contest_name") or []
        nm = name[0].get("content", "?") if name else "?"
        gm = detail.get("game_mode") or {}
        r = gm.get("detail_rule") or {}

        def val(k, d):
            v = r.get(k, d)
            return str(v) if v not in (None, "") else str(d)

        L = [f"📋 {cid} {nm} — 规则说明"]

        # 基础参数
        L.append("")
        L.append("【基础参数】")
        # 基础参数：字段名取自官网大会室 detail_rule 真实默认对象
        # (init_point=初始点数; fandian=返点; jingsuanyuandian=精算原点;
        #  dora_count=宝牌数量; time_fixed/time_add=每巡思考时间+长考时间;
        #  liqibang_value=立直供托点数; changbang_value=场棒点数;
        #  shiduan=食断; noting_fafu_*=顺位得分; shunweima_*=顺位马)
        L.append(f"・初始点数：{val('init_point','25000')}　返点：{val('fandian','30000')}")
        L.append(f"・精算原点：{val('jingsuanyuandian','25000')}　宝牌数量：{val('dora_count','3')} 枚")
        L.append(f"・时间：{val('time_fixed','5')} 思考 + 每次操作 +{val('time_add','20')}s")
        L.append(f"・立直供托点数：{val('liqibang_value','1000')}　场棒点数："
                 f"{val('changbang_value','300')}")
        L.append(f"・食断：{self._fmt_flag(r.get('shiduan', 1))}　顺位得分："
                 f"{val('noting_fafu_1','1000')}/{val('noting_fafu_2','1500')}/"
                 f"{val('noting_fafu_3','3000')}"
                 f"　顺位马：{val('shunweima_2','5')}/{val('shunweima_3','-5')}/"
                 f"{val('shunweima_4','-15')}")

        # 特殊牌型（have_* 系列）
        have = sorted(k for k in r if k.startswith("have_"))
        on  = [k[5:] for k in have if r.get(k)]
        off = [k[5:] for k in have if not r.get(k)]
        # 中文名对照官方大会室 zh 词典
        # (tou_tiao=头跳; si_gang_san_le_liu_ju=四杠散了流局; nan_ru_xi_ru=南入/西入;
        #  gang_biao_dora=杠表宝牌; gang_li_dora=杠里宝牌; li_dora=里宝牌;
        #  local_yaku=古役; renhe=人和; tianbian=天边)
        NAME = {
            "yifa": "一发", "toutiao": "头跳", "zimosun": "自摸损",
            "qieshangmanguan": "切上满贯", "sifenglianda": "四风连打",
            "sigangsanle": "四杠散了", "sijializhi": "四家立直",
            "sanjiahele": "三家和了", "biad_dora": "表宝牌",
            "biao_dora": "表宝牌", "gang_biao_dora": "杠表宝牌",
            "gang_li_dora": "杠里宝牌", "li_dora": "里宝牌",
            "nanruxiru": "南入/西入", "jiuzhongjiupai": "九种九牌",
            "helelianzhuang": "和了连庄", "tingpailianzhuang": "听牌连庄",
            "helezhongju": "和了终局", "tingpaizhongju": "听牌终局",
            "liujumanguan": "流局满贯",
            # 大会室 detail_rule 可能出现的补充字段（官方名）
            "red_dora": "赤宝牌", "local_yaku": "古役", "renhe": "人和", "tianbian": "天边",
        }
        def cn(k):
            return NAME.get(k, k)
        L.append("")
        L.append("【特殊牌型】")
        if on:
            L.append("✅ 允许：" + "、".join(cn(k) for k in on))
        if off:
            L.append("⛔ 禁止：" + "、".join(cn(k) for k in off))

        # 规则开关
        # 只保留立直麻将的正常规则开关。
        # 对照官方 zh 词典: ji_fei=击飞; renhe=人和; shiti=食替;
        #   nontsumo_liqi=末巡立直(Local Yaku/Last Turn Riichi);
        #   baopai_extend_settings=宝牌相关扩展设置(表/里/杠宝牌开关)。
        # 已剔除的特色玩法开关(官方 game_mode_dora3=宝牌狂热、game_mode_xiuluo=修罗之战、
        #   game_mode_wanxiang=万象修罗、game_mode_chiyu=赤羽之战 等的组成字段，不属于正常模式):
        #   dora3_mode / guyi_mode / tianming_mode / jiuchao_mode / muyu_mode /
        #   yongchang_mode / huansanzhang / open_hand / reveal_discard
        SW = [
            ("can_jifei",        "击飞",     "允许击飞", "禁止击飞"),
            ("enable_renhe",     "人和",     "允许人和", "禁止人和"),
            ("enable_shiti",     "食替",     "允许食替", "禁止食替"),
            ("enable_nontsumo_liqi","末巡立直","开启",    "关闭"),
            ("enable_baopai_extend_settings","宝牌扩展","开启", "关闭"),
            ("ming_dora_immediately_open","宝牌即开","开启", "关闭"),
        ]
        L.append("")
        L.append("【规则开关】")
        for k, label, ya, yb in SW:
            if k in r:
                mark = "✅" if r.get(k) else "❌"
                L.append(f"・{label}：{mark} {ya if r.get(k) else yb}")

        # 禁用功能（disable_* 系列：开启即禁用该功能）
        DIS = [
            ("disable_angang_guoshi",   "暗杠"),
            ("disable_composite_yakuman", "复合役满"),
            ("disable_double_wind_four_fu", "双风四符"),
            ("disable_double_yakuman",  "双役满"),
            ("disable_leijiyiman",      "连击役满"),
            ("disable_multi_yukaman",   "多重役满"),
            ("disable_broadcast",       "广播"),
            ("disable_chat_room",       "聊天室"),
        ]
        blocked = [label for k, label in DIS if r.get(k)]
        L.append("")
        L.append("【禁用功能】" + ("、".join(blocked) if blocked else "无（全部允许）"))
        return "\n".join(L)

    @staticmethod
    def _fmt_flag(v) -> str:
        """detail_rule 开关值 → 开启/关闭（兼容 1/0、True/False）"""
        try:
            return "开启" if int(v) else "关闭"
        except (TypeError, ValueError):
            return "开启" if v else "关闭"


    def _do_remove(self, client, uid: int, season: int, param: str) -> str:
        # 删除选手: %删除 id1,id2
        ids = [x.strip() for x in param.replace("\n", ",").split(",") if x.strip()]
        if not ids:
            return "未识别到选手ID"
        players = client.fetch_season_player_list(uid, season)
        in_list = {str(p.get("account_id")): p.get("nickname", "?") for p in players}
        not_in = [i for i in ids if i not in in_list]
        if len(ids) == 1 and not_in:
            return "ID " + ids[0] + " 不在名单中"
        account_list = [{"account_id": int(i), "nickname": in_list.get(i, "?")} for i in ids if i in in_list]
        if not account_list:
            return "所有ID都不在名单中"
        result = client.remove_contest_season_player(uid, season, account_list)
        success = result.get("success", [])
        failed = result.get("failed", [])
        parts = []
        if success:
            names = [in_list.get(str(s), str(s)) for s in success]
            parts.append("已删除: " + ",".join(names))
        if failed:
            fnames = [(f.get("account_id"), f.get("error", "")) for f in failed]
            parts.append("失败: " + "; ".join(str(fid) + "(" + str(err) + ")" for fid, err in fnames))
        return "\n".join(parts) if parts else "无结果"

    def _do_start(self, client, uid: int, season: int, param: str) -> str:
        # 开赛语法:
        #   %开赛                                          → 自动用准备中玩家开赛
        #   %开赛 1234567,13579248                         → 指定玩家ID（不足4人补AI）
        #   %开赛 玩家1,玩家2,...                          → 指定玩家昵称（不足4人补AI）
        #   %开赛 玩家列表||标签                            → 设置对局标签(remark)
        #   %开赛 !玩家列表                                → 固定座位（不随机交换）
        #   %开赛 玩家1 500,玩家2 500,500,500              → 指定初始点数
        try:
            raw = param.strip()

            # 1. 解析标签: ||xxx
            remark = ""
            if "||" in raw:
                body, remark = raw.split("||", 1)
                raw = body.strip()

            # 2. 解析固定座位: !前缀
            shuffle_seats = True
            if raw.startswith("!"):
                shuffle_seats = False
                raw = raw[1:].strip()

            # 3. 如果完全为空，自动取准备中玩家（不足4人自动补AI）
            if not raw:
                ready = client.fetch_ready_player_list(uid, season)
                if not ready:
                    return "没有准备中的玩家，请指定玩家（%开赛 玩家列表）或让更多人进房准备"
                account_list = [p.get("account_id") for p in ready]
                names = [p.get("nickname", "?") for p in ready]
                init_points = [25000] * len(account_list)

            else:
                parts = [x.strip() for x in raw.replace("\n", ",").split(",") if x.strip()]
                account_list = []
                names = []
                init_points = []
                nickname_search_parts = []

                for part in parts:
                    # "玩家1 500" 形式（昵称+点数）
                    if " " in part:
                        sub = part.split()
                        if len(sub) >= 2:
                            try:
                                pt = int(sub[-1])
                                nickname_search_parts.append(sub[0])
                                init_points.append(pt)
                                continue
                            except ValueError:
                                pass
                    nickname_search_parts.append(part)

                # 全纯数字 = ID模式
                all_numeric = all(p.isdigit() for p in nickname_search_parts)

                if all_numeric:
                    ids = list(nickname_search_parts)
                    accounts = client.search_accounts(ids)
                    found = {str(a.get("account_id")): a.get("nickname", "?") for a in accounts}
                    missing = [i for i in ids if i not in found]
                    if missing:
                        return "以下ID无效或不存在: " + ",".join(missing)
                    account_list = [int(i) for i in ids]
                    names = [found.get(i, i) for i in ids]
                else:
                    for nm in nickname_search_parts:
                        search_results = client.fetch_season_player_list(uid, season, search=nm)
                        found_id = None
                        found_name = None
                        for sr in search_results:
                            if sr.get("nickname", "").strip().lower() == nm.strip().lower():
                                found_id = sr.get("account_id")
                                found_name = sr.get("nickname", nm)
                                break
                        if not found_id and search_results:
                            found_id = search_results[0].get("account_id")
                            found_name = search_results[0].get("nickname", nm)
                        if not found_id:
                            return f"❌ 未找到玩家: {nm}"
                        account_list.append(found_id)
                        names.append(found_name)

                # 补齐点数（默认25000）
                if init_points:
                    while len(init_points) < len(account_list):
                        init_points.append(25000)
                    init_points = init_points[:len(account_list)]
                else:
                    init_points = [25000] * len(account_list)

            # 查询赛事规则：目标人数（四麻4人/三麻3人）与规则初始点数
            target = 4
            default_point = 25000
            try:
                detail = client.fetch_contest_detail(uid)
                gm = (detail or {}).get("game_mode") or {}
                if gm.get("mode") in (11, 12):      # EAST_3 / SOUTH_3 → 三麻
                    target = 3
                ip = (gm.get("detail_rule") or {}).get("init_point")
                if ip:
                    default_point = int(ip)
            except Exception:
                pass

            if len(account_list) > target:
                return f"❌ 玩家数量({len(account_list)})超过{target}人上限，请检查名单"
            if len(set(account_list)) != len(account_list):
                return "❌ 玩家列表中存在重复，请检查名单"

            ai_level = 0  # 0=摸切电脑（与 mjdhs 一致）; 服务器会把占位 0 填充为摸切AI
            msg_suffix = ""
            ai_count = target - len(account_list)
            if ai_count > 0:
                # 用 account_id=0 占位补 AI（官方前端 create_game_plan 同款做法）
                for _ in range(ai_count):
                    account_list.append(0)
                    names.append("电脑")
                    init_points.append(default_point)
                msg_suffix = f"（真人{target-ai_count}人，自动补电脑×{ai_count}）"

            # AI 位点数=规则初点，校验总点数守恒（官方前端同样强制）
            if sum(init_points) != default_point * target:
                return (f"⚠️ 初始点数总和({sum(init_points)})与规则不符，"
                        f"应等于 {default_point}×{target}，请重新指定")

            client.create_game_plan(
                uid, season, account_list,
                ai_level=ai_level,
                init_points=init_points,
                remark=remark,
                shuffle_seats=shuffle_seats,
            )
            extra = f" | 标签: {remark}" if remark else ""
            extra += " | 固定座位" if not shuffle_seats else ""
            return "✅ 开赛成功! 玩家: " + ",".join(names) + msg_suffix + extra
        except ApiError as e:
            msg = str(e)
            if "ERR_CONTEST_PLAYER_IN_PLAN" in msg or "2543" in msg:
                return "⚠️ 开赛失败: 所选玩家已在待开始的对局计划中\n可先等待对局开始，或检查对局计划列表"
            return "❌ 开赛失败: " + msg
        except Exception as e:
            return "❌ 开赛失败: " + str(e)

    def _do_game_control(self, client, uid: int, cmd: str, param: str) -> str:
        # 暂停/恢复/终止对局: %暂停 [game_uuid] 或自动取当前对局
        try:
            game_uuid = param.strip()
            if not game_uuid:
                # 自动取当前进行中的对局
                running = client.fetch_running_game_list(uid, self._get_active_season(uid))
                if not running:
                    return "当前没有进行中的对局"
                game_uuid = running[0].get("game_uuid") or ""
                players = [p.get("nickname", "?") for p in running[0].get("players", []) if p]
                if players:
                    prefix = "对局(" + "/".join(players) + "): "
                else:
                    prefix = f"对局 {game_uuid[:20]}: "
            else:
                prefix = ""

            if cmd == "暂停":
                client.pause_contest_running_game(uid, game_uuid)
                return prefix + "✅ 已暂停对局 " + game_uuid[:20]
            elif cmd == "恢复":
                client.resume_contest_running_game(uid, game_uuid)
                return prefix + "✅ 已恢复对局 " + game_uuid[:20]
            elif cmd == "终止":
                client.terminate_contest_running_game(uid, game_uuid)
                return prefix + "✅ 已终止对局 " + game_uuid[:20]
            return "未知操作"
        except Exception as e:
            return "❌ " + cmd + "失败: " + str(e)

    # ---------- 对局开始/结束播报（风格同 mjdhs） ----------
    def _load_bcast(self):
        self._bcast_state = {}
        if BCAST_FILE.exists():
            try:
                self._bcast_state = json.loads(BCAST_FILE.read_text(encoding="utf-8"))
            except Exception:
                self._bcast_state = {}

    def _save_bcast(self):
        try:
            BCAST_FILE.write_text(
                json.dumps(self._bcast_state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _ensure_bcast(self):
        """启动播报轮询线程（幂等）；对局播报关闭时不启动。"""
        if not self.account_cfg.get("broadcast_enabled", True):
            return
        if self._bcast_thread and self._bcast_thread.is_alive():
            return
        self._bcast_stop.clear()
        self._bcast_thread = threading.Thread(
            target=self._bcast_loop, daemon=True, name="mjdhs-broadcast")
        self._bcast_thread.start()

    def _resolve_uid(self, cid) -> int:
        """contest_id -> unique_id（带缓存，避免每轮都拉全量赛事列表）"""
        cached = self._uid_cache.get(cid)
        if cached:
            return cached
        uid = self._resolve_unique_id(cid)
        if uid:
            self._uid_cache[cid] = uid
        return uid

    @staticmethod
    def _fmt_time(ts) -> str:
        """UTC+8 时间，格式同 mjdhs 的 moment.format('H:mm:ss')（小时不补零）"""
        try:
            if not ts:
                return ""
            t = time.localtime(float(ts) + 8 * 3600)
            return f"{t.tm_hour}:{t.tm_min:02d}:{t.tm_sec:02d}"
        except Exception:
            return ""

    def _bcast_loop(self):
        interval = max(int(self.account_cfg.get("poll_interval", 10) or 10), 3)
        logger.info(f"[mjdhs] 播报轮询已启动（每{interval}秒检查进行中对局与已完赛牌谱）")
        while not self._bcast_stop.wait(interval):
            interval = max(int(self.account_cfg.get("poll_interval", 10) or 10), 3)
            try:
                self._bcast_tick()
            except Exception as e:
                logger.warning(f"[mjdhs] 播报轮询异常: {e}")

    def _bcast_tick(self):
        client = None
        for gid, ent in list(self.db.items()):
            cid = self._entry_cid(ent)
            if not cid:
                continue
            session_str = (ent or {}).get("session") if isinstance(ent, dict) else None
            if not session_str:
                continue  # 老绑定记录还没补录 session，等群里下一条消息
            try:
                uid = self._resolve_uid(cid)
                if not uid:
                    continue
                if client is None:
                    client = self._get_client()
                season = self._get_active_season(uid)
                running = client.fetch_running_game_list(uid, season)
                records = client.fetch_game_records(uid, season, limit=20)
            except Exception as e:
                logger.warning(f"[mjdhs] 播报 {cid} 查询失败: {e}")
                continue

            key = str(cid)
            st = self._bcast_state.setdefault(
                key, {"running": [], "ended": [], "seeded": False})
            running_uuids = [g.get("game_uuid") for g in running if g.get("game_uuid")]
            ended_uuids = [r.get("uuid") for r in records if r.get("uuid")]

            if not st.get("seeded"):
                # 首次轮询只记录基线，不补播已存在的对局（与 mjdhs 的推送行为一致）
                st["running"] = running_uuids
                st["ended"] = ended_uuids[-50:]
                st["seeded"] = True
                self._save_bcast()
                continue

            self._bcast_starts(session_str, running, st)
            self._bcast_ends(session_str, running_uuids, ended_uuids, records, st)
            self._save_bcast()

    @staticmethod
    def _names_of(players) -> list:
        """mjdhs 写法: player.nickname ? nickname : '电脑'"""
        out = []
        for p in players or []:
            if not isinstance(p, dict):
                continue
            out.append(p.get("nickname") or "电脑")
        return out

    def _bcast_starts(self, session_str, running, st):
        known = set(st.get("running") or [])
        for g in running:
            guuid = g.get("game_uuid")
            if not guuid or guuid in known:
                continue
            names = self._names_of(g.get("players"))
            # mjdhs: '对局开始: ' + players.join(',')
            msg = "对局开始: " + (",".join(names) if names else str(guuid)[:16])
            self._announce(session_str, msg)

    def _bcast_ends(self, session_str, running_uuids, ended_uuids, records, st):
        ended_set = set(st.get("ended") or [])
        for r in records:
            ruuid = r.get("uuid")
            if not ruuid or ruuid in ended_set:
                continue
            self._announce(session_str, self._format_end(r))
            ended_set.add(ruuid)
        st["ended"] = list(ended_set)[-50:]

        # 进行中的对局消失且不在牌谱里 = 被终止（mjdhs 抓牌谱报 1203 → '对局被终止'）
        still_running = set(running_uuids)
        for guuid in list(st.get("running") or []):
            if guuid in still_running or guuid in ended_set or guuid in ended_uuids:
                continue
            self._announce(session_str, f"对局结束: /?paipu={guuid}\n对局被终止")
            ended_set.add(guuid)
            st["ended"] = list(ended_set)[-50:]

        st["running"] = running_uuids

    def _format_end(self, rec) -> str:
        """完全对齐 mjdhs 的 NotifyContestGameEnd 拼装:
        对局结束: /?paipu=<uuid>
        H:mm:ss - H:mm:ss
        昵称 part_point_1   （head.accounts 里按 seat 匹配，匹配不到就是 电脑）
        """
        uuid = rec.get("uuid") or ""
        lines = [f"对局结束: /?paipu={uuid}"]

        head = rec.get("head") if isinstance(rec.get("head"), dict) else {}
        accounts = rec.get("accounts") or head.get("accounts") or []
        result = rec.get("result") or head.get("result") or {}
        players = result.get("players") if isinstance(result, dict) else None

        if not players:
            lines.append("请求结果时遇到网络错误")
            return "\n".join(lines)

        name_by_seat = {}
        for a in accounts:
            if isinstance(a, dict) and a.get("seat") is not None:
                name_by_seat[a.get("seat")] = a.get("nickname") or "电脑"

        start = rec.get("start_time") or head.get("start_time")
        end = rec.get("end_time") or head.get("end_time")
        if start and end:
            lines.append(f"{self._fmt_time(start)} - {self._fmt_time(end)}")

        for p in players:
            nick = name_by_seat.get(p.get("seat")) or "电脑"
            lines.append(f"{nick} {p.get('part_point_1')}")
        return "\n".join(lines)

    def _announce(self, session_str, text):
        """从播报线程把消息送到绑定的群（跨线程调度到主事件循环）"""
        try:
            chain = MessageChain([Plain(text)])
            loop = self._main_loop
            if loop and loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self.context.send_message(session_str, chain), loop)
            else:
                asyncio.run(self.context.send_message(session_str, chain))
            logger.info(f"[mjdhs] 已播报到 {session_str}: {text.splitlines()[0]}")
        except Exception as e:
            logger.warning(f"[mjdhs] 播报发送失败 {session_str}: {e}")

    def _format_config(self) -> str:
        acct = self.account_cfg.get("account", "")
        pwd = self.account_cfg.get("password", "")
        pwd_masked = pwd[:3] + "***" if len(pwd) > 3 else "***"
        admins = self._admin_ids()
        mode = self.account_cfg.get("admin_mode", "any")
        mode_text = "只认白名单" if mode == "strict" else "白名单+群管理/admins_id"
        return ("【雀魂配置】\n账号: " + (acct or "(未设置)") + "\n密码: " + (pwd_masked if pwd else "(未设置)")
                + "\n可修改的人(admin_ids): " + (", ".join(admins) if admins else "(未设置→走原有管理员判断)")
                + "\n管理模式: " + mode_text
                + "\n改管理员: %配置 管理 123 456 / %配置 管理 清空")

    def _do_config(self, param: str) -> str:
        key_map = {"账号": "account", "密码": "password", "account": "account", "password": "password",
                   "管理": "admin_ids", "管理员": "admin_ids", "白名单": "admin_ids",
                   "模式": "admin_mode", "admin_mode": "admin_mode"}
        tokens = param.split()
        updates = []
        i = 0
        while i < len(tokens):
            t = tokens[i]
            key = key_map.get(t, "")
            if key in ("account", "password") and i + 1 < len(tokens):
                updates.append((key, tokens[i + 1]))
                i += 1
            elif key == "admin_ids":
                ids = []
                j = i + 1
                while j < len(tokens) and tokens[j] not in key_map:
                    ids.append(tokens[j])
                    j += 1
                if not ids:
                    return "用法: %配置 管理 <id...> / %配置 管理 清空"
                updates.append((key, ids))
                i = j - 1
            elif key == "admin_mode" and i + 1 < len(tokens):
                if tokens[i + 1] not in ("any", "strict"):
                    return "模式只能是 any 或 strict"
                updates.append((key, tokens[i + 1]))
                i += 1
            elif "=" in t:
                k, v = t.split("=", 1)
                if k in ("account", "password"):
                    updates.append((k, v))
            i += 1
        if not updates:
            return ("用法: %配置 账号 xxx 密码 xxx\n"
                    "       %配置 管理 123 456 (或 清空)\n"
                    "       %配置 模式 any/strict")
        for k, v in updates:
            if k == "admin_ids":
                if (isinstance(v, list) and len(v) == 1 and str(v[0]).lower() in ("清空", "clear")) \
                        or str(v).lower() in ("清空", "clear"):
                    self.account_cfg["admin_ids"] = []
                else:
                    cur = self._admin_ids()
                    for x in v:
                        s = str(x).strip()
                        if s and s not in cur:
                            cur.append(s)
                    self.account_cfg["admin_ids"] = cur
            else:
                self.account_cfg[k] = v
        self._client = None  # 重新登录
        self._save_config()
        if updates and updates[0][0] in ("account", "password"):
            return "✅ 配置已更新，将使用新账号登录"
        return "✅ 权限配置已更新（立即生效，无需重启）"

    async def terminate(self):
        self._bcast_stop.set()
        try:
            self.context.unregister_all()
        except Exception:
            pass
        await super().terminate()

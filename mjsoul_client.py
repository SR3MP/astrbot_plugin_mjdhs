"""
雀魂 (Mahjong Soul) 大会室 REST API 客户端
==========================================
基于对 www.maj-soul.com/contest-dashboard 后台的分析，纯 Python 实现。

认证流程:
    1. POST /api/login
       body: {account, password: HMAC-SHA256(password, key="lailai"), type: 0}
       → 返回 token
    2. 后续 API 请求头:  authorization: Majsoul <token>

API 基础地址: https://contest-gate-202411.maj-soul.com
"""
import hashlib
import hmac
import json
import urllib.error
import urllib.parse
import urllib.request

API_BASE = "https://contest-gate-202411.maj-soul.com"


def hash_password(password: str) -> str:
    """雀魂大会室密码加密: HMAC-SHA256(password, key='lailai')"""
    return hmac.new(b"lailai", password.encode("utf-8"), hashlib.sha256).hexdigest()


class MjsoulClient:
    """雀魂大会室 REST API 客户端"""

    def __init__(self, account: str = "", password: str = "", token: str = ""):
        self.account = account
        self.password = password
        self.token = token
        self.timeout = 20

    # ---------- 基础请求 ----------
    def _request(self, method: str, path: str, body: dict = None,
                 _allow_relogin: bool = True) -> dict:
        url = API_BASE + path
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
        }
        if self.token:
            headers["authorization"] = f"Majsoul {self.token}"

        data = json.dumps(body).encode("utf-8") if body else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="ignore")
            if e.code == 401 and _allow_relogin and path != "/api/login":
                # token 失效/过期: 自动重新登录后重试一次, 根治 401 刷屏
                self.token = ""  # 登录请求不带失效 token
                self.login()
                return self._request(method, path, body, _allow_relogin=False)
            raise ApiError(f"HTTP {e.code}: {body_text[:300]}", e.code)
        except urllib.error.URLError as e:
            raise ApiError(f"网络错误: {e.reason}", 0)

    # ---------- 登录 ----------
    def login(self) -> str:
        """登录并获取 token"""
        body = {
            "account": self.account,
            "password": hash_password(self.password),
            "type": 0,
        }
        resp = self._request("POST", "/api/login", body, _allow_relogin=False)
        self.token = resp.get("data", {}).get("token", "")
        if not self.token:
            raise ApiError(f"登录失败: {resp}", 401)
        return self.token

    # ---------- 业务 API ----------
    def fetch_contest_list(self) -> list:
        resp = self._request("GET", "/api/contest/fetch_contest_list")
        return resp.get("data", [])

    def fetch_contest_detail(self, unique_id) -> dict:
        resp = self._request("GET", f"/api/contest/fetch_contest_detail?unique_id={unique_id}")
        return resp.get("data", {})

    def fetch_contest_season_list(self, unique_id) -> list:
        resp = self._request("GET", f"/api/contest/fetch_contest_season_list?unique_id={unique_id}")
        return resp.get("data", [])

    def fetch_announcement(self, unique_id, language: str = "default") -> list:
        """获取赛事公告。

        官方 /api/contest/fetch_announcement 是平台级公告(只认 language=chs/chs_t,
        与具体赛事无关), 没有按 unique_id 区分赛事的公告接口;
        赛事自身公告位于 fetch_contest_detail 的
        public_notice(公开) / private_notice(内部) / manager_notice(管理员) 字段。

        返回 [{"title": "公开公告"/"内部公告"/..., "content": "..."}]，兼容 %公告 调用方。
        """
        def _pick(items):
            if not isinstance(items, list):
                return ""
            for it in items:
                if isinstance(it, dict) and it.get("lang") in (language, "default"):
                    c = it.get("content")
                    if isinstance(c, str) and c.strip():
                        return c.strip()
            for it in items:  # 兜底: 任意语言取首个非空
                if isinstance(it, dict):
                    c = it.get("content")
                    if isinstance(c, str) and c.strip():
                        return c.strip()
            return ""

        resp = self._request("GET", f"/api/contest/fetch_contest_detail?unique_id={unique_id}")
        d = resp.get("data") or {}
        out = []
        for title, key in (("公开公告", "public_notice"),
                           ("内部公告", "private_notice"),
                           ("管理员公告", "manager_notice")):
            c = _pick(d.get(key))
            if c:
                out.append({"title": title, "content": c})
        return out

    # ---------- 选手/对局/赛季 ----------
    def fetch_season_player_list(self, unique_id, season_id=0, search="", state=2, offset=0, limit=100) -> list:
        """选手名单 (返回 list)"""
        resp = self._request("GET", f"/api/contest/contest_season_player_list?unique_id={unique_id}&season_id={season_id}&search={urllib.parse.quote(search)}&state={state}&offset={offset}&limit={limit}")
        data = resp.get("data", {})
        return data.get("list", []) if isinstance(data, dict) else data

    def fetch_ready_player_list(self, unique_id, season_id=0) -> list:
        """准备中玩家"""
        resp = self._request("GET", f"/api/contest/ready_player_list?unique_id={unique_id}&season_id={season_id}")
        return resp.get("data", [])

    def fetch_running_game_list(self, unique_id, season_id=0) -> list:
        """进行中对局"""
        resp = self._request("GET", f"/api/contest/contest_running_game_list?unique_id={unique_id}&season_id={season_id}")
        return resp.get("data", [])

    def fetch_game_records(self, unique_id, season_id=0, offset=0, limit=10) -> list:
        """对局记录（返回 record_list）"""
        resp = self._request("GET", f"/api/contest/fetch_contest_game_records?unique_id={unique_id}&season_id={season_id}&offset={offset}&limit={limit}")
        data = resp.get("data", {})
        if isinstance(data, dict):
            return data.get("record_list", [])
        return data

    def search_accounts(self, account_ids: list) -> list:
        """按雀魂ID搜索账号（添加选手前验证用）"""
        resp = self._request("POST", "/api/contest/search_accounts", {"account_list": account_ids})
        return resp.get("data", [])

    def add_contest_season_player(self, unique_id, season_id, account_list: list) -> dict:
        """添加选手到赛季名单
        account_list: [{"account_id": 123, "nickname": "xxx"}, ...]
        返回: {"failed": [], "success": [account_id]}
        """
        resp = self._request("POST", "/api/contest/add_contest_season_player", {
            "unique_id": unique_id,
            "season_id": season_id,
            "account_list": account_list,
        })
        return resp.get("data", {})

    def remove_contest_season_player(self, unique_id, season_id, account_list: list) -> dict:
        """从赛季名单移除选手（推测端点，格式同添加）"""
        resp = self._request("POST", "/api/contest/remove_contest_season_player", {
            "unique_id": unique_id,
            "season_id": season_id,
            "account_list": account_list,
        })
        return resp.get("data", {})

    def pause_contest_running_game(self, unique_id, game_uuid) -> dict:
        """暂停进行中的对局"""
        resp = self._request("POST", "/api/contest/pause_contest_running_game", {
            "unique_id": unique_id,
            "game_uuid": game_uuid,
            "resume": 1,
        })
        return resp.get("data", {})

    def resume_contest_running_game(self, unique_id, game_uuid) -> dict:
        """恢复已暂停的对局"""
        resp = self._request("POST", "/api/contest/pause_contest_running_game", {
            "unique_id": unique_id,
            "game_uuid": game_uuid,
            "resume": 2,
        })
        return resp.get("data", {})

    def terminate_contest_running_game(self, unique_id, game_uuid) -> dict:
        """终止进行中的对局（注意: 参数用 uuid 而非 game_uuid）"""
        resp = self._request("POST", "/api/contest/terminate_contest_running_game", {
            "unique_id": unique_id,
            "uuid": game_uuid,
        })
        return resp.get("data", {})

    def create_game_plan(self, unique_id, season_id, account_list: list,
                         init_points=None, ai_level=0, shuffle_seats=True,
                         game_start_time=0, remark="") -> dict:
        """开赛（创建对局计划）
        account_list: [account_id, ...] 玩家ID数组；不足人数时用 0 占位补 AI（
                      0 = 电脑/AI 占位，ai_level=0 为摸切电脑，与 mjdhs 一致；
                      1简单/2普通）
        init_points: 各座位初始点数，长度须与 account_list 一致，默认全 25000
        """
        if init_points is None:
            init_points = [25000] * max(4, len(account_list))
        if not game_start_time:
            import time
            game_start_time = int(time.time())  # 传当前时间戳 = 立即开赛（官方同款）
        resp = self._request("POST", "/api/contest/create_game_plan", {
            "unique_id": unique_id,
            "season_id": season_id,
            "account_list": account_list,
            "ai_level": ai_level,
            "game_start_time": game_start_time,
            "init_points": init_points,
            "remark": remark,
            "shuffle_seats": shuffle_seats,
        })
        return resp.get("data", {})


class ApiError(Exception):
    """雀魂大会室 API 错误（HTTP / 网络 / 业务错误码）"""

    def __init__(self, message: str, code: int = 0):
        super().__init__(message)
        self.code = code


if __name__ == "__main__":
    # 自测
    import sys
    if len(sys.argv) >= 3:
        client = MjsoulClient(sys.argv[1], sys.argv[2])
        try:
            token = client.login()
            print(f"✅ 登录成功: token={token[:12]}...")
            contests = client.fetch_contest_list()
            print(f"  赛事数: {len(contests)}")
            for c in contests:
                name = c.get("contest_name", [{}])[0].get("content", "?")
                print(f"    - {c.get('contest_id')}: {name}")
        except ApiError as e:
            print(f"❌ 错误: {e}")
    else:
        print("用法: python3 mjsoul_client.py <账号> <密码>")

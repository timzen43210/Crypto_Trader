# -*- coding: utf-8 -*-
"""
B3 (logsetup) 驗收測試 — 交易系統不能靜默失敗。

  AC-1  setup() 之後 getLogger(__name__) 同時寫到終端機與日誌檔；目錄自己建；重複呼叫不重複輸出
  AC-2  含中文與 🔴✅❌⚠️📥 的訊息：終端機沒有 Logging error、那一行還在；檔案裡逐字一致
  AC-3  輪替真的發生、保留份數符合設定、舊內容被丟掉（不會無限長大）、沒有 Logging error
  AC-4  主執行緒 / 子執行緒的未攔截例外帶完整 traceback 進日誌；主執行緒照樣死，
        子執行緒死了程序照樣跑；Ctrl+C 不記成 crash；正常情況 stderr 上 traceback 只有一份
  S-1   setup() 之後 logging 被外部停用 / 重設 / handler 被拆，crash 的 traceback 仍然印到
        stderr（剛好一份），不會完全消失
  AC-5  預設日誌路徑在 runtime/ 底下，且真的被 .gitignore 擋住
  M-5   時間戳是台北時間，與主機時區無關

為什麼大部分用子行程：本檔結尾的 runner（跟其他測試檔一樣）會先把 sys.stdout reconfigure 成
UTF-8，在同一個行程裡測編碼等於白測。子行程的環境拿掉 PYTHONIOENCODING / PYTHONUTF8，
stdout / stderr 都是 pipe，量到的就是 Windows 上真實的預設行為（laptop 是 cp1252）。
而且 logging 永遠不會把編碼錯誤往外拋，所以這裡驗的不是「沒拋例外」（對任何實作都成立），
而是「沒有 --- Logging error --- 而且訊息還在」。

子行程測試的 setup 片段是參數（預設 REAL_SETUP），用來拿「完全沒處理編碼」之類的爛實作跑
同一組斷言、證明斷言有鑑別力。

日誌一律寫到系統暫存目錄，不碰真正的 runtime/；全程不連網。
不依賴 pytest：直接 `python tests/test_logsetup.py` 會逐一跑完並印結果。
"""
import io
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import textwrap

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from live import config, logsetup, paths  # noqa: E402

EMOJI = "🔴✅❌⚠️📥"
MARKER = "B3-ENC-CHECK"
# 頭尾都有 ASCII 片段：終端機上 emoji / 中文可能被跳脫，靠頭尾確認整行都在
MESSAGE = MARKER + " A頻道推播測試 " + EMOJI + " 中文結尾 END-OF-LINE"

# 子行程用的 setup 片段。LOG_FILE 等變數由 _child_code 先定義好。
REAL_SETUP = "from live import logsetup\nlogsetup.setup(log_file=LOG_FILE)\n"
REAL_SETUP_ROTATING = (
    "from live import logsetup\n"
    "logsetup.setup(log_file=LOG_FILE, max_bytes=MAX_BYTES, backup_count=BACKUP_COUNT)\n"
)

ROTATE_MAX_BYTES = 2000
ROTATE_BACKUP_COUNT = 3
ROTATE_MESSAGES = 300


# ============================== 子行程工具 ==============================
def _child_code(log_file, body, **extra):
    """組出子行程要跑的程式：先把 repo 放上 sys.path、定義常數，再接 body。全程 ASCII。"""
    head = [
        "import logging, os, sys",
        "sys.path.insert(0, %r)" % REPO_ROOT,
        "LOG_FILE = %r" % log_file,
        "MESSAGE = %s" % ascii(MESSAGE),
        "EMOJI = %s" % ascii(EMOJI),
    ]
    head += ["%s = %r" % (k, v) for k, v in extra.items()]
    return "\n".join(head) + "\n" + textwrap.dedent(body)


def _run_child(code, env_overrides=None):
    """在乾淨的子行程跑 code：拿掉 PYTHONIOENCODING / PYTHONUTF8，stdout / stderr 都是 pipe。

    回傳 (returncode, stdout, stderr)。輸出用 ASCII + backslashreplace 解碼：不管子行程
    用什麼編碼寫，ASCII 片段（MARKER、Logging error）都原樣保留，比對不會因解碼而失真。
    """
    env = dict(os.environ)
    env.pop("PYTHONIOENCODING", None)
    env.pop("PYTHONUTF8", None)
    env.update(env_overrides or {})
    r = subprocess.run(
        [sys.executable, "-c", code], cwd=REPO_ROOT, env=env,
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
    )
    return (r.returncode,
            r.stdout.decode("ascii", "backslashreplace"),
            r.stderr.decode("ascii", "backslashreplace"))


def _describe(rc, out, err):
    return "rc=%s\n--- stdout ---\n%s\n--- stderr ---\n%s" % (rc, out[-3000:], err[-3000:])


def _read_log(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def _in_tempdir(fn):
    """給 fn 一個全新的暫存目錄，結束後刪掉（刪不掉就讓它拋：代表有檔案沒關）。"""
    tmp = tempfile.mkdtemp(prefix="b3_logsetup_")
    try:
        return fn(tmp)
    finally:
        shutil.rmtree(tmp)


def _with_inprocess_setup(fn, **setup_kwargs):
    """同一行程裡 setup()：日誌導到暫存目錄、終端機導到 StringIO，結束時 teardown 並刪目錄。

    fn(log_file, console) 拿到日誌檔路徑與終端機的 StringIO。只拿來測「不涉及編碼」的行為。
    """
    def go(tmp):
        log_file = os.path.join(tmp, "logs", "live.log")
        console = io.StringIO()
        saved_stderr = sys.stderr
        sys.stderr = console
        try:
            logsetup.setup(log_file=log_file, **setup_kwargs)
            return fn(log_file, console)
        finally:
            logsetup.teardown()
            sys.stderr = saved_stderr
    return _in_tempdir(go)


# ============================== AC-2：非 cp950 字元 ==============================
def _encoding_round(env_overrides, setup_code=REAL_SETUP, pre_setup=""):
    """M-3 的 (a)–(d)。回傳子行程回報的串流編碼（給呼叫端做前提檢查）。"""
    def go(tmp):
        log_file = os.path.join(tmp, "logs", "live.log")
        body = pre_setup + setup_code + textwrap.dedent("""
            print("CHILD-ENC %s/%s %s/%s" % (sys.stdout.encoding, sys.stdout.errors,
                                             sys.stderr.encoding, sys.stderr.errors))
            sys.stdout.flush()
            logging.getLogger("live.b3_child").warning(MESSAGE)
        """)
        rc, out, err = _run_child(_child_code(log_file, body), env_overrides)
        where = "env=%s\n%s" % (env_overrides, _describe(rc, out, err))
        # (a) exit 0
        assert rc == 0, where
        # (b) stdout 與 stderr 都沒有 Logging error
        assert "Logging error" not in out and "Logging error" not in err, where
        # (c) 終端機上找得到這一則，而且整行都在（頭尾 ASCII 片段在同一行）。
        # 排除 `Message: '...'` 開頭的行：那是 --- Logging error --- 傾印出來的原始訊息，
        # 不排除的話，訊息寫失敗的實作也會被當成「找得到」，(c) 就只能靠 (b) 撐著
        lines = [ln for ln in (out + "\n" + err).splitlines()
                 if MARKER in ln and not ln.startswith("Message: ")]
        assert len(lines) == 1, "終端機上應該剛好一行含 %s\n%s" % (MARKER, where)
        assert "END-OF-LINE" in lines[0], "那一行被截斷了\n" + where
        # (d) 日誌檔以 UTF-8 讀回，中文與五個 emoji 逐字一致
        file_lines = [ln for ln in _read_log(log_file).splitlines() if MARKER in ln]
        assert len(file_lines) == 1, "日誌檔應該剛好一行含 %s：%r" % (MARKER, file_lines)
        # MESSAGE 以 MARKER 開頭，所以 endswith 就是「整則訊息逐字都在」；不綁日誌格式
        assert file_lines[0].endswith(MESSAGE), "檔案內容與寫入時不一致：%r" % file_lines[0]
        for ch in ("🔴", "✅", "❌", "⚠️", "📥", "中文"):
            assert ch in file_lines[0], (ch, file_lines[0])
        enc = [ln for ln in out.splitlines() if ln.startswith("CHILD-ENC ")]
        return enc[0] if enc else ""
    return _in_tempdir(go)


def test_ac2_emoji_default_terminal_encoding():
    """不設 PYTHONIOENCODING（laptop 上 stdout / stderr 是 cp1252 的 pipe）。"""
    _encoding_round({})


def test_ac2_emoji_cp950_terminal():
    """PYTHONIOENCODING=cp950，模擬繁中 Windows。"""
    enc = _encoding_round({"PYTHONIOENCODING": "cp950"})
    assert "cp950" in enc, "PYTHONIOENCODING 沒生效，這一輪沒測到 cp950：%r" % enc


def test_ac2_console_does_not_rely_on_stderr_errors_mode():
    """stderr 被設成 errors=strict 也一樣。

    CPython 的 stderr 預設是 backslashreplace，所以「handler 直接掛 sys.stderr」在上面兩輪
    也會碰巧過關；這一輪把那個預設拿掉，證明降級是 handler 自己做的，不是運氣。
    """
    enc = _encoding_round({"PYTHONIOENCODING": "cp950"},
                          pre_setup='sys.stderr.reconfigure(errors="strict")\n')
    assert enc.endswith("cp950/strict"), "前提不成立，stderr 不是 strict：%r" % enc


# ============================== AC-1：基本運作 ==============================
def _basic_round(setup_code=REAL_SETUP):
    def go(tmp):
        log_file = os.path.join(tmp, "not", "yet", "there", "live.log")
        assert not os.path.exists(os.path.dirname(log_file))
        body = setup_code + setup_code + textwrap.dedent("""
            print("HANDLERS=%d" % len(logging.getLogger().handlers))
            sys.stdout.flush()
            logging.getLogger("live.some_module").info("B3-AC1-ONCE hello")
        """)
        rc, out, err = _run_child(_child_code(log_file, body))
        where = _describe(rc, out, err)
        assert rc == 0, where
        assert "HANDLERS=2" in out, "setup() 呼叫兩次後根 logger 應該只有 2 個 handler\n" + where
        assert os.path.isdir(os.path.dirname(log_file)), "日誌目錄沒有被 setup() 建出來"
        assert (out + err).count("B3-AC1-ONCE") == 1, "終端機上應該剛好出現一次\n" + where
        assert _read_log(log_file).count("B3-AC1-ONCE") == 1, "日誌檔裡應該剛好出現一次"
    _in_tempdir(go)


def test_ac1_logger_writes_to_terminal_and_file_once_even_after_double_setup():
    _basic_round()


def test_ac1_setup_without_arguments_follows_config():
    """不傳參數時路徑取自 config.LOG_FILE，而且是呼叫當下才讀（不是 import 時綁死）。"""
    saved = config.LOG_FILE

    def go(tmp):
        target = os.path.join(tmp, "from_config", "live.log")
        config.LOG_FILE = target
        console = io.StringIO()
        saved_stderr = sys.stderr
        sys.stderr = console
        try:
            used = logsetup.setup()
            logging.getLogger("live.b3_default").warning("B3-DEFAULT-PATH")
        finally:
            logsetup.teardown()
            sys.stderr = saved_stderr
        assert used == os.path.abspath(target)
        assert "B3-DEFAULT-PATH" in _read_log(target)
        assert "B3-DEFAULT-PATH" in console.getvalue()

    try:
        _in_tempdir(go)
    finally:
        config.LOG_FILE = saved


def test_ac1_import_has_no_side_effects():
    """import live.logsetup 不掛 handler、不換 excepthook、不建目錄。"""
    code = textwrap.dedent("""
        import logging, os, sys, threading
        sys.path.insert(0, %r)
        from live import config
        log_dir = os.path.dirname(config.LOG_FILE)
        before = os.path.exists(log_dir)
        import live.logsetup
        assert logging.getLogger().handlers == [], logging.getLogger().handlers
        assert sys.excepthook is sys.__excepthook__
        assert threading.excepthook is threading.__excepthook__
        assert os.path.exists(log_dir) == before
        print("IMPORT-CLEAN")
    """ % REPO_ROOT)
    rc, out, err = _run_child(code)
    assert rc == 0 and "IMPORT-CLEAN" in out, _describe(rc, out, err)


def test_teardown_restores_everything_and_releases_the_file():
    root = logging.getLogger()
    orig = (sys.excepthook, threading.excepthook, list(root.handlers), root.level)

    def go(tmp):
        log_file = os.path.join(tmp, "logs", "live.log")
        saved_stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            logsetup.setup(log_file=log_file)
            logsetup.setup(log_file=log_file)
            assert sys.excepthook is not orig[0]
            assert threading.excepthook is not orig[1]
        finally:
            logsetup.teardown()
            sys.stderr = saved_stderr
        # _in_tempdir 收尾時 rmtree 不帶 ignore_errors：Windows 上檔案沒關就會在這裡失敗

    _in_tempdir(go)
    assert (sys.excepthook, threading.excepthook, list(root.handlers), root.level) == orig


def test_invalid_level_fails_fast_and_keeps_previous_setup():
    def check(log_file, console):
        before = list(logging.getLogger().handlers)
        try:
            logsetup.setup(log_file=log_file, level="NOT_A_LEVEL")
        except ValueError:
            pass
        else:
            raise AssertionError("等級名稱打錯卻沒有拋例外")
        assert logging.getLogger().handlers == before, "失敗的 setup() 不該動到原本的 handler"
        logging.getLogger("live.b3_level").warning("B3-STILL-WORKS")
        assert "B3-STILL-WORKS" in _read_log(log_file)
    _with_inprocess_setup(check)


# ============================== M-5：台北時間 ==============================
def test_timestamp_is_taipei_for_fixed_created():
    """created = 0.0 的紀錄經過實際掛上的 handler，時間字串是 1970-01-01 08:00:00。"""
    def check(log_file, console):
        lg = logging.getLogger("live.b3_time")
        rec = lg.makeRecord(lg.name, logging.WARNING, __file__, 1, "B3-EPOCH", None, None)
        rec.created = 0.0
        rec.msecs = 0.0
        lg.handle(rec)
        line = [ln for ln in _read_log(log_file).splitlines() if "B3-EPOCH" in ln][0]
        assert line == "1970-01-01 08:00:00 WARNING live.b3_time: B3-EPOCH", line
        assert "1970-01-01 08:00:00 WARNING live.b3_time: B3-EPOCH" in console.getvalue()
    _with_inprocess_setup(check)


def test_timestamp_ignores_host_timezone():
    """把子行程的主機時區設成 UTC（TZ=UTC0），日誌時間仍然是台北時間。

    laptop 的主機時區本來就是台北，標準 Formatter 在這裡也會印 08:00:00 —— 所以同一個
    子行程裡順便證明「標準 Formatter 在這個環境印的是 00:00:00」，這條測試才有鑑別力。
    """
    def go(tmp):
        log_file = os.path.join(tmp, "logs", "live.log")
        body = REAL_SETUP + textwrap.dedent("""
            import time
            lg = logging.getLogger("live.b3_time")
            rec = lg.makeRecord(lg.name, logging.WARNING, "x", 1, "B3-EPOCH-UTC", None, None)
            rec.created = 0.0
            rec.msecs = 0.0
            print("HOST-HOUR=%d" % time.localtime(0).tm_hour)
            print("STDLIB=" + logging.Formatter().formatTime(rec))
            lg.handle(rec)
        """)
        rc, out, err = _run_child(_child_code(log_file, body), {"TZ": "UTC0"})
        where = _describe(rc, out, err)
        assert rc == 0, where
        assert "HOST-HOUR=0" in out, "前提不成立：TZ=UTC0 沒有讓主機時區變成 UTC\n" + where
        assert "STDLIB=1970-01-01 00:00:00" in out, where
        line = [ln for ln in _read_log(log_file).splitlines() if "B3-EPOCH-UTC" in ln][0]
        assert line.startswith("1970-01-01 08:00:00 "), line
    _in_tempdir(go)


# ============================== AC-3：輪替 ==============================
def _rotation_round(setup_code=REAL_SETUP_ROTATING):
    def go(tmp):
        log_dir = os.path.join(tmp, "logs")
        log_file = os.path.join(log_dir, "live.log")
        body = setup_code + textwrap.dedent("""
            lg = logging.getLogger("live.b3_rotate")
            for i in range(N_MESSAGES):
                lg.info("rotate-%05d %s", i, "x" * 60)
        """)
        code = _child_code(log_file, body, MAX_BYTES=ROTATE_MAX_BYTES,
                           BACKUP_COUNT=ROTATE_BACKUP_COUNT, N_MESSAGES=ROTATE_MESSAGES)
        rc, out, err = _run_child(code)
        where = _describe(rc, out, err)
        assert rc == 0, where
        assert "Logging error" not in out and "Logging error" not in err, where

        expected = ["live.log"] + ["live.log.%d" % k for k in range(1, ROTATE_BACKUP_COUNT + 1)]
        assert sorted(os.listdir(log_dir)) == sorted(expected), \
            "輪替後的檔案應該剛好是 %s，實際 %s" % (expected, sorted(os.listdir(log_dir)))
        sizes = {n: os.path.getsize(os.path.join(log_dir, n)) for n in expected}
        assert all(s <= ROTATE_MAX_BYTES for s in sizes.values()), sizes
        assert sum(sizes.values()) <= (ROTATE_BACKUP_COUNT + 1) * ROTATE_MAX_BYTES, sizes

        # 舊的在編號大的檔、新的在 live.log；留下來的是連續的一段尾巴，最早的已被丟掉
        seen = []
        for name in reversed(expected):
            for ln in _read_log(os.path.join(log_dir, name)).splitlines():
                if "rotate-" in ln:
                    seen.append(int(ln.split("rotate-", 1)[1][:5]))
        assert seen == list(range(seen[0], ROTATE_MESSAGES)), "輪替後的內容不連續或順序錯了"
        assert seen[0] > 0, "最早的訊息還在，代表沒有丟掉任何舊內容"
    _in_tempdir(go)


def test_ac3_rotation_really_happens():
    _rotation_round()


# ============================== AC-4：未攔截例外 ==============================
def _main_crash_round(setup_code=REAL_SETUP):
    def go(tmp):
        log_file = os.path.join(tmp, "logs", "live.log")
        body = setup_code + textwrap.dedent("""
            def _b3_inner_crash():
                raise RuntimeError("B3-CRASH-MAIN " + EMOJI)
            def _b3_outer():
                _b3_inner_crash()
            _b3_outer()
            print("AFTER-CRASH-SHOULD-NOT-PRINT")
        """)
        rc, out, err = _run_child(_child_code(log_file, body), {"PYTHONIOENCODING": "cp950"})
        where = _describe(rc, out, err)
        assert rc != 0, "未攔截例外之後程序應該結束且 exit code 非 0\n" + where
        assert "AFTER-CRASH-SHOULD-NOT-PRINT" not in out, "例外被吞掉、程式繼續跑了\n" + where
        assert "Logging error" not in err, where
        assert "B3-CRASH-MAIN" in err, "終端機上也應該看得到這次 crash\n" + where
        assert err.count("Traceback (most recent call last):") == 1, \
            "正常路徑下 traceback 在 stderr 應該剛好一份\n" + where
        log = _read_log(log_file) if os.path.exists(log_file) else ""
        for piece in ("CRITICAL", "Traceback (most recent call last):",
                      "_b3_outer", "_b3_inner_crash", "RuntimeError: B3-CRASH-MAIN " + EMOJI):
            assert piece in log, "日誌檔裡缺 %r：\n%s" % (piece, log)
    _in_tempdir(go)


def test_ac4_main_thread_crash_is_logged_and_process_dies():
    _main_crash_round()


def _thread_crash_round(setup_code=REAL_SETUP):
    def go(tmp):
        log_file = os.path.join(tmp, "logs", "live.log")
        body = setup_code + textwrap.dedent("""
            import threading
            def _b3_worker():
                raise ValueError("B3-CRASH-THREAD")
            t = threading.Thread(target=_b3_worker, name="b3-worker")
            t.start()
            t.join()
            print("MAIN-STILL-ALIVE")
            logging.getLogger("live.b3_child").warning("B3-AFTER-THREAD")
        """)
        rc, out, err = _run_child(_child_code(log_file, body))
        where = _describe(rc, out, err)
        assert rc == 0, "執行緒死掉不該讓程序結束\n" + where
        assert "MAIN-STILL-ALIVE" in out, where
        assert "Logging error" not in err, where
        assert err.count("Traceback (most recent call last):") == 1, \
            "正常路徑下 traceback 在 stderr 應該剛好一份\n" + where
        log = _read_log(log_file) if os.path.exists(log_file) else ""
        for piece in ("CRITICAL", "b3-worker", "Traceback (most recent call last):",
                      "_b3_worker", "ValueError: B3-CRASH-THREAD", "B3-AFTER-THREAD"):
            assert piece in log, "日誌檔裡缺 %r：\n%s" % (piece, log)
        assert log.index("B3-CRASH-THREAD") < log.index("B3-AFTER-THREAD")
    _in_tempdir(go)


def test_ac4_thread_crash_is_logged_and_process_continues():
    _thread_crash_round()


def test_ac4_keyboard_interrupt_is_not_logged_as_crash():
    def go(tmp):
        log_file = os.path.join(tmp, "logs", "live.log")
        body = REAL_SETUP + "raise KeyboardInterrupt\n"
        rc, out, err = _run_child(_child_code(log_file, body))
        where = _describe(rc, out, err)
        assert rc != 0, "Ctrl+C 之後程序應該結束\n" + where
        assert "KeyboardInterrupt" in err, "應該交回預設處理（印出 KeyboardInterrupt）\n" + where
        log = _read_log(log_file)
        assert "CRITICAL" not in log and "KeyboardInterrupt" not in log, log
    _in_tempdir(go)


# ============================== S-1：logging 被外部停用 / 重設後 ==============================
# setup() 之後若有人停用或重設 logging，crash 的 traceback 不可以因此完全消失：最差也要跟
# 沒有 setup() 一樣印到 stderr。破壞手段只用標準 logging API，不碰 live.logsetup 的內部，
# 所以同一組測試也能拿去打別的實作。每段破壞都自帶前提檢查，成立才印 SABOTAGE-APPLIED ——
# 否則前提沒成立時子行程會因 assert 死掉，stderr 也有 traceback，測試就會假通過。
SABOTAGE_DICTCONFIG = textwrap.dedent("""
    import logging.config
    logging.config.dictConfig({"version": 1, "root": {"level": "INFO"}})
    assert logging.getLogger("live.logsetup").disabled, "dictConfig did not disable live.logsetup"
    assert not logging.getLogger().handlers, "dictConfig did not clear root handlers"
""")
SABOTAGE_DISABLE = textwrap.dedent("""
    logging.disable(logging.CRITICAL)
""")
SABOTAGE_REMOVE_ALL_HANDLERS = textwrap.dedent("""
    for _h in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(_h)
    assert not logging.getLogger().handlers
""")
SABOTAGE_REMOVE_CONSOLE_HANDLER = textwrap.dedent("""
    for _h in list(logging.getLogger().handlers):
        if not isinstance(_h, logging.FileHandler):
            logging.getLogger().removeHandler(_h)
    assert [type(_h).__name__ for _h in logging.getLogger().handlers] == ["RotatingFileHandler"]
""")


def _crash_fallback_round(sabotage, in_thread=False, expect_in_file=False, setup_code=REAL_SETUP):
    """setup() → 外部破壞 logging → crash。stderr 上必須剛好一份完整 traceback。"""
    def go(tmp):
        log_file = os.path.join(tmp, "logs", "live.log")
        if in_thread:
            crash = textwrap.dedent("""
                import threading
                def _b3_fallback_frame():
                    raise ValueError("B3-FALLBACK-CRASH")
                t = threading.Thread(target=_b3_fallback_frame, name="b3-fallback-worker")
                t.start()
                t.join()
                print("MAIN-STILL-ALIVE")
            """)
        else:
            crash = textwrap.dedent("""
                def _b3_fallback_frame():
                    raise ValueError("B3-FALLBACK-CRASH")
                _b3_fallback_frame()
                print("AFTER-CRASH-SHOULD-NOT-PRINT")
            """)
        body = (setup_code + sabotage
                + 'print("SABOTAGE-APPLIED")\nsys.stdout.flush()\n' + crash)
        rc, out, err = _run_child(_child_code(log_file, body))
        where = _describe(rc, out, err)
        assert "SABOTAGE-APPLIED" in out, "前提不成立：破壞手段沒有生效\n" + where
        if in_thread:
            assert rc == 0, "執行緒死掉不該讓程序結束\n" + where
            assert "MAIN-STILL-ALIVE" in out, where
        else:
            assert rc != 0, "未攔截例外之後程序應該結束且 exit code 非 0\n" + where
            assert "AFTER-CRASH-SHOULD-NOT-PRINT" not in out, where
        # 完整 traceback：開頭、出事的那一層函式、例外類別與訊息，而且剛好一份
        assert err.count("Traceback (most recent call last):") == 1, \
            "stderr 上的 traceback 應該剛好一份（0 = 消失，2 = 印兩次）\n" + where
        assert "_b3_fallback_frame" in err, where
        assert err.count("ValueError: B3-FALLBACK-CRASH") == 1, where
        if expect_in_file:
            log = _read_log(log_file) if os.path.exists(log_file) else ""
            assert "CRITICAL" in log and "ValueError: B3-FALLBACK-CRASH" in log, \
                "檔案 handler 還在，crash 應該照樣進日誌檔：\n" + log
    _in_tempdir(go)


def test_s1_main_crash_after_dictconfig_still_prints_traceback():
    _crash_fallback_round(SABOTAGE_DICTCONFIG)


def test_s1_main_crash_after_logging_disable_still_prints_traceback():
    _crash_fallback_round(SABOTAGE_DISABLE)


def test_s1_main_crash_after_all_handlers_removed_prints_exactly_once():
    """handler 全被拆掉時不可以再呼叫 critical()，否則 logging.lastResort 會多印一份。"""
    _crash_fallback_round(SABOTAGE_REMOVE_ALL_HANDLERS)


def test_s1_main_crash_after_console_handler_removed_reaches_file_and_stderr():
    """只拆掉終端機 handler：日誌檔照記，stderr 也要有一份（交回原本的 hook 印）。"""
    _crash_fallback_round(SABOTAGE_REMOVE_CONSOLE_HANDLER, expect_in_file=True)


def test_s1_thread_crash_after_dictconfig_still_prints_traceback():
    _crash_fallback_round(SABOTAGE_DICTCONFIG, in_thread=True)


def test_s1_thread_crash_after_logging_disable_still_prints_traceback():
    _crash_fallback_round(SABOTAGE_DISABLE, in_thread=True)


def test_s1_thread_crash_after_console_handler_removed_reaches_file_and_stderr():
    _crash_fallback_round(SABOTAGE_REMOVE_CONSOLE_HANDLER, in_thread=True, expect_in_file=True)


# ============================== AC-5：不污染版控路徑 ==============================
def test_ac5_default_log_file_is_under_runtime():
    log_file = os.path.abspath(config.LOG_FILE)
    runtime = os.path.abspath(paths.RUNTIME_DIR)
    assert os.path.commonpath([log_file, runtime]) == runtime, log_file
    for forbidden in ("state", "output"):
        top = os.path.join(paths.REPO_ROOT, forbidden)
        assert os.path.commonpath([log_file, top]) != top, log_file


def test_ac5_runtime_logs_are_gitignored():
    """用 git 本人判斷：預設日誌檔與輪替出來的舊檔都被 .gitignore 擋住（檔案不必存在）。"""
    rel = os.path.relpath(config.LOG_FILE, paths.REPO_ROOT).replace(os.sep, "/")
    for path in (rel, rel + ".1", rel + "." + str(config.LOG_BACKUP_COUNT)):
        r = subprocess.run(["git", "check-ignore", "-q", "--", path], cwd=paths.REPO_ROOT,
                           capture_output=True, timeout=30)
        assert r.returncode == 0, "%s 沒有被 .gitignore 擋住（git check-ignore rc=%d）" % (path, r.returncode)


# ============================== FR-6 / M-4：python -m live ==============================
def test_smoke_check_reports_log_location_without_creating_it():
    """python -m live 在 cp1252 / cp950 的 pipe 下 exit 0、報告日誌路徑，且不建目錄也不寫日誌。

    另外跑一次 sys.stdout = None（沒有主控台的服務）確認不會因為 stdout 容錯而崩潰。
    """
    log_dir = os.path.dirname(config.LOG_FILE)
    before = (os.path.exists(log_dir), os.path.exists(config.LOG_FILE))
    for env in ({}, {"PYTHONIOENCODING": "cp950"}):
        e = dict(os.environ)
        e.pop("PYTHONIOENCODING", None)
        e.pop("PYTHONUTF8", None)
        e.update(env)
        r = subprocess.run([sys.executable, "-m", "live"], cwd=REPO_ROOT, env=e,
                           stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
        out = r.stdout.decode("utf-8", "replace")
        where = "env=%s rc=%d\n%s\n%s" % (env, r.returncode, out[-2000:],
                                          r.stderr.decode("ascii", "backslashreplace")[-2000:])
        assert r.returncode == 0, where
        assert "[日誌]" in out and config.LOG_FILE in out, where
    rc, out, err = _run_child(textwrap.dedent("""
        import sys
        sys.path.insert(0, %r)
        from live.__main__ import main
        sys.stdout = None
        main()
    """ % REPO_ROOT))
    assert rc == 0, _describe(rc, out, err)
    assert (os.path.exists(log_dir), os.path.exists(config.LOG_FILE)) == before, \
        "冒煙檢查建了日誌目錄或檔案"


# ============================== AC-6：命名與設定 ==============================
def test_live_module_names_do_not_shadow_stdlib():
    names = [f[:-3] for f in os.listdir(os.path.join(REPO_ROOT, "live"))
             if f.endswith(".py") and not f.startswith("__")]
    assert "logsetup" in names
    clash = [n for n in names if n in sys.stdlib_module_names]
    assert not clash, "live/ 底下的模組跟標準庫同名：%s" % clash


def test_log_params_are_reported_by_execution_params():
    params = config.execution_params()
    assert params["LOG_FILE"] == config.LOG_FILE
    assert params["LOG_LEVEL"] == config.LOG_LEVEL
    assert params["LOG_MAX_BYTES"] == config.LOG_MAX_BYTES
    assert params["LOG_BACKUP_COUNT"] == config.LOG_BACKUP_COUNT
    assert isinstance(logging.getLevelName(config.LOG_LEVEL), int), "LOG_LEVEL 不是有效的等級名稱"
    assert config.LOG_MAX_BYTES > 0 and config.LOG_BACKUP_COUNT > 0, "沒開輪替等於沒有上限"


# ============================== 不用 pytest 也能跑 ==============================
if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS  {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {name}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed} passed, {failed} failed")
    sys.exit(1 if failed else 0)

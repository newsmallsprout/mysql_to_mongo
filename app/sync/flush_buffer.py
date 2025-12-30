# app/sync/flush_buffer.py
import time
import threading
from typing import Dict, List, Tuple, Callable, Optional


class FlushBuffer:
    """
    一个小型缓冲器：
    - add(coll, op)
    - 达到 batch_size 或到时间自动 flush
    - flush 时调用 writer(coll_name, ops)
    """

    def __init__(
        self,
        batch_size: int,
        flush_interval_sec: int,
        writer_func: Callable[[str, List], None],
        on_flush_done: Optional[Callable[[], None]] = None,
        stop_event: Optional[threading.Event] = None,
    ):
        self.batch_size = int(batch_size or 2000)
        self.flush_interval_sec = max(1, int(flush_interval_sec or 2))
        self.writer_func = writer_func
        self.on_flush_done = on_flush_done
        self.stop_event = stop_event or threading.Event()

        self._pending: Dict[str, List] = {}
        self._lock = threading.Lock()
        self._last_flush_ts = time.time()

        self._thread_stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._thread_stop.set()
        if self._thread is not None:
            try:
                self._thread.join(timeout=2)
            except Exception:
                pass
        self.flush(force=True)

    def add(self, coll_name: str, op):
        with self._lock:
            ops = self._pending.setdefault(coll_name, [])
            ops.append(op)

    def size(self, coll_name: str) -> int:
        with self._lock:
            return len(self._pending.get(coll_name, []))

    def flush(self, force: bool = False):
        now = time.time()
        if (not force) and (now - self._last_flush_ts < self.flush_interval_sec):
            return

        with self._lock:
            items: List[Tuple[str, List]] = [(cn, ops[:]) for cn, ops in self._pending.items() if ops]

        if not items:
            self._last_flush_ts = now
            return

        for cn, ops_copy in items:
            self.writer_func(cn, ops_copy)
            with self._lock:
                # 清空原队列
                self._pending.get(cn, []).clear()

        if self.on_flush_done:
            self.on_flush_done()

        self._last_flush_ts = now

    def flush_if_reach_batch(self):
        with self._lock:
            reach = any(len(ops) >= self.batch_size for ops in self._pending.values())
        if reach:
            self.flush(force=True)

    def _loop(self):
        while not self._thread_stop.is_set() and not self.stop_event.is_set():
            time.sleep(self.flush_interval_sec)
            try:
                self.flush(force=True)
            except Exception:
                # 上层会 log，这里不 print
                pass

"""Incremental JSONL output with recovery after an interrupted write."""

import json
import os
import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path


@contextmanager
def jsonl_output(filename, fields, resume):
    path = Path(filename).resolve()
    # SQLite releases the writer lock even when the process is killed.
    with closing(sqlite3.connect(str(path) + ".lock", timeout=0)) as lock:
        try:
            lock.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise RuntimeError(f"Cannot lock output {path}: {exc}") from exc

        completed = {}
        with path.open("a+b" if resume else "w+b") as output:
            if resume:
                output.seek(0)
                while True:
                    position = output.tell()
                    line = output.readline()
                    if not line:
                        break
                    try:
                        row = json.loads(line)
                        key = tuple(row[field] for field in fields)
                        if not all(isinstance(value, str) and value for value in key):
                            raise ValueError("Invalid checkpoint key")
                    except (ValueError, TypeError, KeyError) as exc:
                        if line.endswith(b"\n"):
                            raise ValueError(f"Invalid JSONL record at byte {position} in {path}") from exc
                        # An interruption can leave a partial final line.
                        output.truncate(position)
                        output.flush()
                        os.fsync(output.fileno())
                        break
                    completed[key] = row
                    if not line.endswith(b"\n"):
                        output.write(b"\n")
                        output.flush()
                        os.fsync(output.fileno())

            yield output, completed


def write_result(output, result):
    line = json.dumps(result, ensure_ascii=False) + "\n"
    output.write(line.encode("utf-8"))
    output.flush()
    os.fsync(output.fileno())

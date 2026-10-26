"""可取消、无僵尸进程的异步子进程工具。"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass
class ProcessResult:
    returncode: int
    stdout: str
    stderr: str


async def stop_process(proc: asyncio.subprocess.Process, grace_sec: float = 2.0) -> None:
    """先 terminate，宽限后 kill；把已退出的进程也 wait 掉，避免僵尸。"""
    if proc.returncode is None:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=grace_sec)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
    try:
        await proc.wait()
    except Exception:
        # 清理函数不能在取消/超时路径上再抛出第二个异常。
        pass


async def run_subprocess(cmd, timeout: float) -> ProcessResult:
    """运行命令并在超时/取消时真正终止子进程。

    不使用 shell；stdout/stderr 与旧 subprocess.run(..., text=True) 一样返回 str。
    """
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        # shield 确保清理动作开始并能独立走完；原异常继续向调用方传播。
        await asyncio.shield(stop_process(proc))
        raise
    return ProcessResult(
        returncode=proc.returncode,
        stdout=(stdout or b"").decode("utf-8", errors="replace"),
        stderr=(stderr or b"").decode("utf-8", errors="replace"),
    )

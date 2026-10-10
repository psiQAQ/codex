"""Own Windows descendants before resuming a suspended task process.

Uses documented kernel32 Job Objects and Tool Help thread enumeration. Job
membership survives root-process exit; closing the non-inherited job handle
terminates any remaining owned descendants. No process-name matching is used.
"""

import ctypes
from ctypes import wintypes as w


class BasicLimits(ctypes.Structure):
    _fields_ = [
        ("ProcessTime", ctypes.c_int64),
        ("JobTime", ctypes.c_int64),
        ("Flags", w.DWORD),
        ("MinimumWorkingSet", ctypes.c_size_t),
        ("MaximumWorkingSet", ctypes.c_size_t),
        ("ActiveProcessLimit", w.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", w.DWORD),
        ("SchedulingClass", w.DWORD),
    ]


class ExtendedLimits(ctypes.Structure):
    _fields_ = [
        ("Basic", BasicLimits),
        ("IoCounters", ctypes.c_uint64 * 6),
        ("ProcessMemory", ctypes.c_size_t),
        ("JobMemory", ctypes.c_size_t),
        ("PeakProcessMemory", ctypes.c_size_t),
        ("PeakJobMemory", ctypes.c_size_t),
    ]


class ThreadEntry(ctypes.Structure):
    _fields_ = [
        ("Size", w.DWORD),
        ("Usage", w.DWORD),
        ("ThreadId", w.DWORD),
        ("OwnerProcessId", w.DWORD),
        ("BasePriority", w.LONG),
        ("DeltaPriority", w.LONG),
        ("Flags", w.DWORD),
    ]


class WindowsJob:
    def __init__(self):
        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        declarations = {
            "CreateJobObjectW": ([ctypes.c_void_p, w.LPCWSTR], w.HANDLE),
            "SetInformationJobObject": (
                [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD],
                w.BOOL,
            ),
            "AssignProcessToJobObject": ([w.HANDLE, w.HANDLE], w.BOOL),
            "TerminateJobObject": ([w.HANDLE, w.UINT], w.BOOL),
            "OpenProcess": ([w.DWORD, w.BOOL, w.DWORD], w.HANDLE),
            "OpenThread": ([w.DWORD, w.BOOL, w.DWORD], w.HANDLE),
            "ResumeThread": ([w.HANDLE], w.DWORD),
            "CreateToolhelp32Snapshot": ([w.DWORD, w.DWORD], w.HANDLE),
            "Thread32First": ([w.HANDLE, ctypes.POINTER(ThreadEntry)], w.BOOL),
            "Thread32Next": ([w.HANDLE, ctypes.POINTER(ThreadEntry)], w.BOOL),
            "CloseHandle": ([w.HANDLE], w.BOOL),
        }
        for name, (arguments, result) in declarations.items():
            function = getattr(self.api, name)
            function.argtypes, function.restype = arguments, result
        self.handle = self.api.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = ExtendedLimits()
        limits.Basic.Flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.api.SetInformationJobObject(
            self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)
        ):
            error = ctypes.WinError(ctypes.get_last_error())
            self.api.CloseHandle(self.handle)
            raise error

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.api.CloseHandle(self.handle)

    def attach_and_resume(self, process_id):
        process = self.api.OpenProcess(
            0x0101, False, process_id
        )  # SET_QUOTA | TERMINATE
        if not process:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            if not self.api.AssignProcessToJobObject(self.handle, process):
                raise ctypes.WinError(ctypes.get_last_error())
        finally:
            self.api.CloseHandle(process)
        snapshot = self.api.CreateToolhelp32Snapshot(0x4, 0)  # TH32CS_SNAPTHREAD
        if snapshot == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            entry = ThreadEntry()
            entry.Size = ctypes.sizeof(entry)
            available = self.api.Thread32First(snapshot, ctypes.byref(entry))
            while available:
                if entry.OwnerProcessId == process_id:
                    thread = self.api.OpenThread(
                        0x2, False, entry.ThreadId
                    )  # THREAD_SUSPEND_RESUME
                    if not thread:
                        raise ctypes.WinError(ctypes.get_last_error())
                    try:
                        if self.api.ResumeThread(thread) == 0xFFFFFFFF:
                            raise ctypes.WinError(ctypes.get_last_error())
                    finally:
                        self.api.CloseHandle(thread)
                    return
                available = self.api.Thread32Next(snapshot, ctypes.byref(entry))
            raise OSError("suspended task thread not found")
        finally:
            self.api.CloseHandle(snapshot)

    def terminate(self):
        if not self.api.TerminateJobObject(self.handle, 1):
            raise ctypes.WinError(ctypes.get_last_error())

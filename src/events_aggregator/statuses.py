"""Status values used by the local catalogue and sync worker."""

from enum import StrEnum


class EventStatus(StrEnum):
    NEW = "new"
    PUBLISHED = "published"


class SyncStatus(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

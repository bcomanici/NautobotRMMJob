from nautobot.apps.jobs import register_jobs
from .automox_device_sync_job import SyncAutomoxDevices

register_jobs(SyncAutomoxDevices)

#from nautobot.apps.jobs import register_jobs
#from .automox_device_sync_job import SyncAutomoxDevices

#register_jobs(SyncAutomoxDevices)
from nautobot.apps.jobs import Job, register_jobs

name = "RMM Integrations"

class TestRMMJob(Job):
    class Meta:
        name = "Test RMM Job"

    def run(self):
        self.logger.info("RMM test job loaded.")
        return "OK"

register_jobs(TestRMMJob)

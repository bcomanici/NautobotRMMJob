from nautobot.apps.jobs import Job, register_jobs


class TestRMMJob(Job):
    class Meta:
        name = "Test RMM Job"
        description = "Minimal Git-backed Nautobot Job test."

    def run(self, **kwargs):
        self.logger.info("RMM test job loaded.")
        return "OK"


jobs = (TestRMMJob,)
register_jobs(*jobs)

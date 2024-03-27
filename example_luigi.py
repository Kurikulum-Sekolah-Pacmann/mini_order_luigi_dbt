import subprocess as sp
import luigi
import datetime

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default = datetime.datetime.now())

class dbtDebug(luigi.Task):

    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(f"logs/dbt_debug_log_{self.get_current_timestamp}.log")
    
    def run(self):
        with open(self.output().path, "a") as f:
            p1 = sp.run("cd ./dbt/ && dbt debug",
                        stdout = f,
                        stderr = sp.PIPE,
                        text = True,
                        shell = True,
                        check = True)
            
            if p1.returncode == 0:
                print("Success run dbt debug process")

            else:
                print("Failed to run dbt debug process")
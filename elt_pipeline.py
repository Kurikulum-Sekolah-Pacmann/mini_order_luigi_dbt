import subprocess as sp
import luigi
import datetime
import time
import logging

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default = datetime.datetime.now())

class dbtDebug(luigi.Task):

    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(f"logs/dbt_debug/dbt_debug_logs_{self.get_current_timestamp}.log")
    
    def run(self):
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("cd ./dbt/ && dbt debug",
                            stdout = f,
                            stderr = sp.PIPE,
                            text = True,
                            shell = True,
                            check = True)
                
                if p1.returncode == 0:
                    logging.info("Success Run dbt debug process")

                else:
                    logging.error("Failed to run dbt debug")

            time.sleep(2)
        
        except Exception:
            logging.error("Failed Process")

class dbtDeps(luigi.Task):

    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        return dbtDebug()
    
    def output(self):
        return luigi.LocalTarget(f"logs/dbt_deps/dbt_deps_logs_{self.get_current_timestamp}.log")

    def run(self):
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("cd ./dbt/ && dbt deps",
                            stdout = f,
                            stderr = sp.PIPE,
                            text = True,
                            shell = True,
                            check = True)
                
                if p1.returncode == 0:
                    logging.info("Success installing dependencies")

                else:
                    logging.error("Failed installing dependencies")

            time.sleep(2)

        except Exception:
            logging.error("Failed Process")

class dbtTest(luigi.Task):

    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        return dbtDebug()
    
    def output(self):
        return luigi.LocalTarget(f"logs/dbt_test/dbt_run_logs_{self.get_current_timestamp}.log")

    def run(self):
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("cd ./dbt/ && dbt test",
                            stdout = f,
                            stderr = sp.PIPE,
                            text = True,
                            shell = True,
                            check = True)
                
                if p1.returncode == 0:
                    logging.info("Success running dbt test")

                else:
                    logging.error("Failed running testing")

            time.sleep(2)

        except Exception:
            logging.error("Failed Process")

class dbtRun(luigi.Task):

    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        return dbtTest()
    
    def output(self):
        return luigi.LocalTarget(f"logs/dbt_run/dbt_run_logs_{self.get_current_timestamp}.log")
    
    def run(self):
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("cd ./dbt/ && dbt run",
                            stdout = f,
                            stderr = sp.PIPE,
                            text = True,
                            shell = True,
                            check = True)
                
                if p1.returncode == 0:
                    logging.info("Success running dbt data model")

                else:
                    logging.error("Failed running dbt model")

            time.sleep(2)

        except Exception:
            logging.error("Failed Process")
if __name__ == "__main__":
    luigi.build([dbtDebug(),
                 dbtDeps(),
                 dbtTest(),
                 dbtRun()],
                 local_scheduler = True)
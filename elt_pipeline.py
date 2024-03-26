import subprocess as sp
import luigi
import datetime
import time

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default = datetime.datetime.now())

class dbtDebug(luigi.Task):

    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(f"logs/dbt_debug/dbt_debug_logs_{self.get_current_timestamp}.log")
    
    def run(self):

        with open(self.output().path, "a") as f:
            p1 = sp.run("cd ./dbt/ && dbt debug",
                        stdout = f,
                        stderr = sp.PIPE,
                        text = True,
                        shell = True,
                        check = True)
            
            if p1.returncode == 0:
                print("Success running dbt debug")

            else:
                print("Failed run dbt debug")

        time.sleep(2)

class dbtDeps(luigi.Task):

    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        return dbtDebug()
    
    def output(self):
        return luigi.LocalTarget(f"logs/dbt_deps/dbt_deps_logs_{self.get_current_timestamp}.log")

    def run(self):

        with open(self.output().path, "a") as f:
            p1 = sp.run("cd ./dbt/ && dbt deps",
                        stdout = f,
                        stderr = sp.PIPE,
                        text = True,
                        shell = True,
                        check = True)
            
            if p1.returncode == 0:
                print("Success installing dependencies")

            else:
                print("Failed installing dependencies")

        time.sleep(2)

class dbtTest(luigi.Task):

    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        return dbtDebug()
    
    def output(self):
        return luigi.LocalTarget(f"logs/dbt_test/dbt_run_logs_{self.get_current_timestamp}.log")

    def run(self):
        
        with open(self.output().path, "a") as f:
            p1 = sp.run("cd ./dbt/ && dbt test",
                        stdout = f,
                        stderr = sp.PIPE,
                        text = True,
                        shell = True,
                        check = True)
            
            if p1.returncode == 0:
                print("Success running dbt test")

            else:
                print("Failed running testing")

        time.sleep(2)

class dbtRun(luigi.Task):

    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        return dbtTest()
    
    def output(self):
        return luigi.LocalTarget(f"logs/dbt_run/dbt_run_logs_{self.get_current_timestamp}.log")
    
    def run(self):

        with open(self.output().path, "a") as f:
            p1 = sp.run("cd ./dbt/ && dbt run",
                        stdout = f,
                        stderr = sp.PIPE,
                        text = True,
                        shell = True,
                        check = True)
            
            if p1.returncode == 0:
                print("Success running dbt data model")

            else:
                print("Failed running dbt model")

        time.sleep(2)

if __name__ == "__main__":
    luigi.build([dbtDebug(),
                 dbtDeps(),
                 dbtTest(),
                 dbtRun()])
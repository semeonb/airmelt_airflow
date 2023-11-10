from dbt.cli.main import dbtRunner, dbtRunnerResult


class dbtRunnerWrap(dbtRunner):
    """
    The class is a wrapper for dbtRunner class.
    It is used to run dbt commands from python code.
    """

    def __init__(self, dbt_project_dir, prod=False, *args, **kwargs):
        """
        dbt_project_dir: a RELATIVE path to dbt project directory. For example, "dbt/my_project"
        prod: if True, the target is "prod", otherwise "dev"
        """
        super().__init__(*args, **kwargs)
        self.dbt_project_dir = dbt_project_dir
        if prod:
            self.target = "prod"
        else:
            self.target = "dev"
        self.dbtrunner = super()

    def invoke(self, model_path=None, *args, **kwargs):
        """
        model_path: a RELATIVE path (to the dbt project) to a model directory. For example, "models/applovin"
        """
        results = []
        if model_path:
            invoke_list = [
                "run",
                "--project-dir",
                self.dbt_project_dir,
                "--models",
                model_path,
                "--target",
                self.target,
            ]
        else:
            invoke_list = [
                "run",
                "--project-dir",
                self.dbt_project_dir,
                "--target",
                self.target,
            ]
        res: dbtRunnerResult = self.dbtrunner.invoke(invoke_list, *args, **kwargs)
        for r in res.result:
            results.append(f"{r.node.name}: {r.status}")
        return results

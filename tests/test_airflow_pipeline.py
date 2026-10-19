import unittest

from airflow.dags.lakehouse_daily_pipeline import build_dag


class AirflowPipelineTest(unittest.TestCase):
    def test_build_dag_has_expected_tasks(self):
        dag = build_dag()
        task_ids = list(getattr(dag, "task_ids", []))

        self.assertIn("bootstrap_lakehouse", task_ids)
        self.assertIn("quality_check", task_ids)
        self.assertIn("analytics_snapshot", task_ids)


if __name__ == "__main__":
    unittest.main()

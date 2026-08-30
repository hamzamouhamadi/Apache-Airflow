"""Validate DAG structure without requiring an Airflow installation."""

import importlib.util
import sys
import types
from pathlib import Path


class FakeDAG:
    current = None

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.tasks = {}
        self.edges = set()

    def __enter__(self):
        FakeDAG.current = self
        return self

    def __exit__(self, *_args):
        FakeDAG.current = None


class FakeOperator:
    def __init__(self, task_id, python_callable):
        self.task_id = task_id
        self.python_callable = python_callable
        self.dag = FakeDAG.current
        self.dag.tasks[task_id] = self

    @staticmethod
    def _items(value):
        return value if isinstance(value, list) else [value]

    def __rshift__(self, other):
        for target in self._items(other):
            self.dag.edges.add((self.task_id, target.task_id))
        return other

    def __rrshift__(self, other):
        for source in self._items(other):
            self.dag.edges.add((source.task_id, self.task_id))
        return self


def install_stubs():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG
    operators = types.ModuleType("airflow.operators")
    python_operator = types.ModuleType("airflow.operators.python")
    python_operator.PythonOperator = FakeOperator
    pendulum = types.ModuleType("pendulum")
    pendulum.datetime = lambda *args, **kwargs: (args, kwargs)
    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.operators": operators,
            "airflow.operators.python": python_operator,
            "pendulum": pendulum,
        }
    )


EXPECTED = {
    "mon_premier_dag.py": ("mon_premier_dag", 3, 2),
    "pipeline_big_data_python.py": ("pipeline_big_data_python", 7, 6),
    "pipeline_big_data_parallele.py": ("pipeline_big_data_parallele", 5, 5),
    "pipeline_inscription_etudiants.py": ("pipeline_inscription_etudiants", 7, 7),
}


def main():
    install_stubs()
    dags_dir = Path(__file__).parents[1] / "dags"
    for filename, (dag_id, task_count, edge_count) in EXPECTED.items():
        spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), dags_dir / filename)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        dag = module.dag
        assert dag.dag_id == dag_id
        assert len(dag.tasks) == task_count
        assert len(dag.edges) == edge_count
        print(f"OK {dag_id}: {task_count} tasks, {edge_count} dependencies")


if __name__ == "__main__":
    main()

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

def subdag_factory(parent_dag_name, child_dag_name, start_date, schedule_interval):
    subdag = DAG(dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
                 schedule_interval=schedule_interval,
                 start_date=start_date,
                 catchup=False)
    with subdag:
        # Create 5 dummy operators: subdag_task_0, subdag_task_1,..., subdag_task_4
        dop_list = [DummyOperator(task_id='subdag_task_{0}'.format(i), dag=subdag) for i in range(5)]
        
        # subdag_task_0 >> subdag_task_1 >> subdag_task_2 >> subdag_task_3 >> subdag_task_4
        for i, dop in enumerate(dop_list):
            if i > 0:
                dop_list[i - 1] >> dop
                
    return subdag
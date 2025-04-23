from airflow.decorators import dag, task, task_group
from datetime import datetime

@dag(schedule=None, start_date=datetime(2022, 3, 4), catchup=False)
def dynamic_task_group_example():
    
    @task_group(group_id="group1")
    def tg1():
        @task
        def process_param():
            return ["A", "B", "C"]

        @task
        def process_param2():
            return ["1", "2", "3"]

        processed_params = process_param()
        processed_params2 = process_param2()
        return processed_params2  # Output used for next group

    @task_group(group_id="group2")
    def tg2(param):
        @task
        def process(param):
            return f"Processed {param}"

        process(param)

    processed_params2 = tg1()
    tg2.expand(param=processed_params2)  # Dynamically expand tasks in group2

dynamic_task_group_example()

from airflow.sdk import Asset, dag, task


@dag(schedule=[Asset("my_report")])
def downstream_dag():

    @task
    def my_task():
        pass

    my_task()


downstream_dag()

from airflow.sdk import dag, task, chain


@dag(tags=["syntax_examples"])
def dag_versioning_example():

    @task
    def print_hello():
        print("Hello")

    @task
    def print_world():
        print("Hello, Airflow!")

    chain(
        print_hello(),
        print_world()
    )


dag_versioning_example()

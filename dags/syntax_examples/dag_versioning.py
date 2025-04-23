from airflow.sdk import dag, task, chain


@dag(tags=["syntax_examples"])
def dag_versioning_example():

    @task
    def print_hello():
        print("Hello")

    @task
    def print_world():
        print("Hello, Airflow!")

    # @task 
    # def my_other_task():
    #     print("My other task!")

    # @task 
    # def new_task():
    #     print("New task!")

    chain(
        print_hello(),
        print_world(),
        # my_other_task(),
        # new_task(),
    )


dag_versioning_example()

"""
## Example of a complex DAG structure

This DAG demonstrates how to set up a complex structures including:
- Branches with Labels
- Task Groups
- Dynamically mapped tasks
- Dynamically mapped task groups

The tasks themselves are empty or simple bash statements.
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, Label, chain, chain_linear, dag, task, task_group
from pendulum import datetime

# Define the DAG
@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    tags=["ui_example"],
    params={"fail_task": False},
)
def ui_showcase_dag():

    start = EmptyOperator(task_id="start")

    # Dynamically mapped tasks: .partial() contains all parameters that stay the same
    # between mapped task instances. .expand() contains the parameters that change
    # one mapped task instance will be created for each value in the list provided to `bash_command`
    sales_data_extract = BashOperator.partial(task_id="sales_data_extract").expand(
        bash_command=["echo 1", "echo 2", "echo 3", "echo 4"]
    )
    internal_api_extract = BashOperator.partial(task_id="internal_api_extract").expand(
        bash_command=["echo 1", "echo 2", "echo 3", "echo 4"]
    )

    # Branch task that picks which branch to follow
    @task.branch
    def determine_load_type() -> str:
        """Randomly choose a branch. The return value is the task_id of the branch to follow."""
        import random



        if random.choice([True, False]):
            return "internal_api_load_full"
        return "internal_api_load_incremental"

    sales_data_transform = EmptyOperator(task_id="sales_data_transform")

    # When using the TaskFlow API it is common to assign the called task to an object
    # to use in several dependency definitions without creating several instances of the same task
    determine_load_type_obj = determine_load_type()

    sales_data_load = EmptyOperator(task_id="sales_data_load")
    internal_api_load_full = EmptyOperator(task_id="internal_api_load_full")
    internal_api_load_incremental = EmptyOperator(
        task_id="internal_api_load_incremental"
    )

    # defining a task group that task a parameter (a) to allow for dynamic task group mapping
    @task_group
    def sales_data_reporting(a):
        # the trigger_rule of the first task in the task group can be set to "all_done"
        # to ensure that it runs even if one of the upstream tasks (`internal_api_load_full`)
        # is skipped due to the branch task
        prepare_report = EmptyOperator(
            task_id="prepare_report", trigger_rule="all_done"
        )
        publish_report = EmptyOperator(task_id="publish_report")

        # setting dependencies within the task group
        chain(prepare_report, publish_report)

    # dynamically mapping the task group with `a` being the mapped parameter
    # one task group instance will be created for each value in the list provided to `a`
    sales_data_reporting_obj = sales_data_reporting.expand(a=[1, 2, 3, 4, 5, 6])

    # defining a task group that does not use any additional parameters
    @task_group
    def cre_integration():
        # the trigger_rule of the first task in the task group can be set to "all_done"
        # to ensure that it runs even if one of the upstream tasks (`internal_api_load_full`)
        # is skipped due to the branch task
        cre_extract = EmptyOperator(task_id="cre_extract", trigger_rule="all_done")
        cre_transform = EmptyOperator(task_id="cre_transform")
        cre_load = EmptyOperator(task_id="cre_load")

        # setting dependencies within the task group
        chain(cre_extract, cre_transform, cre_load)

    # calling the task group to instantiate it and assigning it to an object
    # to use in dependency definitions
    cre_integration_obj = cre_integration()

    @task_group
    def mlops():
        # the trigger_rule of the first task in the task group can be set to "all_done"
        # to ensure that it runs even if one of the upstream tasks (`internal_api_load_incremental`)
        # is skipped due to the branch task
        set_up_cluster = EmptyOperator(
            task_id="set_up_cluster", trigger_rule="all_done"
        )
        # the outlets parameter is used to define which datasets are updated by this task
        train_model = EmptyOperator(task_id="train_model")
        tear_down_cluster = EmptyOperator(task_id="tear_down_cluster")

        # setting dependencies within the task group
        chain(set_up_cluster, train_model, tear_down_cluster)

        # turning the `tear_down_cluster`` task into a teardown task and set
        # the `set_up_cluster` task as the associated setup task
        tear_down_cluster.as_teardown(setups=set_up_cluster)

    mlops_obj = mlops()

    @task(outlets=[Asset("my_report")])
    def _end(**context):
        if context["params"]["fail_task"]:
            print(10/0)
        pass

    end = _end()

    # --------------------- #
    # Defining dependencies #
    # --------------------- #

    chain(
        start,
        sales_data_extract,
        sales_data_transform,
        sales_data_load,
        [sales_data_reporting_obj, cre_integration_obj],
        end,
    )
    chain(
        start,
        internal_api_extract,
        determine_load_type_obj,
        [internal_api_load_full, internal_api_load_incremental],
        mlops_obj,
        end,
    )

    chain_linear(
        [sales_data_load, internal_api_load_full],
        [sales_data_reporting_obj, cre_integration_obj],
    )

    # Adding labels to two edges
    chain(
        determine_load_type_obj, Label("additional data"), internal_api_load_incremental
    )
    chain(
        determine_load_type_obj, Label("changed existing data"), internal_api_load_full
    )


# Calling the DAG function will instantiate the DAG
ui_showcase_dag()
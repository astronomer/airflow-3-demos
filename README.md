# Optimizing your Airflow Developer Experience: Exploring DAG Versioning and Backfills 

This repository contains the example dags for the [Optimizing your Airflow Developer Experience: Exploring DAG Versioning and Backfills](https://www.astronomer.io/events/webinars/apache-airflow-3-optimize-your-pipeline-developer-experience-video) webinar.

## How to run this demo

1. Fork and clone this repository.
2. Make sure you have the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) installed and are at least on version 1.34.0. You can upgrade with `brew upgrade astro`.
3. To run the full demo DAGs you will need an [OpenAI API key](https://platform.openai.com/docs/overview), copy the .env_example file to .env and add your OpenAI API key to the file. For the syntax examples you don't need any API keys or connections.
4. Run `astro dev start` to start the local Airflow instance.
5. Enjoy Airflow 3.0!

## Contents

- [Asset graph showcase](/dags/asset_graph_showcase/): toy dags creating a complex asset graph, tagged with `asset_example_complex`
- [Simple asset example](/dags/other_dags/asset_example_simple.py): simple 3 asset toy pipeline, tagged with `asset_example_simple`
- [Example for passing data between assets](/dags/other_dags/asset_example_passing_data.py): example of passing data between assets, tagged with `asset_example_passing_data`
- [Dag versioning example](/dags/dag_versioning_example.py): simple dag to show dag versioning in the webinar 
- [UI showcase](/dags/ui_showcase_dag.py): dag to show the UI features in the webinar
- [Backfill example 1](/dags/backfill_example_1.py) and [Backfill example 2](/dags/backfill_example_2.py): dag to show backfill features in the webinar

The [.env_example](.env_example) file contains the configuration to set up a GitDagBundle. 

## Learn more:

You can learn more about the new features in the [Practical Guide to Apache Airflow 3 ebook](https://www.astronomer.io/ebooks/practical-guide-to-apache-airflow-3/) and our [Learn guides](https://www.astronomer.io/docs/learn/):

- [DAG versioning and DAG bundles](https://www.astronomer.io/docs/learn/airflow-dag-versioning)
- [Backfills](https://www.astronomer.io/docs/learn/rerunning-dags/#backfill)
- [@asset](https://www.astronomer.io/docs/learn/airflow-datasets)
- [Event-driven scheduling](https://www.astronomer.io/docs/learn/airflow-event-driven-scheduling)
- [Airflow 3 architecture](https://www.astronomer.io/docs/learn/airflow-components)

To learn how to upgrade from Airflow 2 to Airflow 3, check out the [upgrade guide](https://www.astronomer.io/docs/learn/airflow-upgrade-2-3). Check out the [release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html) for a full list of new features improvements and changes.

# Webinar demo

This repository contains the demo code for the [Introducing Apache Airflow® 3.0 webinar](https://www.astronomer.io/events/webinars/introducing-apache-airflow-3-0-video/). 

## Personalized Newsletter pipeline

![Personalized newsletter pipeline architecture diagram](/src/personalized_newsletter_architecture_diagram.png)

This pipelines hows:

- `@asset` syntax
- data-aware scheduling
- event-driven scheduling and inference execution
- dynamic task mapping

## Syntax examples

The [`dags/syntax_examples`](dags/syntax_examples) folder contains simple dags showing Airflow 3.0 features. 

## How to run this demo

1. Fork and clone this repository.
2. Make sure you have the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) installed and are at least on version 1.34.0. You can upgrade with `brew upgrade astro`.
3. To run the full demo DAGs you will need an [OpenAI API key](https://platform.openai.com/docs/overview), copy the .env_example file to .env and add your OpenAI API key to the file. For the syntax examples you don't need any API keys or connections.
4. Run `astro dev start` to start the local Airflow instance.
5. Enjoy Airflow 3.0!

You can learn more about the new features in our [Learn guides](https://www.astronomer.io/docs/learn/):

- [DAG versioning](https://www.astronomer.io/docs/learn/airflow-dag-versioning)
- [@asset](https://www.astronomer.io/docs/learn/airflow-datasets)
- [Event-driven scheduling](https://www.astronomer.io/docs/learn/airflow-event-driven-scheduling)
- [Airflow 3 architecture](https://www.astronomer.io/docs/learn/airflow-components)

To learn how to upgrade from Airflow 2 to Airflow 3, check out the [upgrade guide](https://www.astronomer.io/docs/learn/airflow-upgrade-2-3). Check out the [release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html) for a full list of new features improvements and changes.
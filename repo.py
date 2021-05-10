import csv

import requests
from dagster import pipeline, solid, repository


@solid
def download_cereals(_):
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]


@solid
def find_highest_calorie_cereal(_, cereals):
    sorted_cereals = list(
        sorted(cereals, key=lambda cereal: cereal["calories"])
    )
    return sorted_cereals[-1]["name"]

@solid
def display_results(context, most_calories):
    context.log.info(f"Most caloric cereal: {most_calories}")


@pipeline
def complex_pipeline():
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals)
    )

@repository
def deploy_docker_repository_user_code():
    return [complex_pipeline]

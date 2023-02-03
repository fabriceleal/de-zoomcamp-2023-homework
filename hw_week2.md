# homework week 2

Check sources [etl_web_to_gcs.py](hw_week2/etl_web_to_gcs.py) and [etl_gcs_to_bq.py](hw_week2/etl_gcs_to_bq.py).

## Question 1

```shell

prefect deployment build etl_web_to_gcs.py:etl_web_to_gcs_par -n etl_web_to_gcs_par -a
prefect deployment run etl-web-to-gcs-par/etl_web_to_gcs_par -p "month=1" -p "year=2020" -p "color=green"

```

Answer: 447770 rows

## Question 2

```shell
prefect deployment build etl_web_to_gcs.py:etl_web_to_gcs -n etl_web_to_gcs_croned --cron "0 5 1 * *" -a
```

Answer: "0 5 1 * *" 

## Question 3

```shell
prefect deployment run etl-web-to-gcs-par/etl_web_to_gcs_par -p "month=2" -p "year=2019" -p "color=yellow"
prefect deployment run etl-web-to-gcs-par/etl_web_to_gcs_par -p "month=3" -p "year=2019" -p "color=yellow"


prefect deployment build etl_gcs_to_bq.py:etl_gcs_to_bq_par -n etl_gcs_to_bq_par -a
prefect deployment run etl-gcs-to-bq-par/etl_gcs_to_bq_par -p "month=2" -p "year=2019" -p "color=yellow"
prefect deployment run etl-gcs-to-bq-par/etl_gcs_to_bq_par -p "month=3" -p "year=2019" -p "color=yellow"

```

Answer: 14851920

## Question 4

Fetch the flows from the repository

```python
from prefect.filesystems import GitHub
github_block = GitHub.load("github-zoom")
github_block.get_directory(from_path="hw_week2", local_path=".")

```

```shell
prefect deployment build -n githubdeploy -sb github/github-zoom hw_week2/etl_web_to_gcs.py:etl_web_to_gcs_par -a

prefect deployment run etl-web-to-gcs-par/githubdeploy -p "month=11" -p "year=2020" -p "color=green"
```

Answer: 88605

## Question 5

Created/tested through `prefect orion` + slack webhooks

Answer: 514392

## Question 6

Created/tested through the UI

Answer: 8

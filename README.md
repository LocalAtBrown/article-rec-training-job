# article-rec-training-job

Job that runs every two hours to create a new batch of article recommendations, using the latest Google Analytics data available.

## Dev Usage
1. Build the container
```
kar build
```

2. Run the job
```
kar run
```

3. Or, run bash in the container
```
kar run bash
```

## Environment
The following parameters are defined in `env.json`:

* `LOG_LEVEL` (str): log level above which logs will be displayed (_e.g. "INFO"_)
* `SERVICE` (str):  name of the service ("article-rec-training-job")
* `DB_NAME` (str): [AWS SSM](https://docs.aws.amazon.com/systems-manager/latest/userguide/ssm-agent.html) path to the database name secret (_e.g. "/dev/article-rec-db/name"_)
* `DB_PASSWORD` (str): path to the database password secret (_e.g. "/dev/database/password"_)
* `DB_USER` (str): path to the database user secret (_e.g. "/dev/database/user"_)
* `DB_HOST` (str): path to the database host screw (_e.g. "/dev/database/host"_)
* `GA_DATA_BUCKET` (str): bucket where analytics data is being stored (_e.g. "lnl-snowplow-washington-city-paper"_)
* `SAVE_FIGURES` (boolean):  whether or not to output a figure at the filter users step (_e.g. false_)
* `DISPLAY_PROGRESS` (boolean): whether or not to display training progress in logs (_e.g. false_)
* `MAX_RECS` (int): How many recommendations to save to the database for each article (_e.g. 20_)
* `DAYS_OF_DATA` (int): How many days worth of data to fetch and train on (_e.g. 28_)

To select which parameters to use, you must set `STAGE` to the corresponding key in your development environment:

```
export STAGE=local
```

## Testing
The recommendation module uses two testing suites: [`unittest`](https://docs.python.org/3/library/unittest.html) and [`pytest`](https://docs.pytest.org/en/6.2.x/). To test, after running bash in the container:

```
python -m unittest tests/test_save_defaults.py
pytest tests
```

## Infrastructure-as-Code
The infrastructure for this project is defined as code using the [AWS Cloud Development Kit](https://aws.amazon.com/cdk/). Each pull request to main will trigger a new deployment to production when merged (`ArticleRecTrainingJobPipeline` defines the AWS Code Pipeline that deploys the `ArticleRecTrainingJob` Cloud Formation stack).

The components of the stack are defined in `cdk/lib` and are as follows:
* ECS Fargate Task (set to have 2048 vCPUs and 8192 MiB at time of writing)
* Scheduled Event Rule (set to run every hour)
* Log Group (set to retain logs for 1 month)
* Associated IAM roles and policies
* (The database used to store recommendations is defined in the [infrastructure repo](https://github.com/LocalAtBrown/infrastructure/tree/main/lib/databases))

For dev deployment, run:

```
cd cdk; cdk deploy DevArticleRecTrainingJob
```

Then visit the corresponding logs using the links below to track job progress.

###

## Monitoring

### Logs
- [Dev](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/DevArticleRecTrainingJob-DevArticleRecTrainingJobTaskDefinitionDevArticleRecTrainingJobTaskContainerLogGroup9A13F6F1-5dyoUd3VPezx)
- [Prod](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/ArticleRecTrainingJob-ArticleRecTrainingJobTaskDefinitionArticleRecTrainingJobTaskContainerLogGroup2D7DFB71-xD2hRJTbp6vc)

### Dashboards
- [Dev](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=dev-article-rec-training-job;start=PT24H)
- [Prod](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=article-rec-training-job;start=PT24H)

## Pipeline Components
Data pulled in from S3 goes through several preprocessing steps before being trained on, with outputs saved to the database. The full pipeline consists of the following steps, executed in order:

1. `create_model` (`db/helpers.py`): Creates and saves a model object to the database.
2. `fetch_data` (`job/steps/fetch_data.py`): Downloads raw data spanning "days of data," using s3 select on
- `collector_tstamp` time collector received tracking data
- `page_urlpath` path to page being browsed
- `contexts_dev_amp_snowplow_amp_id_1` JSON object containing client ID
Then Transform above raw data:
- `contexts_dev_amp_snowplow_amp_id_1` JSON parsed into `client_id`
- `collector_tstamp` converted to pandas datetime objects `activity_time` and `session_date`
- `page_urlpath` renamed to `landing_page_path`
- `event_category` and `event_action` set as default to "impression" and "snowplow_amp_page_ping"
3. `scrape_metadata` (`job/steps/scrape_metadata.py`): Given a website format (e.g. `Sites.WCP`), scrapes unique `landing_page_path`'s from during step one, and returns DataFrame containing:
- `article_id` the ID of the article in the DB (int)
- `external_id` the ID of the article on client website (WCP) (int)
- `published_at` the time article was published as pandas datetime obj
- `landing_page_path`
4. `filter_flyby_users` (`job/steps/common_preprocessing.py`): Removes any activities by users who only visited one article.
5. `filter_articles` (`job/steps/common_preprocessing.py`): Removes any activities on articles visited by only one user. Then, helper iteratively calls steps 4 and 5 until all users and articles have at least two interactions with unique instances of each other.
6. `fix_dtypes` (`job/steps/common_preprocessing.py`): Fills empty `event_action` and `event_category` fields with defaults from Step 2, and sets `activity_time` and `session_date` to datetime types
7. `time_activities` (`job/steps/common_preprocessing.py`): Calculates dwell time as a difference between subsequent `activity_time` on the same `landing_page_path` by the same `client_id`. It doesn't use any session logic.
8. `filter_activities` (`job/steps/common_preprocessing.py`):
Due to how dwell time is calculated, some activities will have long dwell times corresponding to periods of inactivity. We throw these out when dwell time is above 10 minutes:

![https://s3-us-west-2.amazonaws.com/secure.notion-static.com/5ab4c108-9e9a-4b25-b94b-a46743b8304e/Untitled.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/5ab4c108-9e9a-4b25-b94b-a46743b8304e/Untitled.png)

9. `calculate_defaults` (`job/steps/save_defaults.py`): Find the articles with the highest (statistically significant) dwell time per pageview.
10. `get_weights` (`job/helpers.py`): Weight with exponential publish time decay to ensure default recommendations tend more recently.
11. `save_defaults` (`job/steps/save_defaults.py`): Save default recs to the database.
12. `aggregate_times` (`job/steps/model_preprocessing.py`): Transform aggregated dwell time into a sparse user by article matrix.
13. `train_model` (`job/steps/train_model.py`): Run matrix factorization on sparse matrix and return resulting article and user vectors.
14. `save_predictions` (`job/steps/save_predictions.py`): Calculate distances between article vectors, and save `MAX_RECS` closest articles for each article into database (as recommendations).

At each key step, the latest inputs and outputs are saved to S3 bucket `` using `` (`job/helpers.py`).

## Other Repositories
* [`infrastructure`](https://github.com/LocalAtBrown/article-rec-api): The database used to store recommendations is defined here.
* [`article-rec-api`](https://github.com/LocalAtBrown/article-rec-api): Calls to the API created by this repository return article recommendations and model versions saved by the training pipeline. The API is used to surface recommendations on the front-end.
* [`article-rec-offline-dashboard`](https://github.com/LocalAtBrown/article-rec-offline-dashboard): The training job can be monitored using the notebook in this repository, which employs a similar preprocessing pipeline.
* [`snowplow-analytics`](https://github.com/LocalAtBrown/snowplow-analytics): The analytics pipeline used to collect user clickstream data into the `GA_DATA_BUCKET` is defined in this repository. 
* [`article-recommendations`](https://github.com/LocalAtBrown/article-recommendations): The recommendations are displayed on WordPress [NewsPack](https://newspack.pub/) sites using the PHP widget defined in this repositiory.
# article-rec-training-job

Job that runs every two hours to create a new batch of article recommendations, using the latest Google Analytics data available.

* TOC
{:toc}

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
Environment parameters are defined in `env.json`


## Testing
The recommendation module uses two testing suites: [`unittest`](https://docs.python.org/3/library/unittest.html) and [`pytest`](https://docs.pytest.org/en/6.2.x/). To test, after running bash in the container:

```
pytest tests
```

(pytest should pick up all tests regardless of testing suite and run them, according to [this post](https://docs.pytest.org/en/6.2.x/unittest.html#:~:text=pytest%20supports%20running%20Python%20unittest,full%20advantage%20of%20pytest's%20features):

> pytest will automatically collect unittest.TestCase subclasses and their test methods in test_*.py or *_test.py files.)

## Deploying
The infrastructure for this project is defined as code using the [AWS Cloud Development Kit](https://aws.amazon.com/cdk/). Each pull request to main will trigger a new deployment to production when merged (`ArticleRecTrainingJobPipeline` defines the AWS Code Pipeline that deploys the `ArticleRecTrainingJob` Cloud Formation stack).

The components of the stack are defined in `cdk/lib` and are as follows:
* ECS Fargate Task
* Scheduled Event Rule
* Log Group
* Associated IAM roles and policies
* (The database used to store recommendations is defined in the [infrastructure repo](https://github.com/LocalAtBrown/infrastructure/tree/main/lib/databases))

For dev deployment, run:

```
cd cdk; cdk deploy DevArticleRecTrainingJob
```

Then visit the corresponding logs using the links in the [Monitoring](https://github.com/LocalAtBrown/article-rec-training-job#monitoring) section.

## Monitoring

### Logs
- [Dev](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/DevArticleRecTrainingJob-DevArticleRecTrainingJobTaskDefinitionDevArticleRecTrainingJobTaskContainerLogGroup9A13F6F1-5dyoUd3VPezx)
- [Prod](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/ArticleRecTrainingJob-ArticleRecTrainingJobTaskDefinitionArticleRecTrainingJobTaskContainerLogGroup2D7DFB71-xD2hRJTbp6vc)

### Dashboards
- [Dev](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=dev-article-rec-training-job;start=PT24H)
- [Prod](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=article-rec-training-job;start=PT24H)

## Pipeline Components
Data pulled in from the S3 bucket (as defined in `env.json`) goes through several preprocessing steps before being trained on. The key steps of the job pipeline are defined in the `job/steps` folder. Output predictions are saved to the RDS database (see [Other Repositories](https://github.com/LocalAtBrown/article-rec-training-job#other-repositories)). 

## Other Repositories
* [`infrastructure`](https://github.com/LocalAtBrown/article-rec-api): The database used to store recommendations is defined here.
* [`article-rec-api`](https://github.com/LocalAtBrown/article-rec-api): Calls to the API created by this repository return article recommendations and model versions saved by the training pipeline. The API is used to surface recommendations on the front-end.
* [`article-rec-offline-dashboard`](https://github.com/LocalAtBrown/article-rec-offline-dashboard): The training job can be monitored using the notebook in this repository, which employs a similar preprocessing pipeline.
* [`snowplow-analytics`](https://github.com/LocalAtBrown/snowplow-analytics): The analytics pipeline used to collect user clickstream data into the `GA_DATA_BUCKET` is defined in this repository. 
* [`article-recommendations`](https://github.com/LocalAtBrown/article-recommendations): The recommendations are displayed on WordPress [NewsPack](https://newspack.pub/) sites using the PHP widget defined in this repositiory.
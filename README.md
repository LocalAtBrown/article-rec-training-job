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

## Monitoring

### Logs
- [Dev](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/DevArticleRecTrainingJob-DevArticleRecTrainingJobTaskDefinitionDevArticleRecTrainingJobTaskContainerLogGroup9A13F6F1-5dyoUd3VPezx)
- [Prod](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/ArticleRecTrainingJob-ArticleRecTrainingJobTaskDefinitionArticleRecTrainingJobTaskContainerLogGroup2D7DFB71-xD2hRJTbp6vc)

### Dashboards
- Dev
- Prod

## Migrations

### Adding a Migration
Add a sql file to the migrations folder. The file must be prefixed with the next available version number and two (2) underscores, ie: `V0002__`.

### Running a Migration
1. Ensure you have flyway installed by running
```
brew bundle --file=Brewfile
```

2. Retrieve the host, port, dbname, username, and password from [AWS Secrets Manager](https://console.aws.amazon.com/secretsmanager/home?region=us-east-1#/listSecrets).

3. Check the pending migrations by running:
```
flyway info -url=jdbc:postgresql://<HOST>:5432/<DBNAME> -user=localnewslab -password=<PASSWORD> -locations=filesystem:db/migrations
```

4. Run the pending migrations:
```
flyway migrate -url=jdbc:postgresql://<HOST>:5432/<DBNAME> -user=localnewslab -password=<PASSWORD> -locations=filesystem:db/migrations
```


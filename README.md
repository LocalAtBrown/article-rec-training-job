# article-rec-training-job

Job that runs every two hours to create a new batch of article recommendations, using the latest Google Analytics data available.

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
flyway info -url=jdbc:postgresql://<HOST>:<PORT>/<DBNAME> -user=<USERNAME> -password=<PASSWORD> -locations=filesystem:db/migrations
```

4. Run the pending migrations:
```
flyway migrate -url=jdbc:postgresql://<HOST>:<PORT>/<DBNAME> -user=<USERNAME> -password=<PASSWORD> -locations=filesystem:db/migrations
```

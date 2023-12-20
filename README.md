# article-rec-training-job

Job to update databases and train models for the Local News Lab's article recommendation system.

TODO: This diagram still holds for the most part, but should update it.
![architecture diagram](docs/images/arch-diagram.png)

## Directory Layout

```
.
├── app.py                          # contains the main function to be run by the job
├── scripts/                        # TODO: contains scripts to be run independently of the job (e.g., for backfills)
├── article_rec_training_job
    ├── components/                 # contains reusable components to be used by tasks
    ├── tasks/                      # contains all tasks to be run by the job
    ├── config.py                   # contains all job configuration variables
    ├── shared/                     # contains shared code between tasks and components
```

### A Word on Tasks and Components

One job run executes one or more tasks. One task consists of one or more components, each representing some logical step within the task's execution logic.

Tasks are designed to be run independently of one another. For example, the `UpdatePages` task should be run independently of the `CreateRecommendations` task because the latter can and should still be run even if the former fails. Sometimes, it's nice to impose a sequence of task execution when more than one tasks are executed in a single job run. For example, it'd be nice to run the `UpdatePages` task before the `CreateRecommendations` task because the latter would have access to the most up-to-date data.

Components are designed to be reusable across tasks. For example, the `UpdatePages` task and the `CreateRecommendations` task both need to fetch site traffic data, so they both use the `EventFetcher` component (more precisely, any component that implements the `EventFetcher` protocol). Within a task, a component depends on the components precending it in the task execution and will fail if any of those components fails.

Tasks and components are designed to be very loosely coupled to one another for the sake of modularity. One task includes one more multiple components via composition, which facilites a "plug-and-play" pattern where components can be swapped out for others to fit the configuration of the specific site the job is being run on.

The `article_rec_training_job.shared` module houses stuff that is shared between tasks and components, such as type definitions, data schemas, and `helper` utilities.

Finally, this tasks-components design is a formalization of how the training job was originally conceived. It works for now, but as soon as things become more complicated, it might be worth considering breaking the job into multiple standalone functions and transition to workflow orchestration tools like Apache Airflow. That tasks are loosely coupled from one another and from components means breaking them up should be easy :)

## Development Tools

We use [Poetry](https://python-poetry.org/) to manage dependencies. It also helps with pinning dependency and python
versions. We also use [pre-commit](https://pre-commit.com/) with hooks for [isort](https://pycqa.github.io/isort/),
[black](https://github.com/psf/black), and [flake8](https://flake8.pycqa.org/en/latest/) for consistent code style and
readability. Note that this means code that doesn't meet the rules will fail to commit until it is fixed.

We also use [mypy](https://mypy.readthedocs.io/en/stable/index.html) for static type checking. This can be run manually,
and the CI runs it on PRs.

### Setup

1. [Install Poetry](https://python-poetry.org/docs/#installation).
2. Run `poetry install --no-root`
3. Make sure the virtual environment is active, then
4. Run `pre-commit install`

You're all set up! Your local environment should include all dependencies, including dev dependencies like `black`.
This is done with Poetry via the `poetry.lock` file. As for the containerized code, that still pulls dependencies from
`requirements.txt`. Any containerized dependency requirements need to be updated in `pyproject.toml` then exported to
`requirements.txt`.

### Run Code Format and Linting

To manually run isort, black, and flake8 all in one go, simply run `pre-commit run --all-files`.

### Run Static Type Checking

To manually run mypy, simply run `mypy` from the root directory of the project. It will use the default configuration
specified in the mypy.ini file.

### Update Dependencies

To update dependencies in your local environment, make changes to the `pyproject.toml` file then run `poetry update`.
To update `requirements.txt` for the container, run `poetry export -o requirements.txt --without-hashes`. The pre-commit
hook also automatically re-creates the `requirements.txt` file if it detects changes in `pyproject.toml`.

## Local Usage

```
python app.py
```

## Running Tests

Make sure you have Docker Compose installed. Then, run:

```
poe test
```

to run all tests, including integration tests. (Poe is a task runner that works well with Poetry. Run `poe -h` and refer to `pyproject.toml` for information on all available commands.)

## Running Backfills

TODO

## Deploying

For dev deployment, run:

```
kar deploy
```

Each pull request to main will trigger a new prod deployment when merged.

## For LNL

### Monitoring

#### Logs

TODO

#### System Dashboards

TODO

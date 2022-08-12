# Execute Any Operator CLI

The `execute-any-operator` command is used to arbitrarily run any supported Airflow operator without the need to run Airflow services.

## Requirements

1. Docker
2. Python 3

## Building the Docker Image

Building the Docker image is simple, just execute the following command in the root of the repository:

```bash
docker build -t execute-any-operator .
```

This will create a Docker image tagged with `execute-any-operator` which includes the CLI tool and other required code.

## Adding New Operators

To add new operators to the CLI tool, all that's required is to create a new subcommand that initializes the `ExecuteAnyOperator` and calls `.execute()`. Another important piece to adding a new operator is to include all of the initialization parameters that you might need. Please refer to the example below:

### Implementing the

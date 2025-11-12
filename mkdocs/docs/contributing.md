---
hide:
  - navigation
---

<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Contributing

We welcome contributions to Apache Iceberg! To learn more about contributing to Apache Iceberg, please refer to the [official Iceberg contribution guidelines](https://iceberg.apache.org/contribute/). These guidelines are intended as helpful suggestions to make the contribution process as seamless as possible, and are not strict rules.

If you would like to discuss your proposed change before contributing, we encourage you to visit our [Community](https://iceberg.apache.org/community/) page. There, you will find various ways to connect with the community, including Slack and our mailing lists. Alternatively, you can open a [new issue](https://github.com/apache/iceberg-python/issues) directly in the GitHub repository.

For first-time contributors, feel free to check out our [good first issues](https://github.com/apache/iceberg-python/issues/?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22) for an easy way to get started.

## Contributing to PyIceberg

The PyIceberg Project is hosted on GitHub at <https://github.com/apache/iceberg-python>.

For development, [uv](https://docs.astral.sh/uv/) is used for dependency management and packaging. uv is a Python package installer and resolver, written in Rust, that serves as a drop-in replacement for pip, and virtualenv.

### Getting Started

Install uv and set up the development environment:

```bash
make install
```

This will install uv if needed, create a virtual environment in `.venv`, and install all dependencies.

If you only want to just install uv:

```bash
make install-uv
```

### Python Version Selection

You can specify which Python version to use when creating your virtual environment:

```bash
PYTHON=3.12 make install # Create environment with Python 3.12
make test # Run tests against Python 3.12
```

> **Tip:** `uv python list` shows available interpreters. `uv python install 3.12` can install one if needed.

### IDE Setup

After running `make install`, configure your IDE to use the Python interpreter at `.venv/bin/python`.

**To set up IDEA with uv:**

- Open up the Python project in IntelliJ
- Make sure that you're on latest main
- Go to File -> Project Structure (⌘;)
- Go to Platform Settings -> SDKs
- Add Python SDK -> Virtualenv Environment -> Existing environment
- Point to `.venv/bin/python`

**VS Code:**

- Press Cmd/Ctrl+Shift+P -> "Python: Select Interpreter"
- Choose `.venv/bin/python`

### Advanced uv Usage

For full control over your environment, you can use uv commands directly. See the [uv documentation](https://docs.astral.sh/uv/) to learn more about:

- Managing dependencies with `uv add` and `uv remove`
- Python version management with `uv python`
- Running commands with `uv run`
- Lock file management with `uv.lock`

## Installation from source

Clone the repository for local development:

```sh
git clone https://github.com/apache/iceberg-python.git
cd iceberg-python
pip3 install -e ".[s3fs,hive]"
```

Install it directly for GitHub (not recommended), but sometimes handy:

```shell
pip install "git+https://github.com/apache/iceberg-python.git#egg=pyiceberg[pyarrow]"
```

## Linting

`prek` is used for autoformatting and linting:

```bash
make lint
```

`prek` will automatically fix the violations such as import orders, formatting etc. Pylint errors you need to fix yourself.

In addition to manually running `make lint`, you can install the pre-commit hooks in your local repo with `prek install`. By doing this, linting is run automatically every time you make a commit.

You can bump the integrations to the latest version using `prek auto-update`. This will check if there is a newer version of `{black,mypy,isort,...}` and update the yaml.

## Cleaning

Removal of old cached files generated during the Cython build process:

```bash
make clean
```

Helps prevent build failures and unexpected behavior by removing outdated files, ensuring that only up-to-date sources are used & the build environment is always clean.

## Testing

For Python, `pytest` is used a testing framework in combination with `coverage` to enforce 90%+ code coverage.

```bash
make test
```

By default, S3 and ADLS tests are ignored because that require minio and azurite to be running.
To run the S3 suite:

```bash
make test-s3
```

To run the ADLS suite:

```bash
make test-adls
```

To pass additional arguments to pytest, you can use `PYTEST_ARGS`.

### Run pytest in verbose mode

```sh
make test PYTEST_ARGS="-v"
```

### Run pytest with pdb enabled

```sh
make test PYTEST_ARGS="--pdb"
```

To see all available pytest arguments, run `make test PYTEST_ARGS="--help"`.

### Integration tests

PyIceberg has integration tests with Apache Spark. Spark will create a new database and provision some tables that PyIceberg can query against.

```sh
make test-integration
```

This will restart the containers, to get to a clean state, and then run the PyTest suite. In case something changed in the Dockerfile or the provision script, you can run:

```sh
make test-integration-rebuild
```

To rebuild the containers from scratch.

#### Running Integration Tests against REST Catalogs

!!! warning "Do not run against production catalogs"
    The integration tests will delete data throughout the entirety of your catalog. Running these integration tests against production catalogs will result in data loss.

PyIceberg supports the ability to run our catalog tests against an arbitrary REST Catalog.

In order to run the test catalog, you will need to specify which REST catalog to run against with the `PYICEBERG_TEST_CATALOG` environment variable

```sh
export PYICEBERG_TEST_CATALOG=test_catalog
```

The catalog in question can be configured either through the ~/.pyiceberg.yaml file or through environment variables.

```yaml
catalog:
  test_catalog:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret
```

```sh
export PYICEBERG_CATALOG__TEST_CATALOG__URI=thrift://localhost:9083
export PYICEBERG_CATALOG__TEST_CATALOG__ACCESS_KEY_ID=username
export PYICEBERG_CATALOG__TEST_CATALOG__SECRET_ACCESS_KEY=password
```

## Code standards

Below are the formalized conventions that we adhere to in the PyIceberg project. The goal of this is to have a common agreement on how to evolve the codebase, but also using it as guidelines for newcomers to the project.

### API Compatibility

It is important to keep the Python public API compatible across versions. The Python official [PEP-8](https://peps.python.org/pep-0008/) defines public methods as: _Public attributes should have no leading underscores_. This means not removing any methods without any notice, or removing or renaming any existing parameters. Adding new optional parameters is okay.

If you want to remove a method, please add a deprecation notice by annotating the function using `@deprecated`:

```python
from pyiceberg.utils.deprecated import deprecated


@deprecated(
    deprecated_in="0.1.0",
    removed_in="0.2.0",
    help_message="Please use load_something_else() instead",
)
def load_something():
    pass
```

Which will warn:

```text
Call to load_something, deprecated in 0.1.0, will be removed in 0.2.0. Please use load_something_else() instead.
```

If you want to remove a property or notify about a behavior change, please add a deprecation notice by calling the deprecation_message function:

```python
from pyiceberg.utils.deprecated import deprecation_message

deprecation_message(
    deprecated_in="0.1.0",
    removed_in="0.2.0",
    help_message="The old_property is deprecated. Please use the something_else property instead.",
)
```

Which will warn:

```text
Deprecated in 0.1.0, will be removed in 0.2.0. The old_property is deprecated. Please use the something_else property instead.
```

### Type annotations

For the type annotation the types from the `Typing` package are used.

### Third party libraries

PyIceberg naturally integrates into the rich Python ecosystem, however it is important to be hesitant adding third party packages. Adding a lot of packages makes the library heavyweight, and causes incompatibilities with other projects if they use a different version of the library. Also, big libraries such as `s3fs`, `adlfs`, `pyarrow`, `thrift` should be optional to avoid downloading everything, while not being sure if is actually being used.

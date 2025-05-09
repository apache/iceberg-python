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

For the development, Poetry is used for packing and dependency management. You can install this using:

```bash
make install-poetry
```

To get started, you can run `make install`, which installs all the dependencies of the Iceberg library. This also installs the development dependencies. If you don't want to install the development dependencies, you need to install using `poetry install --without dev` instead of `make install`.

If you want to install the library on the host, you can simply run `pip3 install -e .`. If you wish to use a virtual environment, you can run `poetry shell`. Poetry will open up a virtual environment with all the dependencies set.

> **Note:** If you want to use `poetry shell`, you need to install it using `pip install poetry-plugin-shell`. Alternatively, you can run commands directly with `poetry run`.

To set up IDEA with Poetry:

- Open up the Python project in IntelliJ
- Make sure that you're on latest main (that includes Poetry)
- Go to File -> Project Structure (⌘;)
- Go to Platform Settings -> SDKs
- Click the + sign -> Add Python SDK
- Select Poetry Environment from the left hand side bar and hit OK
- It can take some time to download all the dependencies based on your internet
- Go to Project Settings -> Project
- Select the Poetry SDK from the SDK dropdown, and click OK

For IDEA ≤2021 you need to install the [Poetry integration as a plugin](https://plugins.jetbrains.com/plugin/14307-poetry/).

Now you're set using Poetry, and all the tests will run in Poetry, and you'll have syntax highlighting in the pyproject.toml to indicate stale dependencies.

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

`pre-commit` is used for autoformatting and linting:

```bash
make lint
```

Pre-commit will automatically fix the violations such as import orders, formatting etc. Pylint errors you need to fix yourself.

In contrast to the name suggest, it doesn't run the checks on the commit. If this is something that you like, you can set this up by running `pre-commit install`.

You can bump the integrations to the latest version using `pre-commit autoupdate`. This will check if there is a newer version of `{black,mypy,isort,...}` and update the yaml.

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

## Code standards

Below are the formalized conventions that we adhere to in the PyIceberg project. The goal of this is to have a common agreement on how to evolve the codebase, but also using it as guidelines for newcomers to the project.

## API Compatibility

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

## Type annotations

For the type annotation the types from the `Typing` package are used.

PyIceberg offers support from Python 3.9 onwards, we can't use the [type hints from the standard collections](https://peps.python.org/pep-0585/).

## Third party libraries

PyIceberg naturally integrates into the rich Python ecosystem, however it is important to be hesitant adding third party packages. Adding a lot of packages makes the library heavyweight, and causes incompatibilities with other projects if they use a different version of the library. Also, big libraries such as `s3fs`, `adlfs`, `pyarrow`, `thrift` should be optional to avoid downloading everything, while not being sure if is actually being used.

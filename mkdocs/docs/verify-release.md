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

# Verifying a release

Each Apache PyIceberg release is validated by the community by holding a vote. A community release manager will prepare a release candidate and call a vote on the Iceberg dev list. To validate the release candidate, community members will test it out in their downstream projects and environments.

In addition to testing in downstream projects, community members also check the release’s signatures, checksums, and license documentation.

## Validating a release candidate

Release announcements include links to the following:

- A source tarball
- A signature (.asc)
- A checksum (.sha512)
- KEYS file
- GitHub change comparison

After downloading the source tarball, signature, checksum, and KEYS file, here are instructions on how to verify signatures, checksums, and documentation.

## Verifying signatures

First, import the keys.

```sh
curl https://downloads.apache.org/iceberg/KEYS -o KEYS
gpg --import KEYS
```

Set an environment variable to the version to verify and path to use

```sh
export PYICEBERG_VERSION=<version> # e.g. 0.6.1rc3
export PYICEBERG_VERIFICATION_DIR=/tmp/pyiceberg/${PYICEBERG_VERSION}
```

Next, verify the `.asc` file.

```sh
svn checkout https://dist.apache.org/repos/dist/dev/iceberg/pyiceberg-${PYICEBERG_VERSION}/ ${PYICEBERG_VERIFICATION_DIR}

cd ${PYICEBERG_VERIFICATION_DIR}

for name in $(ls pyiceberg-*.whl pyiceberg-*.tar.gz)
do
    gpg --verify ${name}.asc ${name}
done
```

## Verifying checksums

```sh
cd ${PYICEBERG_VERIFICATION_DIR}
for name in $(ls pyiceberg-*.whl.sha512 pyiceberg-*.tar.gz.sha512)
do
    shasum -a 512 --check ${name}
done
```

## Verifying License Documentation

```sh
export PYICEBERG_RELEASE_VERSION=${PYICEBERG_VERSION/rc?/}  # remove rcX qualifier
tar xzf pyiceberg-${PYICEBERG_RELEASE_VERSION}.tar.gz
cd pyiceberg-${PYICEBERG_RELEASE_VERSION}
```

Run RAT checks to validate license header:

```shell
./dev/check-license
```

## Testing

This section explains how to run the tests of the source distribution.

<!-- prettier-ignore-start -->

!!! note "Python Version"
    Make sure you're using [a supported Python version](https://github.com/apache/iceberg-python/blob/main/pyproject.toml#L29-L32)

<!-- prettier-ignore-end -->

First step is to install the package:

```sh
make install
```

To run the full test coverage, with both unit tests and integration tests:

```sh
make test-coverage
```

This will spin up Docker containers to facilitate running test coverage.

# Cast the vote

Votes are cast by replying to the release candidate announcement email on the dev mailing list with either `+1`, `0`, or `-1`. For example :

> [ ] +1 Release this as PyIceberg 0.3.0
>
> [ ] +0
>
> [ ] -1 Do not release this because…

In addition to your vote, it’s customary to specify if your vote is binding or non-binding. Only members of the Project Management Committee have formally binding votes. If you’re unsure, you can specify that your vote is non-binding. To read more about voting in the Apache framework, checkout the [Voting](https://www.apache.org/foundation/voting.html) information page on the Apache foundation’s website.

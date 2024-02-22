# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import re
import subprocess
from collections import namedtuple
from importlib import resources

import griffe
import pytest
import yaml
from griffe.exceptions import GitError

Exclusion = namedtuple('Exclusion', 'obj_path, kind')


def fetch_tags() -> None:
    check_if_upstream = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        encoding='utf-8',
    )
    if check_if_upstream.returncode != 0:
        raise GitError("This is not a git repository")

    origin = check_if_upstream.stdout.strip()
    fetch_cmd = ["git", "fetch", "--tags"]
    if not origin == "https://github.com/apache/iceberg-python":
        fetch_cmd.append("upstream")

    fetch_tags = subprocess.run(
        fetch_cmd,
        check=False,
    )
    if fetch_tags.returncode != 0:
        raise GitError(f"Cannot fetch Git tags with command: {fetch_cmd} from origin: {origin}")


def get_latest_release() -> str:
    list_tags = subprocess.run(
        ["git", "tag", "-l", "--sort=-version:refname"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        encoding='utf-8',
    )
    output = list_tags.stdout.strip()
    if list_tags.returncode != 0 or not output:
        raise GitError(f"Cannot list Git tags: {output or 'no tags'}")
    tags = output.split("\n")

    p = re.compile(r"^pyiceberg-\d+.\d+.\d+$")

    releases = [t for t in tags if p.match(t)]
    return releases[0]


@pytest.mark.api
def test_breaking_change() -> None:
    """
    Check for breaking changes since the latest release.

    We first fetch the tags from the parent repository, to get
    the commit reference of the latest release. Then, we check for
    breaking changes that aren't specified in the exclusion list.

    If a breaking change is intended, the Exclusion can be added as:
    ```
    - obj_path: pyiceberg.path_to_object
      kind: griffe.enumerations.BreakageKind.name
    ```
    to the corresponding "tests.api.exclude.<release>.yaml" file.
    """
    fetch_tags()
    latest_release = get_latest_release()

    previous = griffe.load_git("pyiceberg", ref=latest_release)
    current = griffe.load("pyiceberg")

    with resources.files("tests.api.exclude").joinpath(f"{latest_release}.yaml").open("r") as exclude_file:
        exclusion_list = yaml.safe_load(exclude_file)["exclude"]

    exclusion_set = {Exclusion(**x) for x in exclusion_list}

    breaking_changes = []

    for breakage in griffe.find_breaking_changes(previous, current):
        if Exclusion(obj_path=breakage.obj.path, kind=breakage.kind.name) not in exclusion_set:
            breaking_changes.append(breakage.as_dict())

    assert not breaking_changes, f"Total of {len(breaking_changes)} breaking API changes since {latest_release}"

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

import fnmatch
import re
import subprocess

import griffe
import importlib_resources as resources
import pytest
import yaml
from griffe.exceptions import GitError


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
    exclude
    - pyiceberg.path_to_object # justification as comment #PRNUM
    ```
    to the corresponding "tests.api.exclude.<release>.yaml" file.
    """
    fetch_tags()
    latest_release = get_latest_release()

    previous = griffe.load_git("pyiceberg", ref=latest_release)
    current = griffe.load("pyiceberg")

    exclusion_list = []

    for file_name in [latest_release, "all"]:
        with resources.files("tests.api.exclude").joinpath(f"{file_name}.yaml").open("r") as exclude_file:
            if (exclude := yaml.safe_load(exclude_file)["exclude"]) is not None:
                exclusion_list += exclude

    breaking_changes = []

    for breakage in griffe.find_breaking_changes(previous, current):
        if not any(fnmatch.fnmatch(breakage.obj.path, exclude_path) for exclude_path in exclusion_list):
            breaking_changes.append(breakage.as_dict())

    assert not breaking_changes, f"Total of {len(breaking_changes)} breaking API changes since {latest_release}"

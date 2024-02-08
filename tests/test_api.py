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

import griffe
from griffe.exceptions import GitError


def test_breaking_change() -> None:
    fetch_process = subprocess.run(
        ["git", "fetch", "--tags", "upstream"],
        check=False,
    )
    if fetch_process.returncode != 0:
        raise GitError("Cannot fetch Git tags from upstream")

    tag_process = subprocess.run(
        ["git", "tag", "-l", "--sort=-version:refname"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        encoding='utf-8',
    )
    output = tag_process.stdout.strip()
    if tag_process.returncode != 0 or not output:
        raise GitError(f"Cannot list Git tags from upstream: {output or 'no tags'}")
    tags = output.split("\n")

    p = re.compile(r"^pyiceberg-\d+.\d+.\d+$")

    releases = [t for t in tags if p.match(t)]

    previous = griffe.load_git("pyiceberg", ref=releases[0])
    current = griffe.load_git("pyiceberg")

    breaking_changes = []

    for breakage in griffe.find_breaking_changes(previous, current):
        breaking_changes.append(breakage.as_dict())

    assert not breaking_changes

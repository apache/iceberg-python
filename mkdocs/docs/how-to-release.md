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

# How to release

The guide to release PyIceberg.

The first step is to publish a release candidate (RC) and publish it to the public for testing and validation. Once the vote has passed on the RC, the RC turns into the new release.

## Preparing for a release

Before running the release candidate, we want to remove any APIs that were marked for removal under the @deprecated tag for this release.

For example, the API with the following deprecation tag should be removed when preparing for the 0.2.0 release.

```python

@deprecated(
    deprecated_in="0.1.0",
    removed_in="0.2.0",
    help_message="Please use load_something_else() instead",
)
```

We also have the `deprecation_message` function. We need to change the behavior according to what is noted in the message of that deprecation.

```python
deprecation_message(
    deprecated_in="0.1.0",
    removed_in="0.2.0",
    help_message="The old_property is deprecated. Please use the something_else property instead.",
)
```

## Running a release candidate

Make sure that the version is correct in `pyproject.toml` and `pyiceberg/__init__.py`. Correct means that it reflects the version that you want to release.

### Setting the tag

Make sure that you're on the right branch, and the latest branch:

For a Major/Minor release, make sure that you're on `main`, for patch versions the branch corresponding to the version that you want to patch, i.e. `pyiceberg-0.6.x`.

```bash
git checkout <branch>
git fetch --all
git reset --hard apache/<branch>
```

Set the tag on the last commit:

```bash
export RC=rc1
export VERSION=0.7.0${RC}
export VERSION_WITHOUT_RC=${VERSION/rc?/}
export VERSION_BRANCH=${VERSION_WITHOUT_RC//./-}
export GIT_TAG=pyiceberg-${VERSION}

git tag -s ${GIT_TAG} -m "PyIceberg ${VERSION}"
git push apache ${GIT_TAG}

export GIT_TAG_REF=$(git show-ref ${GIT_TAG})
export GIT_TAG_HASH=${GIT_TAG_REF:0:40}
export LAST_COMMIT_ID=$(git rev-list ${GIT_TAG} 2> /dev/null | head -n 1)
```

The `-s` option will sign the commit. If you don't have a key yet, you can find the instructions [here](http://www.apache.org/dev/openpgp.html#key-gen-generate-key). To install gpg on a M1 based Mac, a couple of additional steps are required: <https://gist.github.com/phortuin/cf24b1cca3258720c71ad42977e1ba57>.
If you have not published your GPG key in [KEYS](https://dist.apache.org/repos/dist/dev/iceberg/KEYS) yet, you must publish it before sending the vote email by doing:

```bash
svn co https://dist.apache.org/repos/dist/dev/iceberg icebergsvn
cd icebergsvn
echo "" >> KEYS # append a newline
gpg --list-sigs <YOUR KEY ID HERE> >> KEYS # append signatures
gpg --armor --export <YOUR KEY ID HERE> >> KEYS # append public key block
svn commit -m "add key for <YOUR NAME HERE>"
```

### Upload to Apache SVN

Both the source distribution (`sdist`) and the binary distributions (`wheels`) need to be published for the RC. The wheels are convenient to avoid having people to install compilers locally. The downside is that each architecture requires its own wheel. [use `cibuildwheel`](https://github.com/pypa/cibuildwheel) runs in Github actions to create a wheel for each of the architectures.

Before committing the files to the Apache SVN artifact distribution SVN hashes need to be generated, and those need to be signed with gpg to make sure that they are authentic.

Go to [Github Actions and run the `Python release` action](https://github.com/apache/iceberg-python/actions/workflows/python-release.yml). **Set the version to main, since we cannot modify the source**.

![Github Actions Run Workflow for SVN Upload](assets/images/ghactions-run-workflow-svn-upload.png)

Download the zip, and sign the files:

```bash
cd release-main/

for name in $(ls pyiceberg-*.whl pyiceberg-*.tar.gz)
do
    gpg --yes --armor --local-user fokko@apache.org --output "${name}.asc" --detach-sig "${name}"
    shasum -a 512 "${name}" > "${name}.sha512"
done
```

Now we can upload the files from the same directory:

```bash
export SVN_TMP_DIR=/tmp/iceberg-${VERSION_BRANCH}/
svn checkout https://dist.apache.org/repos/dist/dev/iceberg $SVN_TMP_DIR

export SVN_TMP_DIR_VERSIONED=${SVN_TMP_DIR}pyiceberg-$VERSION/
mkdir -p $SVN_TMP_DIR_VERSIONED
cp * $SVN_TMP_DIR_VERSIONED
svn add $SVN_TMP_DIR_VERSIONED
svn ci -m "PyIceberg ${VERSION}" ${SVN_TMP_DIR_VERSIONED}
```

### Upload to PyPi

Go to Github Actions and run the `Python release` action again. This time, set the **version** of the release candidate as the input: e.g. `0.7.0rc1`. Download the zip and unzip it locally.

![Github Actions Run Workflow for PyPi Upload](assets/images/ghactions-run-workflow-pypi-upload.png)

Next step is to upload them to pypi. Please keep in mind that this **won't** bump the version for everyone that hasn't pinned their version, since it is set to an RC [pre-release and those are ignored](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#pre-release-versioning).

```bash
twine upload release-0.7.0rc1/*
```

Final step is to generate the email to the dev mail list:

```bash
cat << EOF > release-announcement-email.txt
To: dev@iceberg.apache.org
Subject: [VOTE] Release Apache PyIceberg $VERSION
Hi Everyone,

I propose that we release the following RC as the official PyIceberg $VERSION_WITHOUT_RC release.

A summary of the high level features:

* <Add summary by hand>

The commit ID is $LAST_COMMIT_ID

* This corresponds to the tag: $GIT_TAG ($GIT_TAG_HASH)
* https://github.com/apache/iceberg-python/releases/tag/$GIT_TAG
* https://github.com/apache/iceberg-python/tree/$LAST_COMMIT_ID

The release tarball, signature, and checksums are here:

* https://dist.apache.org/repos/dist/dev/iceberg/pyiceberg-$VERSION/

You can find the KEYS file here:

* https://dist.apache.org/repos/dist/dev/iceberg/KEYS

Convenience binary artifacts are staged on pypi:

https://pypi.org/project/pyiceberg/$VERSION/

And can be installed using: pip3 install pyiceberg==$VERSION

Instructions for verifying a release can be found here:

* https://py.iceberg.apache.org/verify-release/

Please download, verify, and test.

Please vote in the next 72 hours.
[ ] +1 Release this as PyIceberg $VERSION_WITHOUT_RC
[ ] +0
[ ] -1 Do not release this because...
EOF

cat release-announcement-email.txt
```

## Vote has passed

Once the vote has been passed, you can close the vote thread by concluding it:

```text
Thanks everyone for voting! The 72 hours have passed, and a minimum of 3 binding votes have been cast:

+1 Foo Bar (non-binding)
...
+1 Fokko Driesprong (binding)

The release candidate has been accepted as PyIceberg <VERSION>. Thanks everyone, when all artifacts are published the announcement will be sent out.

Kind regards,
```

### Copy the artifacts to the release dist

```bash
export RC=rc2
export VERSION=0.7.0${RC}
export VERSION_WITHOUT_RC=${VERSION/rc?/}

export SVN_DEV_DIR_VERSIONED="https://dist.apache.org/repos/dist/dev/iceberg/pyiceberg-${VERSION}"
export SVN_RELEASE_DIR_VERSIONED="https://dist.apache.org/repos/dist/release/iceberg/pyiceberg-${VERSION_WITHOUT_RC}"

svn mv ${SVN_DEV_DIR_VERSIONED} ${SVN_RELEASE_DIR_VERSIONED} -m "PyIceberg: Add release ${VERSION_WITHOUT_RC}"
```

<!-- prettier-ignore-start -->

!!! note
    Only a PMC member has the permission to upload an artifact to the SVN release dist.

<!-- prettier-ignore-end -->

### Upload the accepted release to PyPi

The latest version can be pushed to PyPi. Check out the Apache SVN and make sure to publish the right version with `twine`:

```bash
svn checkout https://dist.apache.org/repos/dist/release/iceberg /tmp/iceberg-dist-release/
cd /tmp/iceberg-dist-release/pyiceberg-${VERSION_WITHOUT_RC}
twine upload pyiceberg-*.whl pyiceberg-*.tar.gz
```

Send out an announcement on the dev mail list:

```text
To: dev@iceberg.apache.org
Subject: [ANNOUNCE] Apache PyIceberg release <VERSION>

I'm pleased to announce the release of Apache PyIceberg <VERSION>!

Apache Iceberg is an open table format for huge analytic datasets. Iceberg
delivers high query performance for tables with tens of petabytes of data,
along with atomic commits, concurrent writes, and SQL-compatible table
evolution.

This Python release can be downloaded from: https://pypi.org/project/pyiceberg/<VERSION>/

Thanks to everyone for contributing!
```

## Release the docs

A committer triggers the [`Python Docs` Github Actions](https://github.com/apache/iceberg-python/actions/workflows/python-ci-docs.yml) through the UI by selecting the branch that just has been released. This will publish the new docs.

## Update the Github template

Make sure to create a PR to update the [GitHub issues template](https://github.com/apache/iceberg-python/blob/main/.github/ISSUE_TEMPLATE/iceberg_bug_report.yml) with the latest version.

## Update the integration tests

Ensure to update the `PYICEBERG_VERSION` in the [Dockerfile](https://github.com/apache/iceberg-python/blob/main/dev/Dockerfile).

## Create a Github Release Note

Create a [new Release Note](https://github.com/apache/iceberg-python/releases/new) on the iceberg-python Github repository.

Input the tag in **Choose a tag** with the newly approved released version (e.g. `0.7.0`) and set it to **Create new tag** on publish. Pick the target commit version as the commit ID the release was approved on.
For example:
![Generate Release Notes](assets/images/gen-release-notes.jpg)

Then, select the previous release version as the **Previous tag** to use the diff between the two versions in generating the release notes.

**Generate release notes**.

**Set as the latest release** and **Publish**.

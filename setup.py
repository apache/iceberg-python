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

import os

from setuptools import Extension, find_packages, setup
from setuptools.command.sdist import sdist as _sdist


class sdist(_sdist):
    """Custom sdist that excludes .egg-info and setup.cfg."""

    def make_release_tree(self, base_dir: str, files: list[str]) -> None:
        # Filter egg-info from the file manifest
        files = [f for f in files if ".egg-info" not in f]

        super().make_release_tree(base_dir, files)

        # Remove setup.cfg after setuptools creates it
        setup_cfg = os.path.join(base_dir, "setup.cfg")
        if os.path.exists(setup_cfg):
            os.remove(setup_cfg)


allowed_to_fail = os.environ.get("CIBUILDWHEEL", "0") != "1"

ext_modules = []

try:
    import Cython.Compiler.Options
    from Cython.Build import cythonize

    Cython.Compiler.Options.annotate = True

    if os.name == "nt":  # Windows
        extra_compile_args = ["/O2"]
    else:  # UNIX-based systems (Linux, macOS)
        extra_compile_args = ["-O3"]

    package_path = "pyiceberg"

    extensions = [
        Extension(
            "pyiceberg.avro.decoder_fast",
            [os.path.join(package_path, "avro", "decoder_fast.pyx")],
            extra_compile_args=extra_compile_args,
            language="c",
        )
    ]

    ext_modules = cythonize(
        extensions,
        include_path=[package_path],
        compiler_directives={"language_level": "3"},
        annotate=True,
    )
except Exception:
    if not allowed_to_fail:
        raise

pyiceberg_packages = find_packages(include=["pyiceberg*"])
vendor_packages = find_packages(where="vendor", include=["fb303", "hive_metastore"])

packages = pyiceberg_packages + vendor_packages

setup(
    packages=packages,
    package_dir={
        "": ".",
        "fb303": "vendor/fb303",
        "hive_metastore": "vendor/hive_metastore",
    },
    include_package_data=True,
    ext_modules=ext_modules,
    cmdclass={"sdist": sdist},
)

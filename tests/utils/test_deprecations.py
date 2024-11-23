#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#
#  This file is originally licensed under the BSD 3-Clause
#  License by Anaconda, Inc.
#
#  For details, see the LICENSE file and the following URL:
#  https://opensource.org/licenses/BSD-3-Clause
#
#  SPDX-License-Identifier: BSD-3-Clause
#
#  Copyright (C) 2012 Anaconda, Inc

from __future__ import annotations

import sys
from argparse import ArgumentParser, _StoreAction, _StoreTrueAction
from contextlib import nullcontext
from typing import TYPE_CHECKING

import pytest

from pyiceberg.utils._deprecations import DeprecatedError, DeprecationHandler

if TYPE_CHECKING:
    pass

PENDING = pytest.param(
    DeprecationHandler("1.0"),  # deprecated
    PendingDeprecationWarning,  # warning
    "pending deprecation",  # message
    id="pending",
)
FUTURE = pytest.param(
    DeprecationHandler("2.0"),  # deprecated
    FutureWarning,  # warning
    "deprecated",  # message
    id="future",
)
DEPRECATED = pytest.param(
    DeprecationHandler("2.0"),  # deprecated
    DeprecationWarning,  # warning
    "deprecated",  # message
    id="deprecated",
)
REMOVE = pytest.param(
    DeprecationHandler("3.0"),  # deprecated
    None,  # warning
    None,  # message
    id="remove",
)

parametrize_user = pytest.mark.parametrize(
    "deprecated,warning,message",
    [PENDING, FUTURE, REMOVE],
)
parametrize_dev = pytest.mark.parametrize(
    "deprecated,warning,message",
    [PENDING, DEPRECATED, REMOVE],
)


@parametrize_dev
def test_function(
    deprecated: DeprecationHandler,
    warning: DeprecationWarning | None,
    message: str | None,
) -> None:
    """Calling a deprecated function displays associated warning (or error)."""
    with nullcontext() if warning else pytest.raises(DeprecatedError):

        @deprecated("2.0", "3.0")
        def foo() -> bool:
            return True

        with pytest.warns(warning, match=message):
            assert foo()


@parametrize_dev
def test_method(
    deprecated: DeprecationHandler,
    warning: DeprecationWarning | None,
    message: str | None,
) -> None:
    """Calling a deprecated method displays associated warning (or error)."""
    with nullcontext() if warning else pytest.raises(DeprecatedError):

        class Bar:
            @deprecated("2.0", "3.0")
            def foo(self) -> bool:
                return True

        with pytest.warns(warning, match=message):
            assert Bar().foo()


@parametrize_dev
def test_class(
    deprecated: DeprecationHandler,
    warning: DeprecationWarning | None,
    message: str | None,
) -> None:
    """Calling a deprecated class displays associated warning (or error)."""
    with nullcontext() if warning else pytest.raises(DeprecatedError):

        @deprecated("2.0", "3.0")
        class Foo:
            pass

        with pytest.warns(warning, match=message):
            assert Foo()


@parametrize_dev
def test_arguments(
    deprecated: DeprecationHandler,
    warning: DeprecationWarning | None,
    message: str | None,
) -> None:
    """Calling a deprecated argument displays associated warning (or error)."""
    with nullcontext() if warning else pytest.raises(DeprecatedError):

        @deprecated.argument("2.0", "3.0", "three")
        def foo(_one: int, _two: int) -> bool:
            return True

        # too many arguments, can only deprecate keyword arguments
        with pytest.raises(TypeError):
            assert foo(1, 2, 3)  # type: ignore[call-arg]

        # alerting user to pending deprecation
        with pytest.warns(warning, match=message):
            assert foo(1, 2, three=3)  # type: ignore[call-arg]

        # normal usage not needing deprecation
        assert foo(1, 2)


@parametrize_user
def test_action(
    deprecated: DeprecationHandler,
    warning: DeprecationWarning | None,
    message: str | None,
) -> None:
    """Calling a deprecated argparse.Action displays associated warning (or error)."""
    with nullcontext() if warning else pytest.raises(DeprecatedError):
        parser = ArgumentParser()
        parser.add_argument(
            "--foo",
            action=deprecated.action("2.0", "3.0", _StoreTrueAction),
        )
        parser.add_argument(
            "bar",
            action=deprecated.action("2.0", "3.0", _StoreAction),
        )

        with pytest.warns(warning, match=message):
            parser.parse_args(["--foo", "some_value"])

        with pytest.warns(warning, match=message):
            parser.parse_args(["bar"])


@parametrize_dev
def test_module(
    deprecated: DeprecationHandler,
    warning: DeprecationWarning | None,
    message: str | None,
) -> None:
    """Importing a deprecated module displays associated warning (or error)."""
    with pytest.warns(warning, match=message) if warning else pytest.raises(DeprecatedError):
        deprecated.module("2.0", "3.0")


@parametrize_dev
def test_constant(
    deprecated: DeprecationHandler,
    warning: DeprecationWarning | None,
    message: str | None,
) -> None:
    """Using a deprecated constant displays associated warning (or error)."""
    with nullcontext() if warning else pytest.raises(DeprecatedError):
        deprecated.constant("2.0", "3.0", "SOME_CONSTANT", 42)
        module = sys.modules[__name__]

        with pytest.warns(warning, match=message):
            module.SOME_CONSTANT  # noqa: B018


@parametrize_dev
def test_topic(
    deprecated: DeprecationHandler,
    warning: DeprecationWarning | None,
    message: str | None,
) -> None:
    """Reaching a deprecated topic displays associated warning (or error)."""
    with pytest.warns(warning, match=message) if warning else pytest.raises(DeprecatedError):
        deprecated.topic("2.0", "3.0", topic="Some special topic")


@parametrize_dev
def test_message(
    deprecated: DeprecationHandler,
    warning: DeprecationWarning | None,
    message: str,
) -> None:
    expected = {
        "pending deprecation": "some function is pending deprecation and will be removed in 3.0. more context",
        "deprecated": "some function is deprecated and will be removed in 3.0. more context",
    }
    if warning:
        assert deprecated.message("2.0", "3.0", "some function", "more context") == expected[message]
    else:
        pytest.raises(DeprecatedError)

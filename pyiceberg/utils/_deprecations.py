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

"""Tools to aid in deprecating code."""

from __future__ import annotations

import sys
import warnings
from argparse import Action
from functools import wraps
from types import ModuleType
from typing import TYPE_CHECKING, Any, Callable

from packaging.version import InvalidVersion, Version

if TYPE_CHECKING:
    from argparse import ArgumentParser, Namespace

    from typing_extensions import ParamSpec, TypeVar

    T = TypeVar("T")
    P = ParamSpec("P")

    ActionType = TypeVar("ActionType", bound=type[Action])

from pyiceberg import __version__


class DeprecatedError(RuntimeError):
    pass


# inspired by deprecation (https://deprecation.readthedocs.io/en/latest/) and
# CPython's warnings._deprecated
class DeprecationHandler:
    _version: str | None
    _version_tuple: tuple[int, ...] | None
    _version_object: Version | None

    def __init__(self, version: str) -> None:
        """Create a deprecation handle for the specified version.

        :param version: The version to compare against when checking deprecation statuses.
        """
        self._version = Version(version)
        # Try to parse the version string as a simple tuple[int, ...] to avoid
        # packaging.version import and costlier version comparisons.
        self._version_tuple = self._get_version_tuple(version)
        self._version_object = None

    @staticmethod
    def _get_version_tuple(version: str) -> tuple[int, ...] | None:
        """Return version as non-empty tuple of ints if possible, else None.

        :param version: Version string to parse.
        """
        try:
            return tuple(int(part) for part in version.strip().split(".")) or None
        except (AttributeError, ValueError):
            return None

    def _version_less_than(self, version: str) -> bool:
        """Compare the current version with the provided version string."""
        try:
            target_version = Version(version)
        except InvalidVersion as e:
            raise ValueError(f"Invalid semantic version: {version}") from e

        return self._version < target_version

    def __call__(
        self,
        deprecate_in: str,
        remove_in: str,
        *,
        addendum: str | None = None,
        stack: int = 0,
        deprecation_type: type[Warning] = DeprecationWarning,
    ) -> Callable[[Callable[P, T]], Callable[P, T]]:
        """Deprecate decorator for functions, methods, & classes.

        :param deprecate_in: Version in which code will be marked as deprecated.
        :param remove_in: Version in which code is expected to be removed.
        :param addendum: Optional additional messaging. Useful to indicate what to do instead.
        :param stack: Optional stacklevel increment.
        """

        def deprecated_decorator(func: Callable[P, T]) -> Callable[P, T]:
            # detect function name and generate message
            category, message = self._generate_message(
                deprecate_in=deprecate_in,
                remove_in=remove_in,
                prefix=f"{func.__module__}.{func.__qualname__}",
                addendum=addendum,
                deprecation_type=deprecation_type,
            )

            # alert developer that it's time to remove something
            if not category:
                raise DeprecatedError(message)

            # alert user that it's time to remove something
            @wraps(func)
            def inner(*args: P.args, **kwargs: P.kwargs) -> T:
                with warnings.catch_warnings():  # temporarily override warning handling
                    warnings.simplefilter("always", category)  # turn off filter

                    warnings.warn(message, category, stacklevel=2 + stack)

                return func(*args, **kwargs)

            return inner

        return deprecated_decorator

    def argument(
        self,
        deprecate_in: str,
        remove_in: str,
        argument: str,
        *,
        rename: str | None = None,
        addendum: str | None = None,
        stack: int = 0,
        deprecation_type: type[Warning] = DeprecationWarning,
    ) -> Callable[[Callable[P, T]], Callable[P, T]]:
        """Deprecate decorator for keyword arguments.

        :param deprecate_in: Version in which code will be marked as deprecated.
        :param remove_in: Version in which code is expected to be removed.
        :param argument: The argument to deprecate.
        :param rename: Optional new argument name.
        :param addendum: Optional additional messaging. Useful to indicate what to do instead.
        :param stack: Optional stacklevel increment.
        """

        def deprecated_decorator(func: Callable[P, T]) -> Callable[P, T]:
            # detect function name and generate message
            category, message = self._generate_message(
                deprecate_in=deprecate_in,
                remove_in=remove_in,
                prefix=f"{func.__module__}.{func.__qualname__}({argument})",
                # provide a default addendum if renaming and no addendum is provided
                addendum=(f"Use '{rename}' instead." if rename and not addendum else addendum),
                deprecation_type=deprecation_type,
            )

            # alert developer that it's time to remove something
            if not category:
                raise DeprecatedError(message)

            # alert user that it's time to remove something
            @wraps(func)
            def inner(*args: P.args, **kwargs: P.kwargs) -> T:
                # only warn about argument deprecations if the argument is used
                if argument in kwargs:
                    with warnings.catch_warnings():  # temporarily override warning handling
                        warnings.simplefilter("always", category)  # turn off filter

                        warnings.warn(message, category, stacklevel=2 + stack)

                    # rename argument deprecations as needed
                    value = kwargs.pop(argument, None)
                    if rename:
                        kwargs.setdefault(rename, value)

                return func(*args, **kwargs)

            return inner

        return deprecated_decorator

    def action(
        self,
        deprecate_in: str,
        remove_in: str,
        action: ActionType,
        *,
        addendum: str | None = None,
        stack: int = 0,
        deprecation_type: type[Warning] = FutureWarning,
    ) -> ActionType:
        """Wrap any argparse.Action to issue a deprecation warning."""

        class DeprecationMixin(Action):
            category: type[Warning]
            help: str  # override argparse.Action's help type annotation

            def __init__(inner_self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)

                category, message = self._generate_message(
                    deprecate_in=deprecate_in,
                    remove_in=remove_in,
                    prefix=(
                        # option_string are ordered shortest to longest,
                        # use the longest as it's the most descriptive
                        f"`{inner_self.option_strings[-1]}`"
                        if inner_self.option_strings
                        # if not a flag/switch, use the destination itself
                        else f"`{inner_self.dest}`"
                    ),
                    addendum=addendum,
                    deprecation_type=deprecation_type,
                )

                # alert developer that it's time to remove something
                if not category:
                    raise DeprecatedError(message)

                inner_self.category = category
                inner_self.help = message

            def __call__(
                inner_self,
                parser: ArgumentParser,
                namespace: Namespace,
                values: Any,
                option_string: str | None = None,
            ) -> None:
                # alert user that it's time to remove something
                if values:
                    with warnings.catch_warnings():  # temporarily override warning handling
                        warnings.simplefilter("always", inner_self.category)  # turn off filter

                        warnings.warn(
                            inner_self.help,
                            inner_self.category,
                            stacklevel=7 + stack,
                        )

                super().__call__(parser, namespace, values, option_string)

        return type(action.__name__, (DeprecationMixin, action), {})  # type: ignore[return-value]

    def module(
        self,
        deprecate_in: str,
        remove_in: str,
        *,
        addendum: str | None = None,
        stack: int = 0,
    ) -> None:
        """Deprecate function for modules.

        :param deprecate_in: Version in which code will be marked as deprecated.
        :param remove_in: Version in which code is expected to be removed.
        :param addendum: Optional additional messaging. Useful to indicate what to do instead.
        :param stack: Optional stacklevel increment.
        """
        self.topic(
            deprecate_in=deprecate_in,
            remove_in=remove_in,
            prefix=self._get_module(stack)[1],
            addendum=addendum,
            stack=2 + stack,
        )

    def constant(
        self,
        deprecate_in: str,
        remove_in: str,
        constant: str,
        value: Any,
        *,
        addendum: str | None = None,
        stack: int = 0,
        deprecation_type: type[Warning] = DeprecationWarning,
    ) -> None:
        """Deprecate function for module constant/global.

        :param deprecate_in: Version in which code will be marked as deprecated.
        :param remove_in: Version in which code is expected to be removed.
        :param constant:
        :param value:
        :param addendum: Optional additional messaging. Useful to indicate what to do instead.
        :param stack: Optional stacklevel increment.
        """
        # detect calling module
        module, fullname = self._get_module(stack)
        # detect function name and generate message
        category, message = self._generate_message(
            deprecate_in=deprecate_in,
            remove_in=remove_in,
            prefix=f"{fullname}.{constant}",
            addendum=addendum,
            deprecation_type=deprecation_type,
        )

        # alert developer that it's time to remove something
        if not category:
            raise DeprecatedError(message)

        # patch module level __getattr__ to alert user that it's time to remove something
        super_getattr = getattr(module, "__getattr__", None)

        def __getattr__(name: str) -> Any:
            if name == constant:
                with warnings.catch_warnings():  # temporarily override warning handling
                    warnings.simplefilter("always", category)  # turn off filter

                    warnings.warn(message, category, stacklevel=2 + stack)
                return value

            if super_getattr:
                return super_getattr(name)

            raise AttributeError(f"module '{fullname}' has no attribute '{name}'")

        module.__getattr__ = __getattr__  # type: ignore[method-assign]

    def topic(
        self,
        deprecate_in: str,
        remove_in: str,
        *,
        addendum: str | None = None,
        prefix: str | None = None,
        stack: int = 0,
        deprecation_type: type[Warning] = DeprecationWarning,
    ) -> None:
        """Deprecate function for a addendum.

        :param deprecate_in: Version in which code will be marked as deprecated.
        :param remove_in: Version in which code is expected to be removed.
        :param addendum: Optional additional messaging. Useful to indicate what to do instead.
        :param prefix: The topic being deprecated.
        :param stack: Optional stacklevel increment.
        :param deprecation_type: The warning type to use for active deprecations.
        """
        # detect function name and generate message
        category, message = self._generate_message(
            deprecate_in=deprecate_in,
            remove_in=remove_in,
            prefix=prefix,
            addendum=addendum,
            deprecation_type=deprecation_type,
        )

        # alert developer that it's time to remove something
        if not category:
            raise DeprecatedError(message)

        # alert user that it's time to remove something
        with warnings.catch_warnings():  # temporarily override warning handling
            warnings.simplefilter("always", category)  # turn off filter

            warnings.warn(message, category, stacklevel=2 + stack)

    def _get_module(self, stack: int) -> tuple[ModuleType, str]:
        """Detect the module from which we are being called.

        :param stack: The stacklevel increment.
        :return: The module and module name.
        """
        try:
            frame = sys._getframe(2 + stack)
        except IndexError:
            # IndexError: 2 + stack is out of range
            pass
        else:
            # Shortcut finding the module by manually inspecting loaded modules.
            try:
                filename = frame.f_code.co_filename
            except AttributeError:
                # AttributeError: frame.f_code.co_filename is undefined
                pass
            else:
                # use a copy of sys.modules to avoid RuntimeError during iteration
                # see https://github.com/conda/conda/issues/13754
                for loaded in tuple(sys.modules.values()):
                    if not hasattr(loaded, "__file__"):
                        continue
                    if loaded.__file__ == filename:
                        return (loaded, loaded.__name__)

            # If above failed, do an expensive import and costly getmodule call.
            import inspect

            module = inspect.getmodule(frame)
            if module is not None:
                return (module, module.__name__)

        raise DeprecatedError("unable to determine the calling module")

    def message(
        self,
        deprecate_in: str,
        remove_in: str,
        prefix: str,
        addendum: str,
    ) -> str:
        """Generate a deprecation message.

        :param deprecate_in: Version in which code will be marked as deprecated.
        :param remove_in: Version in which code is expected to be removed.
        :param prefix: The message prefix, usually what should be deprecated.
        :param addendum: Additional messaging. Useful to indicate what to do instead.
        """
        _, message = self._generate_message(
            deprecate_in=deprecate_in,
            remove_in=remove_in,
            prefix=prefix,
            addendum=addendum,
        )

        return message

    def _generate_message(
        self,
        deprecate_in: str,
        remove_in: str,
        prefix: str | None,
        addendum: str | None,
        deprecation_type: type[Warning] | None = None,
    ) -> tuple[type[Warning] | None, str]:
        """Generate the standardized deprecation message.

        Determine whether the deprecation is pending, active, or past.

        :param deprecate_in: Version in which code will be marked as deprecated.
        :param remove_in: Version in which code is expected to be removed.
        :param prefix: The message prefix, usually the function name.
        :param addendum: Additional messaging. Useful to indicate what to do instead.
        :param deprecation_type: The warning type to use for active deprecations.
        :return: The warning category (if applicable) and the message.
        """
        category: type[Warning] | None

        if self._version_less_than(deprecate_in):
            category = PendingDeprecationWarning
            warning = f"is pending deprecation and will be removed in {remove_in}."
        elif self._version_less_than(remove_in):
            category = deprecation_type or DeprecationWarning
            warning = f"is deprecated and will be removed in {remove_in}."
        else:
            category = None
            warning = f"was slated for removal in {remove_in}."

        return category, " ".join(filter(None, [prefix, warning, addendum]))


deprecated = DeprecationHandler(__version__)

from typing import Any

from pydantic import ValidationError
from pydantic.fields import FieldInfo


class PlatformUtilityException(Exception):
    """A requested utility exited exceptionally."""

    pass


class InfoNotReceivedException(Exception):
    """A plugin did not respond in time."""

    pass


class ClimatoologyVersionMismatchException(Exception):
    """The plugins' library version does not match the required minimum version."""

    pass


class ClimatoologyUserError(Exception):
    """A non-recoverable error from the plugin whose cause should be forwarded to the user."""

    pass


class InputValidationError(Exception):
    """The user input validation failed."""

    def __init__(self, validation_errors: ValidationError, fields: dict[str, FieldInfo] = None):
        self._errors = validation_errors.errors()
        self._model_fields = fields

    def __str__(self):
        error_description = [
            f'{self._create_prefix(e["loc"])}{e["msg"]}. You provided: {self._clean_inputs(e["input"])}.'
            for e in self._errors
        ]
        return '\n'.join(error_description)

    def _create_prefix(self, error_loc: tuple) -> str:
        if not self._model_fields or len(error_loc) < 1:
            return ''

        field_names = []
        for f in error_loc:
            try:
                field_names.append(self._model_fields[f].title)
            except Exception:
                field_names.append(title=f)

        return ','.join(field_names) + ': '

    def _clean_inputs(self, input_vals: Any) -> Any:
        if not self._model_fields or not isinstance(input_vals, dict):
            return input_vals

        for k in list(input_vals.keys()):
            try:
                input_vals[self._model_fields[k].title] = input_vals[k]
                input_vals.pop(k)
            except Exception:
                continue
        return input_vals

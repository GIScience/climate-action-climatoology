from typing import Any

from pydantic import ValidationError
from pydantic.fields import FieldInfo

from climatoology.base.logging import get_climatoology_logger

log = get_climatoology_logger(__name__)


class PlatformUtilityError(Exception):
    """A requested utility exited exceptionally."""

    pass


class InfoNotReceivedError(Exception):
    """A plugin did not respond in time."""

    pass


class VersionMismatchError(Exception):
    """The plugins' library version does not match the required minimum version."""

    pass


class ClimatoologyUserError(Exception):
    """A non-recoverable error from the plugin whose cause should be forwarded to the user."""

    pass


class InputValidationError(Exception):
    """The user input validation failed."""

    pass


def create_pretty_validation_message(
    validation_error: ValidationError, model_fields: dict[str, FieldInfo] = None
) -> str:
    """Create a pretty validation message from a pydantic validation error.

    :param validation_error: the pydantic ValidationError
    :param model_fields: the `model_fields` attribute from the pydantic base model which triggered the validation
    :return: a string containing the pretty error message
    """

    def _create_prefix(error_fields: tuple) -> str:
        """Create a comma separated list of the `title` attributes of the `error_fields`, ending with a colon."""
        if not model_fields or len(error_fields) < 1:
            return ''

        field_names = []
        for f in error_fields:
            try:
                field_names.append(model_fields[f].title)
            except Exception as e:
                log.debug(f'Field name extraction failed for field {f}, using fallback', exc_info=e)
                field_names.append(f)

        return ','.join(field_names) + ': '

    def _clean_inputs(input_vals: Any) -> Any:
        """If `input_vals` is a dict, replace the attribute keys with their corresponding `title` value."""
        if not model_fields or not isinstance(input_vals, dict):
            return input_vals

        for k in list(input_vals.keys()):
            try:
                input_vals[model_fields[k].title] = input_vals[k]
                input_vals.pop(k)
            except Exception as e:
                log.debug(f'Input name for {k} could not be extracted, ignoring', exc_info=e)
                continue
        return input_vals

    error_description = [
        f'{_create_prefix(e["loc"])}{e["msg"]}. You provided: {_clean_inputs(e["input"])}.'
        for e in validation_error.errors()
    ]
    return '\n'.join(error_description)

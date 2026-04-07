from pathlib import Path
from typing import Optional

from babel.support import Translations
from pandas import DataFrame

from climatoology.base.logging import get_climatoology_logger
from climatoology.base.plugin_info import DEFAULT_LANGUAGE
from climatoology.base.utils import deep_apply_dataframe, deep_apply_dict

log = get_climatoology_logger(__name__)

# We backed up from designing a custom lazy translation class as it created issues in some edge cases, although it
# worked for many parts.
# See https://stackoverflow.com/questions/79911276/make-userstring-behave-like-str for details


def N_(message_id: str) -> str:  # noqa: N802
    """
    Mark `message_id` as translatable. It will be detected when creating .pot files and added to the translation
    lookups. To do the actual translation, call `tr` on the returned string.

    Warning: this acts as a lookup, so you cannot modify the string before calling `tr`.

    :param message_id: the **string declaration** to be translated. It MUST not be defined as a variable because that
    will not be picked up when creating .pot files.
    """
    return message_id


def tr(message_id: str) -> str:
    """
    Translate `message_id` using gettext and the translations in the activated .mo files. The `message_id` must have
    been declared with `N_` and run through the gettext extraction pipeline.
    """
    try:
        # noinspection PyUnresolvedReferences
        # GNU gettext function _ is globally installed on computation start and info upload
        translated_string = _(message_id)
    except NameError as e:
        log.warning('Gettext is not properly installed, returning message id instead of translated string.', exc_info=e)
        translated_string = message_id
    return translated_string


def set_language(lang: str, localisation_dir: Optional[Path], domain: str = 'messages') -> None:
    # We are using: https://babel.pocoo.org/en/latest/support.html#extended-translations-class
    # which extends from: https://docs.python.org/3/library/gettext.html#changing-languages-on-the-fly
    i18n = Translations.load(
        dirname=localisation_dir,
        locales=[lang, DEFAULT_LANGUAGE],
        domain=domain,
    )
    i18n.install()


def translate_dataframe(
    df: DataFrame,
    *,
    translate_index_name: bool = True,
    translate_index_values: bool = True,
    exclude_columns: tuple[str] = tuple(),
    exclude_column_names: tuple[str] = tuple(),
):
    """
    Deep translate the provided dataframe.

    :param df: the dataframe to be translated.
    :param translate_index_name: whether or not to translate the index name (MultiIndex not supported). Defaults to True.
    :param translate_index_values: whether or not to translate the values of the index (MultiIndex not supported).
      Defaults to True.
    :param exclude_columns: columns for which the values should not be translated. By default all columns will be
      translated.
    :param exclude_column_names: column names which should not be translated. By default all column names will be
      translated. If column names are translated, you will no longer be able to access them from their original names.
    :return: a copy of the dataframe that has been translated.
    """
    return deep_apply_dataframe(
        df=df,
        func=tr,
        apply_index_name=translate_index_name,
        apply_index_values=translate_index_values,
        exclude_column_names=exclude_column_names,
        exclude_columns=exclude_columns,
    )


def deep_translate_dict(data: dict, target_keys: set) -> dict:
    return deep_apply_dict(data=data, func=tr, target_keys=target_keys)

from abc import ABC
from datetime import date, timedelta
from typing import Optional

import requests
from pydantic import BaseModel, Field, model_validator
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from climatoology.base.logging import get_climatoology_logger

log = get_climatoology_logger(__name__)


class TimeRange(BaseModel):
    start_date: Optional[date] = Field(
        title='Start Date',
        description='Lower bound (inclusive) of remote sensing imagery acquisition date (UTC). '
        'If not set it will be automatically set to one year before `end_date`',
        examples=['2024-01-01'],
        default=None,
    )
    end_date: date = Field(
        title='End Date',
        description='Upper bound (inclusive) of remote sensing imagery acquisition date (UTC). '
        'Defaults to the 31st December of last year.',
        examples=['2024-12-31'],
        default=date(date.today().year - 1, 12, 31),
    )

    @model_validator(mode='after')
    def check_order(self) -> 'TimeRange':
        if self.start_date is not None:
            assert self.start_date < self.end_date, 'Start date must be before end date'
        return self

    @model_validator(mode='after')
    def minus_year(self) -> 'TimeRange':
        if not self.start_date:
            self.start_date = self.end_date - timedelta(days=365)
        return self


class HealthCheck(BaseModel):
    status: str = 'ok'


class PlatformHttpUtility(ABC):
    def __init__(
        self,
        base_url: str,
        max_retries: int = 5,
    ):
        assert base_url[-1] != '/', 'The base_url must not end with a /'
        self.base_url = base_url

        retries = Retry(total=max_retries, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])

        self.session = requests.Session()

        # We have to mount http:// to overwrite the default adapters
        # noinspection HttpUrlsUsage
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        assert self.health(), 'Utility startup failed: the API is not reachable.'

    def health(self) -> bool:
        try:
            url = f'{self.base_url}/health'
            response = self.session.get(url=url)
            response.raise_for_status()
            assert response.json().get('status') == HealthCheck().status
        except Exception as e:
            log.error(f'{self.__class__.__name__} API not reachable', exc_info=e)
            return False
        return True

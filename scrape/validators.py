import re
from typing import List, Dict
from urllib.parse import urlparse
from operator import itemgetter

from merch.db import PostgresDB


class URLValidator:
    def __init__(
        self,
        domains: List,
        blacklist_conn_id: str,
        blacklist_query: str,
        target_table: Dict,
        temp_table: Dict
    ) -> None:
        self.domains = domains
        self.blacklist = self._get_blacklist(
            blacklist_conn_id,
            blacklist_query,
            target_table,
            temp_table
        )

    @staticmethod
    def _is_correct_url(url: str) -> bool:
        regex = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        if re.match(regex, url) is not None:
            return True
        else:
            return False

    @staticmethod
    def _get_domain(url: str, levels: int = 2) -> str:
        netloc = urlparse(url).netloc
        domain = '.'.join(netloc.split('.')[-levels:]).lower()
        return domain

    def _is_correct_domain(self, url: str) -> bool:
        domain = self._get_domain(url)

        if domain in self.domains:
            return True
        else:
            return False

    def _get_blacklist(
        self,
        blacklist_conn_id: str,
        blacklist_query: str,
        target_table: Dict,
        temp_table: Dict
    ) -> List:
        pg_db = PostgresDB(blacklist_conn_id)
        pg_db.drop_table(temp_table['table_name'])
        pg_db.create_table(temp_table)
        pg_db.create_table(target_table)

        get_url = itemgetter(0)
        blacklist = list(map(get_url, pg_db.query(blacklist_query)))
        return blacklist

    def validate_url(self, url: str) -> Dict:
        validation_result: Dict = {}
        validation_result['url'] = url
        validation_result['is_valid'] = False
        validation_result['domain'] = self._get_domain(url)

        if not self._is_correct_url(url):
            validation_result['error_code'] = 'incorrect_url'
        elif not self._is_correct_domain(url):
            validation_result['error_code'] = 'wrong_domain'
        elif url in self.blacklist:
            validation_result['error_code'] = 'no_update'
        else:
            validation_result['is_valid'] = True

        return validation_result

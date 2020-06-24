import re
from functools import partial
from typing import List, Dict, Tuple, Set
from time import sleep
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests import Response
from requests.exceptions import HTTPError


def parse_habr_views_count(response: Response) -> str:
    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    views_count = soup.find('span', class_="post-stats__views-count").text
    return views_count


def prepare_pikabu_request(url: str, headers: Dict) -> Tuple[str, Dict]:
    story_id = url.split('_')[-1]
    stat_url = f'https://d.pikabu.ru/stat/story/{story_id}'
    return stat_url, headers


def parse_pikabu_views_count(response: Response) -> str:
    stats_data = response.json()
    views_count = stats_data['data']['v']
    return views_count


def parse_pornhub_views_count(response: Response) -> str:
    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    views_count = soup.find('span', class_="count").text
    return views_count


def parse_rutube_views_count(response: Response) -> str:
    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    views_count = soup.find('span', class_="video-info-card__view-count").text
    return views_count


def prepare_vimeo_request(url: str, headers: Dict) -> Tuple[str, Dict]:
    stat_url = f'{url}?action=load_stat_counts'
    headers['x-requested-with'] = 'XMLHttpRequest'
    return stat_url, headers


def parse_vimeo_views_count(response: Response) -> str:
    json_data = response.json()
    views_count = json_data['total_plays']['raw']
    return views_count


def parse_youtube_views_count(response: Response) -> str:
    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')

    scripts = soup.find_all('script')
    scripts_str = list(map(lambda x: x.string, scripts))
    count_scripts_str = list(filter(lambda x: x is not None,
                                    scripts_str))
    count_scripts_str = list(filter(lambda x: 'viewCount' in x,
                                    count_scripts_str))

    pattern = re.compile(r'viewCount[\\]{,1}":[\\]{,1}"(\d+)')
    views_count = re.findall(pattern, count_scripts_str[0])[0]
    return views_count


def filter_urls(
    valid_results: List,
    domain: str,
    is_valid: bool = True
) -> Set:
    results = filter(lambda x: x['is_valid'] is is_valid
                     and x['domain'] == domain,
                     valid_results)
    urls = set(map(lambda x: x['url'], results))
    return urls


def clean_views_count(raw_views_count: str) -> int:
    units_map = {'k': 1000}
    raw_views_count = str(raw_views_count).lower().strip()

    if raw_views_count[-1] in units_map:
        unit = raw_views_count[-1]
        num = raw_views_count[:-1]

        views_count = float(re.sub(r'\D', '', num))
        decimal_seps = re.findall(r'[\.,]', num)

        if len(decimal_seps) > 0:
            decimal_sep = decimal_seps[0]
            decimal_pos = len(num) - num.find(decimal_sep) - 1
        else:
            decimal_pos = 0

        views_count = int(views_count * units_map[unit] / (10**decimal_pos))
    else:
        views_count = int(re.sub(r'\D', '', raw_views_count))

    return views_count


def parse_views_count(response: Response, domain: str) -> str:
    return DOMAIN_PARSERS[domain]['parse'](response)


def get_domain(url: str, levels: int = 2) -> str:
    netloc = urlparse(url).netloc
    domain = '.'.join(netloc.split('.')[-levels:]).lower()
    return domain


def get_views_count(url: str, sleep_time: int = 1) -> Dict:
    print(url, sleep_time)
    parse_results: Dict = {}
    parse_results['url'] = url
    parse_results['is_parsed'] = False

    domain = get_domain(url)
    prepare_request = DOMAIN_PARSERS[domain].get('prepare_request')
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'
    }

    if prepare_request is not None:
        url, headers = prepare_request(url, headers)

    response = requests.get(url, headers=headers)
    sleep(sleep_time)

    try:
        response.raise_for_status()
    except HTTPError as e:
        status_code = str(e.response.status_code)
        reason = '_'.join(e.response.reason.lower().split())
        error_code = "_".join([status_code, reason])
        parse_results['error_code'] = error_code
        print(parse_results)
        return parse_results

    try:
        views_count = parse_views_count(response, domain)
    except (AttributeError, IndexError, KeyError):
        parse_results['error_code'] = 'element_not_found'
        print(parse_results)
        return parse_results

    parse_results['is_parsed'] = True
    parse_results['raw_views_count'] = views_count
    parse_results['views_count'] = clean_views_count(views_count)
    print(parse_results)

    return parse_results


def get_domain_counts(
    valid_results: List,
    domain: str,
    sleep_time: int
) -> List:
    domain_urls = filter_urls(valid_results, domain)
    get_views_count_with_sleep = partial(
        get_views_count,
        sleep_time=sleep_time
    )

    return list(map(get_views_count_with_sleep, domain_urls))


DOMAIN_PARSERS: Dict = {
    'habr.com': {
        'parse': parse_habr_views_count,
    },
    'pikabu.ru': {
        'parse': parse_pikabu_views_count,
        'prepare_request': prepare_pikabu_request
    },
    'pornhub.com': {
        'parse': parse_pornhub_views_count
    },
    'rutube.ru': {
        'parse': parse_rutube_views_count
    },
    'vimeo.com': {
        'parse': parse_vimeo_views_count,
        'prepare_request': prepare_vimeo_request
    },
    'youtube.com': {
        'parse': parse_youtube_views_count
    }
}

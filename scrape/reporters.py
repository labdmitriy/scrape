from csv import DictWriter
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from typing import List


def calculate_processing_stats(
    valid_results: List,
    parse_results: List
) -> List:
    processing_stats = []

    for row_num, valid_result in enumerate(valid_results):
        stat = {}
        stat['row_num'] = row_num
        stat['url'] = valid_result['url']

        if not valid_result['is_valid']:
            value = valid_result['error_code']
            stat['is_parsed'] = False
        else:
            parse_result = list(filter(lambda x: x['url'] == stat['url'],
                                parse_results))[0]

            stat['is_parsed'] = parse_result['is_parsed']

            if not parse_result['is_parsed']:
                value = parse_result['error_code']
            else:
                value = parse_result['views_count']

        stat['value'] = value
        processing_stats.append(stat)

    return processing_stats


def generate_report(
    processing_stats: List,
    url_errors_path: Path
) -> str:
    parsed_urls = list(filter(lambda x: x['is_parsed'] is True,
                              processing_stats))
    error_urls = list(filter(lambda x: x['is_parsed'] is False,
                             processing_stats))
    get_value = itemgetter('value')
    errors = sorted(map(get_value, error_urls))
    errors_stats = {key: len(list(group)) for key, group in groupby(errors)}
    errors_stats_message = '\n'.join([f'{error}: {count}'
                                      for (error, count) in errors_stats.items()])

    summary_message = 'Processing results\n\n'
    summary_message += f'Processed URLs count: {len(processing_stats)}\n'
    summary_message += f'Parsed URLs count: {len(parsed_urls)}\n\n'
    summary_message += f'Errors\n\n{errors_stats_message}'

    with open(url_errors_path, 'w') as f:
        fieldnames = ['row_num', 'url', 'value']
        writer = DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        for line in error_urls:
            writer.writerow(line)

    return summary_message

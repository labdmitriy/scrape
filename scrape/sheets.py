from typing import Iterable, List

import gspread
from airflow.models.variable import Variable
from oauth2client.service_account import ServiceAccountCredentials as sac


class GoogleSheet:
    def __init__(self, key_name: str) -> None:
        self.secret_key = Variable.get(key_name, deserialize_json=True)

    def get_sheet(self, sheet_url_name: str) -> None:
        google_sheet_url = Variable.get(sheet_url_name)

        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = sac.from_json_keyfile_dict(self.secret_key, scope)
        client = gspread.authorize(creds)

        self.sheet = client.open_by_url(google_sheet_url).sheet1

    def get_column_values(self, column_name: str) -> List:
        head = self.sheet.find(column_name)
        row = head.row
        col = head.col
        values = self.sheet.col_values(col)[row:]
        return values

    def calculate_update_range(
        self,
        column_name: str,
        update_length: int
    ) -> str:
        head = self.sheet.find(column_name)
        head_address = head.address

        row = int(head_address[1:])
        col = head_address[0]

        start_row = row + 1
        end_row = row + update_length

        update_range = f'{col}{start_row}:{col}{end_row}'
        return update_range

    def update(self, update_range: str, update_values: Iterable) -> None:
        self.sheet.update(update_range,
                          [[value] for value in update_values])

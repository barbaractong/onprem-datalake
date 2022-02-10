import pandas as pd
from datetime import datetime


class ProcessRawData:
    def __init__(self, csv_path):
        self._csv_path = csv_path
        self.date_now = datetime.now().strftime("%m%d%y")


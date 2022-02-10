import os
from os import listdir
from os.path import isfile, join

from zipfile import ZipFile
from datetime import datetime


class ImportDataService:
    def __init__(self, origin_path):
        self._origin_path = origin_path
        self._files_list = []
        self.date_now = datetime.now().strftime("%m%d%y")

    def load_zip_files(self):
        self._files_list = [file for file in listdir(self._origin_path) if isfile(join(self._origin_path, file))]

        for file in self._files_list:
            if file.endswith('.zip') or file.endswith('.rar'):
                with ZipFile(os.path.join(self._origin_path, file)) as zip_file:
                    for _csv in zip_file.namelist():
                        if _csv.endswith('.csv'):
                            # TODO: renomear arquivo na hora de escrever no disco
                            zip_file.extract(_csv, '../data-container/stage')


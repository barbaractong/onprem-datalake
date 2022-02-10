import os
import shutil

import pandas
from datetime import datetime

from services.GetData import ImportDataService

get_data_from_origin = ImportDataService('../data-origin')

try:
    get_data_from_origin.load_zip_files()
except EOFError:
    raise "Failed to load data. Check data integrity to proceed."

for prep_csv in os.listdir('../data-container/stage'):
    temp_prep_csv = pandas.read_csv(filepath_or_buffer=f'../data-container/stage/{prep_csv}', sep='|', encoding='UTF-8')
    temp_prep_csv.assign(DT_CARGA=datetime.today().strftime('%Y%m%d'))

    # TODO: Verificar erro de permissao
    try:
        temp_prep_csv.to_parquet(f'../data-container/raw/{os.path.splitext(prep_csv)[0]}', compression='snappy')
        shutil.move(f'../data-container/stage/{prep_csv}', '../data-container/stage/moviment_ingestion_history/')
    except PermissionError:
        raise 'Warning: Files moved but not deleted from dir. Execute this script with admin.'
    except FileNotFoundError:
        raise f'File {prep_csv} not find.'





from datetime import datetime
import polars as pl
import os
import gc

try:
    # 1º STEP: Obtaining files from data directory:
    PATH_DADOS = r'../../dados'
    PATH_BRONZE = r'../../bronze'
    init_hour = datetime.now()
    print('\n'+'Loading data...')
    df_bolsa_familia = None

    # 2º STEP: Creating files list:
    files_list = []
    files_dir_list = os.listdir(PATH_DADOS)
    
    for file in files_dir_list:
        if file.endswith('.csv'):
            files_list.append(file)
    
    print(files_list)

    for file in files_list:
        print(f'Reading archive named: {file}')
        df = pl.read_csv(PATH_DADOS + file, separator=';', encoding='iso-8859-1')

    # 3º STEP: Creating a dataframe by concating files from files list:
    if df_bolsa_familia is None:
        df_bolsa_familia = df
    else:
        df_bolsa_familia = pl.concat([df_bolsa_familia, df])
    del df

    print(f'Sucess reading file named: {file}')

    # 4º STEP: Converting type string to float at column "VALOR DA PARCELA":
    df_bolsa_familia = df_bolsa_familia.with_columns(pl.col('VALOR PARCELA').str.replace(',', '.').cast(pl.Float64))
    
    print('Sucess on concating files from files list')

    # 5º STEP: Iniciating the burn of file Parquet:
    print('Iniciating the burn of file Parquet...')

    df_bolsa_familia.write_parquet(PATH_BRONZE + 'bolsa_familia.parquet')
    print(df_bolsa_familia.head())
    print(df_bolsa_familia.shape)
    del df_bolsa_familia
    gc.collect()

    print('Sucess on burning file Parquet')

    fin_hour = datetime.now()
    print(f'Run time overall: {fin_hour - init_hour}')

except Exception as e:
    print(f'Error while trying to obtain and/or read files from data directory! {e}')

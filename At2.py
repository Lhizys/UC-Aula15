import polars as pl
import numpy as np
from datetime import datetime
import os
import gc

# try:

#     ENDERECO_DADOS = r'./dados/'

#     print('Obtendo Dados')

#     inicio = datetime.now()

#     #ler os arquisvos

#     lst_arquivos = []  
#     lst_dir = os.listdir(ENDERECO_DADOS)  
    

#     for arquivo in lst_dir:
#         if arquivo.endswith('.csv'):  
#          lst_arquivos.append(arquivo)  

#         print(lst_arquivos)  

#     for arquivo in lst_arquivos:
#         print(f"Processando arquivo {arquivo}")
    
#         # Leitura de cada um dos dataframes
#         df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')
    
#         if 'df_bolsa_familia' in locals():  
#             df_bolsa_familia = pl.concat([df_bolsa_familia, df])  
#         else:
#             df_bolsa_familia = df  

#         print(df_bolsa_familia)  

# except ImportError as e:
#     print ("Erro ao obter dados: ", e)



# try:
#     ENDERECO_DADOS = r'./dados/'

#     #hora de inicio
    
#     print('Obtendo dados')

#     inicio = datetime.now()

#     lista_arquivos = []

#     # Lista final dos arquivos de dados que vieram do diretorio

#     lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

#     #Pegando os arquivos CSVs do diretorio
    
#     for arquivo in lista_dir_arquivos:
#         if arquivo.endswith('.csv'):
#             lista_arquivos.append(arquivo)

#     print(lista_arquivos)

# #Leitura dos arquivos
#     for arquivo in lista_arquivos:
#         print(f'Processando arquivo {arquivo}')

#     #Leitura de cada um dos dataframes
#     df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

#     if 'df_bolsa_familia' in locals():
#        df_bolsa_familia = pl.concat([df_bolsa_familia, df])
#     else:
#         df_bolsa_familia =df
#     #prints
#     print(df_bolsa_familia.head())
#     #print(df_bolsa_familia.shape)
#     #print(df_bolsa_familia.columns)
#     #print(df_bolsa_familia.dtypes)

#     print(f'Arquivo {arquivo} procenssado com sucesso!')

#     #Criar arquivo Parquet
#     df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')


# except ImportError as e:
#     print('Erro ao obter dados: ', e)



try:
    ENDERECO_DADOS = r'./dados/'  # Caminho para o diretório de dados

    print('Obtendo Dados')

    inicio = datetime.now()

    # Ler os arquivos CSV no diretório
    lst_arquivos = []  
    lst_dir = os.listdir(ENDERECO_DADOS)  

    # Filtrar arquivos .csv
    for arquivo in lst_dir:
        if arquivo.endswith('.csv'):  
            lst_arquivos.append(arquivo)  

    # Imprimir a lista de arquivos encontrados (após o loop)
    print(lst_arquivos)

    # Inicializar o DataFrame vazio para concatenar
    df_bolsa_familia = None

    # Processar cada arquivo CSV
    for arquivo in lst_arquivos:
        print(f"Processando arquivo {arquivo}")
    
        # Leitura de cada um dos arquivos CSV
        df = pl.read_csv(os.path.join(ENDERECO_DADOS, arquivo), separator=';', encoding='iso-8859-1')
    
        # Converter a coluna 'VALOR PARCELA' para float
        if "VALOR PARCELA" in df.columns:
            df = df.with_columns(
                pl.col("VALOR PARCELA")
                .str.replace(",", ".")  # Substituir vírgulas por pontos
                .cast(pl.Float64)  # Converter para float
            )

        # Concatenar os DataFrames
        if df_bolsa_familia is not None:
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df
    
        print(df_bolsa_familia)  # Visualizar o DataFrame concatenado

        # Liberar memória após cada iteração
        gc.collect()

    # Exibir o tempo total de execução
    print(f"Tempo total: {datetime.now() - inicio}")

    #limpar df da memoria
    del df
    # residuos da memoria
    gc.collect()

except ImportError as e:
    print("Erro ao obter dados: ", e)

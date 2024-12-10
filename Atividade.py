#import pandas as pd
import polars as pl
from datetime import datetime
import gc


try:
    ENDERECO_DADOS = r'./dados/'

    #hora de inicio
    
    print('Obtendo dados..')
    hora_inicial = datetime.now()

    #carregar dados

    df_feveireiro = pl.read_csv(ENDERECO_DADOS + '202402_NovoBolsaFamilia.csv', separator=';', encoding='iso-8859-1')

    print(df_feveireiro.head())


    #hora final 

    hora_final = datetime.now()

    #print calcular o tempo

    print(f'Tempo de execução: {hora_final - hora_inicial}')


except ImportError as e:
    print('Erro ao obter dados: ', e)

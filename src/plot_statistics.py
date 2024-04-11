"""
    Plot visualizations of the statistics of the dataset statistics.csv
"""

# %% Imports

import os

import dotenv
import pandas as pd 
import matplotlib.pyplot as plt


dotenv.load_dotenv()
CSV_FILEPATH = os.getenv('STATISTICS_FILEPATH')

# %% Assets 
def compute_statitics():
    """
        Compute the statistics of the dataset
    """
    df = pd.read_csv(CSV_FILEPATH, header=0, sep=';')
    df = df.convert_dtypes()
    df['tempo_em_segundos'] = df['tempo_em_segundos'].astype('float64')
    df['tempo_em_minutos'] = (df['tempo_em_segundos'] / 60).round(2)
    return df

def compute_small_tables_statistics(df: pd.DataFrame):
    """
        Compute the statistics of the small tables
    """
    df = df[df['tabela_carregada'].str.contains('small')]
    return df 

def compute_large_tables_statistics(df: pd.DataFrame):
    """
        Compute the statistics of the large tables
    """
    df = df[
            (df['tabela_carregada'].str.contains('large'))
        |   (df['tabela_carregada'].str.contains('bucketed'))
     ]
    return df

# %% Execution
if __name__ == '__main__':
    df = compute_statitics()
    small_tables = compute_small_tables_statistics(df)
    large_tables = compute_large_tables_statistics(df)
    
    # Plotting the statistics for small tables
    ax = small_tables.plot(kind='bar', x='tabela_carregada', y='tempo_em_minutos', title='Tempo de carregamento das tabelas pequenas')

    # Adding the text inside the bars
    for p in ax.patches:
        ax.annotate(str(p.get_height()), (p.get_x() * 1.005, p.get_height() * 1.005))

    plt.show()

    # Plotting the statistics for large tables
    ax = large_tables.plot(kind='bar', x='tabela_carregada', y='tempo_em_minutos', title='Tempo de carregamento das tabelas grandes')

    # Adding the text inside the bars
    for p in ax.patches:
        ax.annotate(str(p.get_height()), (p.get_x() * 1.005, p.get_height() * 1.005))

    plt.show()

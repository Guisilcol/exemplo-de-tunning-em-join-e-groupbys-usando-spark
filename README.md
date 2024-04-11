




## Especificações dos arquivos  

world_cities.csv: 26.467 registros
    Arquivo que armazena uma lista de cidades e seus respectivos países

    - name: string
    - country: string
    - subcountry: string

city_weather.parquet: 150000000 registros
    Arquivo que armazena temperaturas ocorridas em cidades 

    - city: string 
    - temperature: double 
    - id: bigint

city_tags.parquet: 187488292 registros 
    Arquivo que armazena TAGS das temperaturas ocorridas presentes no arquivo city_weather.parquet 

    - city_weather_id: bigint
    - tag: string


## Relacionamento entre os arquivos 

world_cities - 1:N - city_weather
city_weather - 1:N - city_tags

Os testes considerarão as seguintes condições nos joins (apenas Joins equivalentes):

    1. city_weather.id == city_tags.city_weather_id
    2. city_weather.city == world_cities.name


## Tipos de joins suportados

https://miro.medium.com/v2/resize:fit:1400/format:webp/1*fZA9FNEL8e2MSyXqF_glXQ.png


## Tempo 

Join entre tabela grande e pequena:
    -   tabela: broadcast_small_join_table
        tempo: 339.1755319369986
        tipo_join: broadcast

    -   tabela: shuffle_hash_small_join_table
        tempo: 391.7438182889964
        tipo_join: shuffle_hash

    -   tabela: shuffle_merge_small_join_table
        tempo: 425.4038099460013
        tipo_join: shuffle_merge

Join entre tabelas grandes:
    -   tabela: shuffle_hash_large_join_table
        tempo: 575.2981968640015
        tipo_join: shuffle_hash
    
    -   tabela: shuffle_merge_large_join_table
        tempo: 573.3632849860005
        tipo_join: shuffle_merge
    
    -   tabela: shuffle_merge_bucketed_join_table
        tempo: 504.77595568200195
        tipo_join: shuffle_merge

OBS: Não há broadcast_large_join_table, porque o tamanho da tabela é muito grande para ser broadcasted.



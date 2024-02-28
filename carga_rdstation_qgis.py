#IMPORTANDO BIBLIOTECAS

import pandas as pd
import requests
import sqlalchemy
import psycopg2
from sqlalchemy import create_engine
from pandas import json_normalize


#CONFIGURANDO CONEXÃO COM O POSTGRESQL

conn_string = 'postgresql://braulio:8bVnSiPBK1Xh@ep-bold-term-a5ihew27.us-east-2.aws.neon.tech/PFC_Florestal_Captacao'



db = create_engine(conn_string)
conn = db.connect()

conn1 = psycopg2.connect(
    database="PFC_Florestal_Captacao",
  user='braulio',
  password='8bVnSiPBK1Xh',
  host='ep-bold-term-a5ihew27.us-east-2.aws.neon.tech',
  port= '5432'
)


#TRATAMENTO DA BASE DE ORGANIZAÇÕES - BRONZE

url_base = "https://crm.rdstation.com/api/v1/organizations"
token = "6525530ac1344a001019b9d8"
headers = {"accept": "application/json"}
page = 1
limit = 200
all_results = []
while True:
    # URL com os parâmetros da página atual
    url = f"{url_base}?token={token}&page={page}&limit={limit}"
    # Faça a solicitação à API
    response = requests.get(url, headers=headers)
    # Validar solicitação
    if response.status_code == 200:
        # Adicione os resultados à lista geral
        response_json = response.json()
        # Normaliza o campo 'organizations' e expande os campos 'custom_fields'
        org_df = json_normalize(response_json["organizations"])
        # Cria um DataFrame para cada entrada em 'custom_fields' replicando '_id'
        custom_fields_dfs = [json_normalize(custom_field).assign(_id=org_id) for org_id, custom_fields in zip(org_df['_id'], org_df['custom_fields']) for custom_field in custom_fields]
        # Concatena os DataFrames criados
        custom_fields_df = pd.concat(custom_fields_dfs, ignore_index=True)
        # Adiciona colunas específicas para cada campo em custom_fields
        final_df = pd.merge(org_df, custom_fields_df, how='left', on='_id')
        # Adiciona o DataFrame resultante à lista
        all_results.append(final_df)
        # Verifica se há mais páginas
        if len(response_json["organizations"]) < limit:
            break  # Se não houver mais páginas, saia do loop
        else:
            page += 1  # Se houver mais páginas, incremente o número da página
    else:
        print(f"Falha na solicitação. Código de status: {response.status_code}")
        break  # Saia do loop em caso de falha
# Concatene todos os DataFrames em um único DataFrame
final_result_df = pd.concat(all_results, ignore_index=True)



#Seleciona somente as colunas necessárias
final_result_df = final_result_df[['id', 'name', 'custom_field.label', 'value', 'created_at_x', 'updated_at_x']]


#Estabelece cursor para inserir dados no PostgreSql
cursor = conn1.cursor()

#Converte o Dataframe para Tabela do PostgreSql
final_result_df.to_sql('tb_organizacoes_rd_station', conn, if_exists= 'replace', schema='bronze_layer')

#Confirma a transação
#conn.commit()



#INSERE DADOS DA ORGANIZAÇÃO NA SILVER LAYER

#Encapsula o comando sql para popular a tabela de organizações na silver layer
var = """
INSERT INTO silver_layer.tb_organizacoes_rd_station
select id as company_id, name, cast(created_at_x as TIMESTAMP) as created_at, cast(updated_at_x as timestamp) as updated_at
, max(case when ("custom_field.label" = 'ID') then value else null end) as id
, max(case when ("custom_field.label" = 'CIDADE') then value else null end) as cidade
, max(case when ("custom_field.label" = 'CAR ou INCRA') then value else null end) as car_incra
, max(case when ("custom_field.label" = 'ÁREA ÚTIL') then value else null end) as area_util
, max(case when ("custom_field.label" = 'VOLUME CALCULADO') then value else null end) as volume_calculado
, max(case when ("custom_field.label" = 'PROPRIETÁRIO') then value else null end) as proprietario
, max(case when ("custom_field.label" = 'LAND VIEWER') then value else null end) as landviewer
, max(case when ("custom_field.label" = 'REGISTRO RURAL') then value else null end) as registro_rural
, max(case when ("custom_field.label" = 'GOOGLE MAPS') then value else null end) as google_maps
,now() as dt_log
, max(case when ("custom_field.label" = 'COORDENADA') then value else null end) as coordenada
from bronze_layer.tb_organizacoes_rd_station
group by
id, name, cast(created_at_x as TIMESTAMP), cast(updated_at_x as timestamp)
"""


#Trunca a tabela na silver layer
conn.execute(sqlalchemy.text('TRUNCATE TABLE silver_layer.tb_organizacoes_rd_station').execution_options(autocommit=True))

#Confirma a transação
#conn.commit()

#Usa o comando encapsulado anteriormente para popular a tabela
conn.execute(sqlalchemy.text(var).execution_options(autocommit=True))

#Confirma a transação
#conn.commit()


#EXPORTA PARA CSV (PARA VALIDAÇÃO)


#from google.colab import files
#org_df.to_csv('org_df.csv', encoding = 'utf-8-sig')
#files.download('org_df.csv')

#contacts_df.to_csv('contacts_df.csv', encoding = 'utf-8-sig')
#files.download('contacts_df.csv')


#custom_fields_df.to_csv('custom_fields_df.csv', encoding = 'utf-8-sig')
#files.download('custom_fields_df.csv')


#TRATAMENTO DA BASE DE FUNIS - BRONZE


url_base = "https://crm.rdstation.com/api/v1/deal_pipelines"
token = "6525530ac1344a001019b9d8"
headers = {"accept": "application/json"}
page = 1
limit = 200

# Prepara a url para o GET
url = f"{url_base}?token={token}&page={page}&limit={limit}"

# Consulta a URL com GET tratando cabeçalhos
response = requests.get(url, headers=headers)

# Verifica se a solicitação foi bem-sucedida
if response.status_code == 200:
    # Obtém os dados em formato JSON
    response_json = response.json()

    # Inicializa uma lista para armazenar os dados tabulares
    tabular_data = []

    # Itera sobre os elementos do JSON
    for item in response_json:
        # Obtém os dados do deal_stage
        deal_stages = item.get('deal_stages', [])

        # Itera sobre os deal_stages
        for deal_stage in deal_stages:
            # Cria uma entrada tabular para cada deal_stage
            entry = {
                'Name': item.get('name'),
                'DealStage_ID': deal_stage.get('_id'),
                'DealStage_Name': deal_stage.get('name'),
            }
            # Adiciona a entrada à lista
            tabular_data.append(entry)

    # Converte a lista de dicionários em DataFrame
    df_tabular = pd.DataFrame(tabular_data)

    # Exibe o DataFrame tabular
    #print(df_tabular)

else:
    print(f"Erro na solicitação: {response.status_code}")


#Estabelece cursor para inserir dados no PostgreSql
cursor = conn1.cursor()

#Converte o Dataframe para Tabela do PostgreSql
df_tabular.to_sql('tb_funis_rd_station', conn, if_exists= 'replace', schema='bronze_layer')


#TRATAMENTO DA BASE DE ETAPAS DO FUNIL - BRONZE

url_base = "https://crm.rdstation.com/api/v1/deal_stages"
token = "6525530ac1344a001019b9d8"
headers = {"accept": "application/json"}
page = 1
limit = 200
all_results = []

#Prepara a url para o GET
url = f"{url_base}?token={token}&page={page}&limit={limit}"

#Consulta a URL com GET tratando cabeçalhos
response = requests.get(url, headers=headers)

#Formata o resultado em json
response_json = response.json()

#Normaliza o json para Dataframe
df_funil = json_normalize(response_json["deal_stages"])

#Seleciona somente as colunas necessárias
df_funil = df_funil[['id', 'name', 'created_at', 'updated_at']]

#Estabelece cursor para inserir dados no PostgreSql
cursor = conn1.cursor()

#Converte o Dataframe para Tabela do PostgreSql
df_funil.to_sql('tb_etapas_funil_rd_station', conn, if_exists= 'replace', schema='bronze_layer')

#Confirma a transação
#conn.commit()


#TRATAMENTO DA BASE DE NEGOCIAÇÕES - BRONZE

url_base = "https://crm.rdstation.com/api/v1/deals"
token = "6525530ac1344a001019b9d8"
headers = {"accept": "application/json"}
page = 1
limit = 200
all_results = []
while True:
    # URL com os parâmetros da página atual
    url = f"{url_base}?token={token}&page={page}&limit={limit}"
    # Faça a solicitação à API
    response = requests.get(url, headers=headers)
    # Validar solicitação
    if response.status_code == 200:
        # Adicione os resultados à lista geral
        response_json = response.json()
        # Normaliza o campo 'organizations' e expande os campos 'custom_fields'
        org_df = json_normalize(response_json["deals"])
        # Adiciona o DataFrame resultante à lista
        all_results.append(org_df)
        # Verifica se há mais páginas
        if len(response_json["deals"]) < limit:
            break  # Se não houver mais páginas, saia do loop
        else:
            page += 1  # Se houver mais páginas, incremente o número da página
    else:
        print(f"Falha na solicitação. Código de status: {response.status_code}")
        break  # Saia do loop em caso de falha
# Concatene todos os DataFrames em um único DataFrame
final_result_df = pd.concat(all_results, ignore_index=True)

#Seleciona as colunas necessárias
final_result_df = final_result_df[['id', 'name', 'last_activity_at', 'interactions', 'win', 'closed_at', 'organization.id', 'organization.name', 'user.name', 'deal_stage.id', 'deal_stage.name', 'deal_lost_reason.name', 'created_at', 'updated_at']]


#Estabelece cursor para inserir dados no PostgreSql
cursor = conn1.cursor()

#Converte o Dataframe para Tabela do PostgreSql
final_result_df.to_sql('tb_negociacoes_rd_station', conn, if_exists= 'replace', schema='bronze_layer')

#Confirma a transação
#conn.commit()


#INSERE DADOS DA NEGOCIAÇÃO NA SILVER LAYER


#Encapsula o comando sql para popular a tabela de negociações na silver layer
var = """
insert INTO silver_layer.tb_negociacoes_rd_station
select
id
,"name"
,cast(last_activity_at as TIMESTAMP) as last_activity_at
,interactions
,win
,cast(closed_at as TIMESTAMP) as closed_at
,"organization.id" as organization_id
,"organization.name" as organization_name
,"user.name" as user_name
,"deal_stage.id" as stage_id
,"deal_stage.name" as stage_name
,"deal_lost_reason.name" as lost_reason
,cast(created_at as timestamp) as created_at
,cast(updated_at as timestamp) as updated_at
,f."Name" AS funnel_name
from bronze_layer.tb_negociacoes_rd_station n
LEFT JOIN
(SELECT DISTINCT "Name", 	"DealStage_ID" FROM bronze_layer.tb_funis_rd_station) f
ON n."deal_stage.id" = f."DealStage_ID"
"""


#Trunca a tabela na silver layer
conn.execute(sqlalchemy.text('TRUNCATE TABLE silver_layer.tb_negociacoes_rd_station').execution_options(autocommit=True))

#Confirma a transação
#conn.commit()

#Usa o comando encapsulado anteriormente para popular a tabela
conn.execute(sqlalchemy.text(var).execution_options(autocommit=True))

#Confirma a transação
#conn.commit()


#CARGA BASE TAREFAS - BRONZE LAYER


url_base = "https://crm.rdstation.com/api/v1/tasks"
token = "6525530ac1344a001019b9d8"
headers = {"accept": "application/json"}
page = 1
limit = 200
all_results = []
while True:
    # URL com os parâmetros da página atual
    url = f"{url_base}?token={token}&page={page}&limit={limit}"
    # Faça a solicitação à API
    response = requests.get(url, headers=headers)
    # Validar solicitação
    if response.status_code == 200:
        # Adicione os resultados à lista geral
        response_json = response.json()
        # Normaliza o campo 'organizations' e expande os campos 'custom_fields'
        task_df = json_normalize(response_json["tasks"])
        # Adiciona o DataFrame resultante à lista
        all_results.append(task_df)
        # Verifica se há mais páginas
        if len(response_json["tasks"]) < limit:
            break  # Se não houver mais páginas, saia do loop
        else:
            page += 1  # Se houver mais páginas, incremente o número da página
    else:
        print(f"Falha na solicitação. Código de status: {response.status_code}")
        break  # Saia do loop em caso de falha
# Concatene todos os DataFrames em um único DataFrame
final_result_df = pd.concat(all_results, ignore_index=True)


#Seleciona somente as colunas necessárias
final_result_df = final_result_df[['id', 'subject', 'type', 'markup', 'done', 'notes', 'deal_id', 'done_date', 'created_at', 'date']]


#Estabelece cursor para inserir dados no PostgreSql
cursor = conn1.cursor()

#Converte o Dataframe para Tabela do PostgreSql
final_result_df.to_sql('tb_tarefas_rd_station', conn, if_exists= 'replace', schema='bronze_layer')

#Confirma a transação
#conn.commit()


#CRIA A BASE FINAL NA GOLD LAYER

#Encapsula o comando sql para popular a tabela de negociações na gold layer
var = """
insert into gold_layer.tb_negociacoes_qgis
with cte as

(
select
A.id_organizacao
,A.nome_organizacao
,A.cidade
,A.car_incra
,A.area_util
,A.volume_calculado
,A.proprietario
,A.landviewer
,A.registro_rural
,A.google_maps,A.coordenada_completa
,A.nome_negociacao
,A.responsavel
,A.etapa,A.motivo_perda
,A.dt_criacao_negociacao
,A.dt_atualizacao_negociacao
,A.qtde_tarefas
,A.dt_carga
,cast(A.latitude as numeric) as latitude
,cast(A.longitude as numeric) as longitude
,A.funnel_name
from 
(
with cte as
(
select distinct
cpn.id as id_organizacao
,cpn.name as nome_organizacao
,cpn.cidade
,cpn.car_incra
,cpn.area_util
,cpn.volume_calculado
,cpn.proprietario
,cpn.landviewer
,cpn.registro_rural
,cpn.google_maps
,case when POSITION(',' IN cpn.registro_rural) = 0 then cpn.coordenada else substring(cpn.registro_rural, POSITION('-' IN cpn.registro_rural) ) end as coordenada_completa
,ngc.name as nome_negociacao
,ngc.user_name as responsavel
,ngc.stage_name as etapa
,ngc.lost_reason as motivo_perda
,ngc.created_at as dt_criacao_negociacao
,ngc.updated_at as dt_atualizacao_negociacao
,ngc.funnel_name
,tasks.qtde_tarefas
,cpn.dt_log as dt_carga

from silver_layer.tb_organizacoes_rd_station cpn
left join silver_layer.tb_negociacoes_rd_station ngc
on cpn.company_id = ngc.organization_id
left join 
(
select
deal_id, COUNT(*) as qtde_tarefas
from bronze_layer.tb_tarefas_rd_station
group by deal_id
) tasks
on ngc.id = tasks.deal_id 
)
select c.*
,case when POSITION('-' IN c.coordenada_completa) = 0 then NULL else
replace( substring(c.coordenada_completa, POSITION('-' IN c.coordenada_completa), POSITION(',' IN c.coordenada_completa) ), ',', '' ) end as latitude
,replace(case when POSITION('-' IN c.coordenada_completa) = 0 then NULL else
substring(c.coordenada_completa, POSITION(',' IN c.coordenada_completa) +1, 10000) end, ',','.') as longitude
from cte c

) A


)
select 
cte.id_organizacao
,cte.nome_organizacao
,cte.cidade
,cte.car_incra
,cte.area_util
,cte.volume_calculado
,cte.proprietario
,cte.landviewer
,cte.registro_rural
,cte.google_maps,cte.coordenada_completa
,cte.nome_negociacao
,cte.responsavel
,cte.etapa,cte.motivo_perda
,cte.dt_criacao_negociacao
,cte.dt_atualizacao_negociacao
,cte.qtde_tarefas
,cte.dt_carga
,cte.latitude
,cte.longitude
,ST_SetSRID(ST_MakePoint(longitude, latitude),4326)
,cte.funnel_name AS nome_funil
,m.geom
from cte
LEFT JOIN stage.mapeamento_florestas m
ON cte.id_organizacao = CAST(m.id_organizacao AS VARCHAR)
WHERE cte.funnel_name = 'Em Negociação'
"""


#Trunca a tabela na silver layer
conn.execute(sqlalchemy.text('TRUNCATE TABLE gold_layer.tb_negociacoes_qgis').execution_options(autocommit=True))

#Confirma a transação
#conn.commit()

#Usa o comando encapsulado anteriormente para popular a tabela
conn.execute(sqlalchemy.text(var).execution_options(autocommit=True))


#CRIA A BASE FINAL NA GOLD LAYER

#Encapsula o comando sql para popular a tabela de negociações na gold layer
var = """
insert into gold_layer.tb_captacoes_qgis
with cte as

(
select
A.id_organizacao
,A.nome_organizacao
,A.cidade
,A.car_incra
,A.area_util
,A.volume_calculado
,A.proprietario
,A.landviewer
,A.registro_rural
,A.google_maps,A.coordenada_completa
,A.nome_negociacao
,A.responsavel
,A.etapa,A.motivo_perda
,A.dt_criacao_negociacao
,A.dt_atualizacao_negociacao
,A.qtde_tarefas
,A.dt_carga
,cast(A.latitude as numeric) as latitude
,cast(A.longitude as numeric) as longitude
,A.funnel_name
from 
(
with cte as
(
select distinct
cpn.id as id_organizacao
,cpn.name as nome_organizacao
,cpn.cidade
,cpn.car_incra
,cpn.area_util
,cpn.volume_calculado
,cpn.proprietario
,cpn.landviewer
,cpn.registro_rural
,cpn.google_maps
,case when POSITION(',' IN cpn.registro_rural) = 0 then cpn.coordenada else substring(cpn.registro_rural, POSITION('-' IN cpn.registro_rural) ) end as coordenada_completa
,ngc.name as nome_negociacao
,ngc.user_name as responsavel
,ngc.stage_name as etapa
,ngc.lost_reason as motivo_perda
,ngc.created_at as dt_criacao_negociacao
,ngc.updated_at as dt_atualizacao_negociacao
,ngc.funnel_name
,tasks.qtde_tarefas
,cpn.dt_log as dt_carga

from silver_layer.tb_organizacoes_rd_station cpn
left join silver_layer.tb_negociacoes_rd_station ngc
on cpn.company_id = ngc.organization_id
left join 
(
select
deal_id, COUNT(*) as qtde_tarefas
from bronze_layer.tb_tarefas_rd_station
group by deal_id
) tasks
on ngc.id = tasks.deal_id 
)
select c.*
,case when POSITION('-' IN c.coordenada_completa) = 0 then NULL else
replace( substring(c.coordenada_completa, POSITION('-' IN c.coordenada_completa), POSITION(',' IN c.coordenada_completa) ), ',', '' ) end as latitude
,replace(case when POSITION('-' IN c.coordenada_completa) = 0 then NULL else
substring(c.coordenada_completa, POSITION(',' IN c.coordenada_completa) +1, 10000) end, ',','.') as longitude
from cte c

) A


)
select 
cte.id_organizacao
,cte.nome_organizacao
,cte.cidade
,cte.car_incra
,cte.area_util
,cte.volume_calculado
,cte.proprietario
,cte.landviewer
,cte.registro_rural
,cte.google_maps,cte.coordenada_completa
,cte.nome_negociacao
,cte.responsavel
,cte.etapa,cte.motivo_perda
,cte.dt_criacao_negociacao
,cte.dt_atualizacao_negociacao
,cte.qtde_tarefas
,cte.dt_carga
,cte.latitude
,cte.longitude
,ST_SetSRID(ST_MakePoint(longitude, latitude),4326)
,cte.funnel_name AS nome_funil
,m.geom
from cte
LEFT JOIN stage.mapeamento_florestas m
ON cte.id_organizacao = CAST(m.id_organizacao AS VARCHAR)
WHERE cte.funnel_name = 'Captação Florestal'
"""


#Trunca a tabela na silver layer
conn.execute(sqlalchemy.text('TRUNCATE TABLE gold_layer.tb_captacoes_qgis').execution_options(autocommit=True))

#Confirma a transação
#conn.commit()

#Usa o comando encapsulado anteriormente para popular a tabela
conn.execute(sqlalchemy.text(var).execution_options(autocommit=True))

#Confirma a transação
#conn.commit()

#Encerra a conexão
conn.close()
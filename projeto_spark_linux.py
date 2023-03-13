from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import requests, json, warnings, io, time, os ,pandas as pd, shutil, stat, rarfile
from datetime import date, datetime
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
import wget
import patoolib

spark = SparkSession.builder.config("spark.jars", "/usr/share/java/mysql-connector-j-8.0.32.jar").getOrCreate() #Iniciando a sessão Spark

def covid_spark():
    #Minerando a URL principal para obter a URL dos arquivos do dia corrente
    url = "https://qd28tcd6b5.execute-api.sa-east-1.amazonaws.com/prod/PortalGeral"
    head = {"X-Parse-Application-Id": "unAFkcaNDeXajurGB7LChj8SgQYS2ptm"}
    req = requests.get(url, headers=head)
    c_json = json.loads(req.content)
    url2 = c_json["results"][0]["arquivo"]["url"]
    m = url2[136:139]

    #Baixando os CSV's através da URL minerada, e extraindo para a pasta no diretório local
    warnings.filterwarnings('ignore')
    response = requests.get(url2, verify = False, stream = True)
    with rarfile.RarFile(io.BytesIO(response.content)) as rf:
        rf.extractall(fr'/opt/airflow/files')

    #Listando os arquivos baixados para facilitar na hora de carregar os CSV's
    dir = os.listdir("/opt/airflow/files")

    #Carregando os CSV's já baixados e substituindo os valores N/A por "Não Informado" para facilitar no tratamento dos arquivos
    csv20p1 = spark.read.csv(fr"/opt/airflow/files/{dir[0]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
    csv20p2 = spark.read.csv(fr"/opt/airflow/files/{dir[1]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
    csv21p1 = spark.read.csv(fr"/opt/airflow/files/{dir[2]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
    csv21p2 = spark.read.csv(fr"/opt/airflow/files/{dir[3]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
    csv22p1 = spark.read.csv(fr"/opt/airflow/files/{dir[4]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
    csv22p2 = spark.read.csv(fr"/opt/airflow/files/{dir[5]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
    csv23p1 = spark.read.csv(fr"/opt/airflow/files/{dir[6]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")

    #Função para concatenar os DF's já excluindo as linhas que estão como "Não informado" na coluna "codmun", fiz esse tratamento para evitar informações desnecessárias
    def unionAll(*dfs):
        return reduce(DataFrame.unionAll, dfs)

    dff = unionAll(csv20p1.filter(csv20p1.codmun != "Não informado"), csv20p2.filter(csv20p2.codmun != "Não informado"), csv21p1.filter(csv21p1.codmun != "Não informado"), csv21p2.filter(csv21p2.codmun != "Não informado")
               , csv22p1.filter(csv22p1.codmun != "Não informado"), csv22p2.filter(csv22p2.codmun != "Não informado"), csv23p1.filter(csv23p1.codmun != "Não informado"))

    dff.head(20)
    #Ecluindo colunas desnecessárias
    dff = dff.drop(*("codRegiaoSaude", "nomeRegiaoSaude", "interior/metropolitana","Recuperadosnovos", "emAcompanhamentoNovos"))

    #Enviando os dados para o Banco de Dados Mysql sobreescrevendo os dados
    dff.write.format('jdbc').option("url",'jdbc:mysql://localhost:3306/covid_datasus').option("driver",'com.mysql.jdbc.Driver').option("dbtable",'covid').option(
        "user",'root').option("password",'***').mode('overwrite').save()


with DAG('projeto_covid_spark', start_date = datetime(2023,3,9),
    schedule_interval="* 5 * * *", catchup=False) as dag:

    covid_spark = PythonOperator(
         task_id = 'covid_spark',
         python_callable = covid_spark
    )

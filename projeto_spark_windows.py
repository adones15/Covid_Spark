import requests, rarfile, json, warnings, io, time, os ,pandas as pd, shutil, stat
from datetime import date
from functools import reduce
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.config("spark.driver.extraClassPath", "C:\Spark\mysql-connector-j-8.0.31.jar").getOrCreate() #Iniciando a sessão Spark

#Limpando o diretório, excluindo os CSV's do dia anterior para receber os novos do dia corrente 
dir1 = r"G:\Projetos_Python\Spark\csvs"
for i in os.listdir(dir1):
    os.remove(os.path.join(dir1, i))

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
file = rarfile.RarFile(io.BytesIO(response.content))
file.extractall(r"G:Projetos_Python\Spark\csvs")

#Listando os arquivos baixados para facilitar na hora de carregar os CSV's
dir = os.listdir(r"G:\Projetos_Python\Spark\csvs")

#Carregando os CSV's já baixados e substituindo os valores N/A por "Não Informado" para facilitar no tratamento dos arquivos
csv20p1 = spark.read.csv(fr"G:\Projetos_Python\Spark\csvs\{dir[0]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
csv20p2 = spark.read.csv(fr"G:\Projetos_Python\Spark\csvs\{dir[1]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
csv21p1 = spark.read.csv(fr"G:\Projetos_Python\Spark\csvs\{dir[2]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
csv21p2 = spark.read.csv(fr"G:\Projetos_Python\Spark\csvs\{dir[3]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
csv22p1 = spark.read.csv(fr"G:\Projetos_Python\Spark\csvs\{dir[4]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
csv22p2 = spark.read.csv(fr"G:\Projetos_Python\Spark\csvs\{dir[5]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")
csv23p1 = spark.read.csv(fr"G:\Projetos_Python\Spark\csvs\{dir[6]}", encoding='utf-8', sep=';', header=True).fillna("Não informado")

#Função para concatenar os DF's já excluindo as linhas que estão como "Não informado" na coluna "codmun", fiz esse tratamento para evitar informações desnecessárias
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

dff = unionAll(csv20p1.filter(csv20p1.codmun != "Não informado"), csv20p2.filter(csv20p2.codmun != "Não informado"), csv21p1.filter(csv21p1.codmun != "Não informado"), csv21p2.filter(csv21p2.codmun != "Não informado")
               , csv22p1.filter(csv22p1.codmun != "Não informado"), csv22p2.filter(csv22p2.codmun != "Não informado"), csv23p1.filter(csv23p1.codmun != "Não informado"))

#Ecluindo colunas desnecessárias
dff = dff.drop(*("codRegiaoSaude", "nomeRegiaoSaude", "interior/metropolitana","Recuperadosnovos", "emAcompanhamentoNovos"))

#Passando os dados do DF final já tratado para o BD passando como "overwrite" para sempre sobreescrever os dados antigos e ficar com os dados atualizados do dia
dff.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/covid_datasus',driver='com.mysql.jdbc.Driver',dbtable='covid',user='root',password='***').mode('overwrite').save()


import requests, rarfile, io, warnings, json, pandas as pd, shutil, os, stat, sqlalchemy as sq, pymysql, time
from datetime import date
start = time.time()

#removendo o diretório antigo do dia anterior para criarmos o novo diretório do dia atual
def remove_readonly(func, path, _):
    os.chmod(path, stat.S_IWRITE)
    func(path)
shutil.rmtree(r"G:\Projetos_Python\Spark\csvs", onerror=remove_readonly)

#obtendo a URL atual
url = "https://qd28tcd6b5.execute-api.sa-east-1.amazonaws.com/prod/PortalGeral"
head = {"X-Parse-Application-Id": "unAFkcaNDeXajurGB7LChj8SgQYS2ptm"}
req = requests.get(url, headers=head)
c_json = json.loads(req.content)
url2 = c_json["results"][0]["arquivo"]["url"]
m = url2[136:139]

#descompactando os arquivos
warnings.filterwarnings('ignore')
response = requests.get(url2, verify = False, stream = True)
file = rarfile.RarFile(io.BytesIO(response.content))
file.extractall(r"G:\Projetos_Python\Spark\csvs")

#Obtendo os valores referente as datas
d_h = date.today().strftime("%d/%m/%Y")
dia = int(d_h[0:2])
mes = d_h[3:5]
ano = d_h[6:10]

#Tratando sábado e domingo
DIAS = ['Segunda-feira','Terça-feira','Quarta-feira','Quinta-Feira','Sexta-feira','Sábado','Domingo']
data = date.today()
dia_v = 0
i_semana = data.weekday()
d_semana = DIAS[i_semana]
if d_semana == "Sábado":
    dia_v = dia - 1
elif d_semana == "Domingo":
    dia_v = dia - 2
else:
    dia_v = dia

#carregando os csv's extraidos
csv20p2 = pd.read_csv(fr"G:\Projetos_Python\Spark\csvs\HIST_PAINEL_COVIDBR_2020_Parte2_10{m}{ano}.csv", encoding='utf-8', sep=';')
csv21p1 = pd.read_csv(fr"G:\Projetos_Python\Spark\csvs\HIST_PAINEL_COVIDBR_2021_Parte1_10{m}{ano}.csv", encoding='utf-8', sep=';')
csv21p2 = pd.read_csv(fr"G:\Projetos_Python\Spark\csvs\HIST_PAINEL_COVIDBR_2021_Parte2_10{m}{ano}.csv", encoding='utf-8', sep=';')
csv20p1 = pd.read_csv(fr"G:\Projetos_Python\Spark\csvs\HIST_PAINEL_COVIDBR_2020_Parte1_10{m}{ano}.csv", encoding='utf-8', sep=';')
csv22p1 = pd.read_csv(fr"G:\Projetos_Python\Spark\csvs\HIST_PAINEL_COVIDBR_2022_Parte1_10{m}{ano}.csv", encoding='utf-8', sep=';')
csv22p2 = pd.read_csv(fr"G:\Projetos_Python\Spark\csvs\HIST_PAINEL_COVIDBR_2022_Parte2_10{m}{ano}.csv", encoding='utf-8', sep=';')
csv23p1 = pd.read_csv(fr"G:\Projetos_Python\Spark\csvs\HIST_PAINEL_COVIDBR_2022_Parte2_10{m}{ano}.csv", encoding='utf-8', sep=';')

#função para excluir linhas desnecessárias
def del_linha(df, coluna, valor):
    index_n = df[df[coluna] == valor].index
    df.drop(index_n, inplace=True)

#função para identificar e excluir as linhas desnecessárias
def norm_base(df):
    df.fillna("Não informado", inplace=True)
    del_linha(df, "codmun", "Não informado")

#executando a função para excluir as linhas desnecessárias
norm_base(csv20p1)
norm_base(csv20p2)
norm_base(csv21p1)
norm_base(csv21p2)
norm_base(csv22p1)
norm_base(csv22p2)
norm_base(csv23p1)

#juntando todos os df's em um só
dff = pd.concat([csv20p1, csv20p2, csv21p1, csv21p2, csv22p1, csv22p2, csv23p1])

#excluindo algumas colunas que não tem muita relevância
dff.drop(columns=["codRegiaoSaude", "nomeRegiaoSaude", "interior/metropolitana",
                 "Recuperadosnovos", "emAcompanhamentoNovos"], inplace=True)
senha_bd = "***"

#enviando os dados do df para o mysql
con = sq.create_engine(f"mysql+pymysql://root:{senha_bd}@localhost/covid_datasus")
dff.to_sql("covid", con, if_exists="replace", index=False)

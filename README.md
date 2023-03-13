# Covid_Spark
Projeto desenvolvido para automatizar a consulta dos dados do COVID no Brasil, obtidos do site do Governo federal.

Para o o desenvolvimento foram utilizadas as ferramentas: Python, Pyspark, Apache Hadoop, Apache Airflow, Docker e MySQL.

O Script basicamente faz um web scraping no site do governo federal acessando uma URL fixam e obtendo a URL que é alterada toda vez que os dados são atualizados, com isso o script faz a extração dos CSV's do arquivo .rar e salva no container, logo após é feita a leitura dos CSV's, já fazendo alguns tratamentos, no final é feita uma concatenação dos DF's e enviado para o banco de dados MySQL sobreescrevendo os dados da tebela.

Inicialmente eu desenvolvi o código utilizando apenas Python / Pandas na primeira edição do projeto, já na segunda edição eu queria melhorar o desempenho do código, então resolvi substituir Pandas por Pyspark.

A melhora em desempenho foi significativa, o código em Pandas demorou cerca de 5 minutos e meio para ser executado completamente, já a segunda edição utilizando Pyspark e Hadoop demorou certa de 3 minutos, um ganho de 2 minutos e meio, além do desempenho em tempo, tive um ganho em código, pois em Pyspark ficou bem mais curto.

Após fazer o teste localmente dos dois códigos, resolvi colocar o código em Spark no Airflow para agendar o código para ser executado uma vez por dia, e para isso criei um container Docker para receber o Airflow e agendar a execução do script.

Anexo estão os códigos em Spark e Pandas para teste de desempenho e a foto mostrando o resultado dos testes, bem como o código em Spark adaptado para Linux para poder rodar no Airflow pelo Docker

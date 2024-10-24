# 1. Usando DataFrames com operações mais complexas
# 1.1. Criando um DataFrame a partir de um dicionário

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExemplosIntermediarios").getOrCreate()

data = [
    {"nome": "Alice", "idade": 30, "cidade": "São Paulo"},
    {"nome": "Bob", "idade": 25, "cidade": "Rio de Janeiro"},
    {"nome": "Cathy", "idade": 35, "cidade": "São Paulo"},
]

df = spark.createDataFrame(data)
df.show()

# 1.2. Usando funções agregadas
# Você pode usar funções agregadas para realizar cálculos em colunas:

from pyspark.sql import functions as F

# Média de idades por cidade
media_idade = df.groupBy("cidade").agg(F.avg("idade").alias("media_idade"))
media_idade.show()

# 2. Manipulação de Strings
# Você pode realizar várias operações em colunas de string:

# Converter nomes para maiúsculas
df = df.withColumn("nome_maiusculo", F.upper(df.nome))
df.show()


# 3. Trabalhando com DataFrames grandes
# 3.1. Parquet
# Leia e escreva arquivos Parquet, que são otimizados para grandes conjuntos de dados:

# Salvando como Parquet
df.write.parquet(".arquivo_saida.parquet")

# Lendo um arquivo Parquet
df_parquet = spark.read.parquet("./arquivo_saida.parquet")
df_parquet.show()

# 4. Juntando DataFrames
# Você pode combinar dados de diferentes DataFrames usando joins:

data2 = [
    {"nome": "Alice", "profissao": "Engenheira"},
    {"nome": "Bob", "profissao": "Médico"},
]

df_profissao = spark.createDataFrame(data2)
df_join = df.join(df_profissao, on="nome", how="inner")
df_join.show()

# 5. Trabalhando com dados temporais
# 5.1. Manipulação de datas
# Se você tiver dados com timestamps, pode manipulá-los:

from pyspark.sql.functions import to_date

# Adicionando uma coluna de data
df = df.withColumn("data_registro", to_date(F.lit("2023-01-01")))
df.show()

# 6. UDFs (User Defined Functions)
# Você pode criar funções definidas pelo usuário para operações mais complexas:

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def idade_em_meses(idade):
    return idade * 12


udf_idade_em_meses = udf(idade_em_meses, IntegerType())

df = df.withColumn("idade_em_meses", udf_idade_em_meses(df.idade))
df.show()

# 7. Persistência de dados
# Você pode persistir DataFrames na memória ou disco para otimizar o desempenho

# Persistindo em memória
df.persist()

# Para liberar a memória
df.unpersist()

# 8. Visualizando dados
# Para visualizar dados de forma mais amigável, você pode usar bibliotecas como matplotlib
# ou seaborn em combinação com PySpark. Primeiro, você precisa coletar os dados do DataFrame:

import pandas as pd
import matplotlib.pyplot as plt

# Coletando dados para o Pandas
pdf = df.toPandas()

# Criando um gráfico
pdf.plot(kind="bar", x="nome", y="idade")
plt.show()

# 9. Finalizando a sessão
# Por fim, não esqueça de parar a sessão:

spark.stop()

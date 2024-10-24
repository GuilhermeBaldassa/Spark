# 1. Pipeline de Machine Learning
# 1.1. Criação de um Pipeline

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
from pyspark.sql import Window
import pandas as pd
from pyspark.ml import Pipeline
from graphframes import GraphFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ExemplosAvancados").getOrCreate()

# Suponha que temos um DataFrame com dados de entrada
data = [
    (0, 1.0, 0.0),
    (1, 0.0, 1.0),
    (2, 1.0, 1.0),
]
columns = ["id", "feature1", "feature2"]

df = spark.createDataFrame(data, columns)

# Montando os dados em um vetor
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")

# Modelo de Regressão Logística
lr = LogisticRegression(featuresCol="features", labelCol="id")

# Criando um Pipeline
pipeline = Pipeline(stages=[assembler, lr])

# Ajustando o modelo
model = pipeline.fit(df)

# 2. Análise de Dados em Tempo Real com Structured Streaming

# Lendo dados em tempo real de um arquivo (exemplo)
input_stream = (
    spark.readStream.format("csv")
    .option("header", "true")
    .load("caminho/para/seus/dados/")
)

# Realizando algumas transformações
query = (
    input_stream.groupBy("coluna_de_interesse")
    .count()
    .writeStream.outputMode("complete")
    .format("console")
    .start()
)

query.awaitTermination()

# 3. Manipulação Avançada de Dados
# 3.1. Pivotagem de Dados

# Suponha que temos um DataFrame com vendas
sales_data = [
    ("2023-01-01", "Produto A", 100),
    ("2023-01-01", "Produto B", 150),
    ("2023-01-02", "Produto A", 200),
    ("2023-01-02", "Produto B", 100),
]

sales_df = spark.createDataFrame(sales_data, ["data", "produto", "valor"])

# Pivotando dados
pivot_df = sales_df.groupBy("data").pivot("produto").sum("valor")
pivot_df.show()

# 4. Otimização de Consultas
# 4.1. Usando Caching e Persistência

# Persistindo DataFrames em memória
df.persist()
# Operações de transformação
transformed_df = df.groupBy("coluna").agg(F.sum("outra_coluna"))

# Quando não precisar mais, liberar memória
df.unpersist()

# 5. Trabalhando com GraphFrames


# Criando um DataFrame de vértices
vertices = spark.createDataFrame(
    [
        ("a", "Alice"),
        ("b", "Bob"),
        ("c", "Cathy"),
    ],
    ["id", "name"],
)

# Criando um DataFrame de arestas
edges = spark.createDataFrame(
    [
        ("a", "b", "friend"),
        ("b", "c", "follow"),
    ],
    ["src", "dst", "relationship"],
)

# Criando o GraphFrame
g = GraphFrame(vertices, edges)

# Encontrando os vizinhos
g.bfs("id = 'a'", "id = 'c'").show()

# 6. Uso de UDFs com Pandas UDF


@pandas_udf(DoubleType())
def calcula_media(s: pd.Series) -> pd.Series:
    return s.mean()


df = df.withColumn("media", calcula_media(df["idade"]))
df.show()

# 7. Trabalhando com API SQL do Spark

# Registrando o DataFrame como uma tabela temporária
df.createOrReplaceTempView("tabela_temporaria")

# Executando consultas SQL
resultado_sql = spark.sql(
    "SELECT nome, AVG(idade) as media_idade FROM tabela_temporaria GROUP BY nome"
)
resultado_sql.show()

# 8. Análise de Dados com Spark SQL

# Usando funções analíticas

window_spec = Window.partitionBy("cidade").orderBy("idade")
df.withColumn("rank", F.rank().over(window_spec)).show()

# 9. Gerenciamento de Dados
# 9.1. Schema Evolution
# Supondo que você tenha um DataFrame e deseja gravar como Delta
df.write.format("delta").mode("overwrite").save("caminho/para/delta")

# Lendo com suporte a evolução de schema
delta_df = spark.read.format("delta").load("caminho/para/delta")

# 10. Finalizando a Sessão
# Lembre-se de parar a sessão quando terminar:
spark.stop()

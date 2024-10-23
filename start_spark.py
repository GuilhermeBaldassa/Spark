# Importando a biblioteca e criando a sessão:
from pyspark.sql import SparkSession

# Criando uma sessão do Spark
spark = SparkSession.builder \
    .appName("MinhaAplicacao") \
    .getOrCreate()

# Exibindo a versão do Spark
print(spark.version)

# Lendo um arquivo CSV
df = spark.read.csv("./files/dados.csv", header=True, inferSchema=True)

# Exibindo as primeiras linhas do DataFrame
df.show()

# Finalizando a sessão: Quando terminar, é uma boa prática parar a sessão do Spark:
spark.stop()
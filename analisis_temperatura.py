from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("TemperatureClassifierStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir esquema del JSON entrante
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType())
])

# 3. Leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# 4. Convertir el JSON a columnas
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. Clasificar la temperatura
classified_df = parsed_df.withColumn(
    "category",
    when(col("temperature") < 18, "Frio")
    .when((col("temperature") >= 18) & (col("temperature") <= 25), "Agradable")
    .otherwise("Caliente")
)

# 6. Contar cuántas lecturas hay por categoría
category_counts = classified_df.groupBy("category").count()

# 7. Mostrar resultados en la consola
query = category_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, avg

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("ETL_Ventas") \
    .master("local[*]") \
    .getOrCreate()

# 2. Extraer datos (leer CSV)
df = spark.read.csv("ventas.csv", header=True, inferSchema=True)
print("Datos originales:")
df.show()

# 3. Transformar datos
df2 = df.withColumn("monto", col("monto").cast("double")) \
        .withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))

ventas_por_cat = df2.groupBy("categoria").agg(_sum("monto").alias("ventas_totales"))
ticket_prom = df2.groupBy("cliente").agg(avg("monto").alias("ticket_promedio"))

print("Ventas por categoría:")
ventas_por_cat.show()

print("Ticket promedio por cliente:")
ticket_prom.show()

# 4. Cargar resultados (guardar en CSV)
ventas_por_cat.coalesce(1).write.mode("overwrite").option("header", True).csv("out/ventas_por_categoria")
ticket_prom.coalesce(1).write.mode("overwrite").option("header", True).csv("out/ticket_promedio")

# 5. Finalizar
spark.stop()

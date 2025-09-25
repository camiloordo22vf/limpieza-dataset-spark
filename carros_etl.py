# ==========================================
# EXAMEN PARCIAL - PYSPARK
# Dataset: Concesionario (datos sucios)
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, avg, count, desc, when, round, abs

# 1. Crear sesión Spark
spark = SparkSession.builder \
    .appName("ExamenConcesionario") \
    .getOrCreate()

# 2. Cargar dataset sucio
df = spark.read.csv("concesionario_sucio.csv", header=True, inferSchema=True)

print("==== Dataset original (5 registros) ====")
df.show(5, truncate=False)

# 3. Limpieza del dataset
df_clean = df.dropDuplicates()

# Normalizar texto: Cliente, Vehiculo, Marca, Ciudad, Concesionario
df_clean = df_clean.withColumn("Cliente", trim(upper(col("Cliente")))) \
                   .withColumn("Vehiculo", trim(upper(col("Vehiculo")))) \
                   .withColumn("Marca", trim(upper(col("Marca")))) \
                   .withColumn("Concesionario", trim(upper(col("Concesionario"))))

# Quitar precios negativos o nulos
df_clean = df_clean.filter((col("precio").isNotNull()) & (col("precio") > 0))

df_clean = df_clean.withColumn(
    "Marca",
    when(col("Marca").rlike("TOYOT|TOYOA|TOYTA|TOYOYA|TOYTO|TOTYO|TOYO"), "TOYOTA")
    .otherwise(col("Marca"))
)
df_clean = df_clean.withColumn(
    "Marca",
    when(col("Marca").rlike("honda|honDA|Hnda|HNDA"), "HONDA")
    .otherwise(col("Marca"))
)
print("==== Dataset limpio (5 registros) ====")
df_clean.show(5, truncate=False)

# ==============================
# 4. CONSULTAS
# ==============================

# a) Top 10 clientes que más han gastado
top_clientes = df_clean.groupBy("Cliente").sum("precio") \
                       .withColumnRenamed("sum(precio)", "total_gastado") \
                       .orderBy(desc("total_gastado")) \
                       .limit(10)
print("==== Top 10 clientes con mayor gasto ====")
top_clientes.show()

# b) Top 10 clientes que menos han gastado
bottom_clientes = df_clean.groupBy("Cliente").sum("precio") \
                          .withColumnRenamed("sum(precio)", "total_gastado") \
                          .orderBy("total_gastado") \
                          .limit(10)
print("==== Top 10 clientes con menor gasto ====")
bottom_clientes.show()

# c) Promedio de precios por marca
promedio_marcas = df_clean.groupBy("Marca").agg(avg("precio").alias("promedio_precio")) \
                          .orderBy(desc("promedio_precio"))
print("==== Promedio de precios por marca ====")
promedio_marcas.show()

# d) Concesionario con mayor volumen de ventas
ventas_concesionario = df_clean.groupBy("Concesionario").agg(count("*").alias("num_ventas")) \
                               .orderBy(desc("num_ventas"))
print("==== Concesionario con mayor número de ventas ====")
ventas_concesionario.show(1)



# f) Marca más vendida
marca_mas_vendida = df_clean.groupBy("Marca").agg(count("*").alias("num_ventas")) \
                            .orderBy(desc("num_ventas"))
print("==== Marca más vendida ====")
marca_mas_vendida.show(1)

df_clean.write.mode("overwrite").option("header", True).csv("concesionario_limpio.csv")
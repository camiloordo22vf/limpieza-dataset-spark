from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, concat, upper, lower, substring, to_date, sum as _sum, avg

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("ETL_Ventas_Limpieza") \
    .master("local[*]") \
    .getOrCreate()

# 2. Cargar datos
df = spark.read.csv("ventas_limpieza.csv", header=True, inferSchema=True)

print("Datos originales:")
df.show(truncate=False)

# 3. Limpieza de datos

# Eliminar espacios en cliente y categoría
df_clean = df.withColumn("cliente", trim(col("cliente"))) \
             .withColumn("categoria", trim(col("categoria")))

# Normalizar nombres de clientes (Primera letra mayúscula, resto minúscula)
df_clean = df_clean.withColumn(
    "cliente",
    when(
        col("cliente").isNotNull(),
        concat(
            upper(substring(col("cliente"), 1, 1)),
            lower(substring(col("cliente"), 2, 100))
        )
    ).otherwise(None)
)

# Convertir monto a double de forma segura: si no es numérico, poner NULL
df_clean = df_clean.withColumn(
    "monto",
    when(col("monto").rlike("^[0-9]+(\\.[0-9]+)?$"), col("monto").cast("double")).otherwise(None)
)

# Convertir fecha a date
df_clean = df_clean.withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))

# Eliminar duplicados
df_clean = df_clean.dropDuplicates()

# Eliminar filas con valores nulos en campos críticos
df_clean = df_clean.dropna(subset=["cliente", "categoria", "monto", "fecha"])

print("Datos limpios:")
df_clean.show(truncate=False)

# 4. Transformaciones adicionales

# Ventas totales por categoría
ventas_por_categoria = df_clean.groupBy("categoria").agg(_sum("monto").alias("ventas_totales"))

# Ticket promedio por cliente
ticket_promedio = df_clean.groupBy("cliente").agg(avg("monto").alias("ticket_promedio"))

print("Ventas por categoría:")
ventas_por_categoria.show(truncate=False)

print("Ticket promedio por cliente:")
ticket_promedio.show(truncate=False)

# 5. Guardar resultados
ventas_por_categoria.coalesce(1).write.mode("overwrite").option("header", True).csv("out/ventas_por_categoria")
ticket_promedio.coalesce(1).write.mode("overwrite").option("header", True).csv("out/ticket_promedio")

# 6. Finalizar sesión Spark
spark.stop()

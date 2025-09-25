from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, upper, avg, row_number, when, regexp_replace
from pyspark.sql.window import Window

# 1. Crear sesión de Spark
spark = SparkSession.builder.appName("LimpiezaCalificaciones").getOrCreate()

# 2. Leer CSV
df = spark.read.csv("calificaciones_sucias.csv", header=True, inferSchema=True)

print("Datos originales:")
df.show(5, truncate=False)

# 3. Limpieza
df_clean = (
    df.withColumn("nombre", trim(col("nombre")))
      .withColumn("materia", trim(col("materia")))
      .withColumn("universidad", trim(col("universidad")))
      .withColumn("nombre", lower(col("nombre")))  # normalizar nombres
      .withColumn("materia", lower(col("materia")))  # normalizar materias
      .withColumn("universidad", lower(col("universidad")))  # normalizar universidades
      .withColumn("calificacion", when(col("calificacion") < 0, None).otherwise(col("calificacion")))  # eliminar negativos
      .dropna(subset=["calificacion"])  # eliminar nulos en calificación
      .withColumn("universidad", upper(col("universidad")))
      .dropDuplicates()
      .dropna(subset=["calificacion"])  # eliminar nulos en calificación
)
df_clean = (
    df_clean.withColumn(
        "universidad",
        regexp_replace(col("universidad"), "U\\. NARIÑIO", "UNIVERSIDAD DE NARIÑO")
    )
    .withColumn(
        "universidad",
        regexp_replace(col("universidad"), "UNIVERSIDAD DE NARINO", "UNIVERSIDAD DE NARIÑO")
    )
    .withColumn(
        "universidad",
        regexp_replace(col("universidad"), "U\\. CAUCA", "UNIVERSIDAD DEL CAUCA")
    )
    .withColumn(
        "universidad",
        regexp_replace(col("universidad"), "UNIVERESIDAD DEL CAUCA", "UNIVERSIDAD DEL CAUCA")
    )
    .withColumn(
        "universidad",
        regexp_replace(col("universidad"), "UNIVERSIDAD NAL DE COLOMBIA", "UNIVERSIDAD NACIONAL DE COLOMBIA")
    )
    .withColumn(
        "universidad",
        regexp_replace(col("universidad"), "UCC", "UNIVERSIDAD COOPERATIVA DE COLOMBIA")
    )
)

print("Datos limpios:")
df_clean.show(5, truncate=False)

# 4. Top y bottom estudiantes
window = Window.orderBy(col("calificacion").desc())
top10 = df_clean.withColumn("rank", row_number().over(window)).filter(col("rank") <= 10)

window2 = Window.orderBy(col("calificacion").asc())
bottom10 = df_clean.withColumn("rank", row_number().over(window2)).filter(col("rank") <= 10)

print("Top 10 estudiantes:")
top10.show()

print("Peores 10 estudiantes:")
bottom10.show()

# 5. Mejor y peor universidad
uni_avg = df_clean.groupBy("universidad").agg(avg("calificacion").alias("promedio"))
mejor_uni = uni_avg.orderBy(col("promedio").desc()).limit(1)
peor_uni = uni_avg.orderBy(col("promedio").asc()).limit(1)

print("Mejor universidad:")
mejor_uni.show()

print("Peor universidad:")
peor_uni.show()

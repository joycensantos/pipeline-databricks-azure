// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------



// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------



// COMMAND ----------

display(
  df.select("anuncio.*","anuncio.endereco.*")
  )

// COMMAND ----------



// COMMAND ----------

val dados_detalhados = df.select("anuncio.*","anuncio.endereco.*")

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------



// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas","endereco")
display (df_silver)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/silver")

// COMMAND ----------



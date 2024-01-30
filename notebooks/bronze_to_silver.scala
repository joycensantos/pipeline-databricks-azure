// Databricks notebook source
// MAGIC %md
// MAGIC ## Validando acesso a pasta

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Lendo o arquivo delta da camada bronze

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Verificando as colunas que podemos ter no campo anuncio

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Verificando as colunas que podemos ter no campo endereco

// COMMAND ----------

display(
  df.select("anuncio.*","anuncio.endereco.*")
  )

// COMMAND ----------

// MAGIC %md
// MAGIC ## Salvando as alterações de coluna em uma variável

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*","anuncio.endereco.*")

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Removendo os campos que não serão mais utilizados

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas","endereco")
display (df_silver)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Salvando as alterações na camada silver em arquivo tipo delta

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Validando o salvamento

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/silver")

// COMMAND ----------



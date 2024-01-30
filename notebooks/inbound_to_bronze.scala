// Databricks notebook source
// MAGIC %md
// MAGIC ## Verificando acesso a pasta

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/inbound")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Lendo da Pasta

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Dropando as colunas conforme solicitado

// COMMAND ----------

val dados_anuncio = dados.drop("imagens", "usuario")
display(dados_anuncio)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Criando nova coluna com o conteúdo do campo id

// COMMAND ----------

// MAGIC %md
// MAGIC ##### import da functions para utilizar withColumn

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id",col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Salvando as alterações na camada bronze em arquivo tipo delta

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Validando se foi salvo com êxito

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------



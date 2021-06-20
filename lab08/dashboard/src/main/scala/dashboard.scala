import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Неужели это последняя лаба
 */

object dashboard extends App {
  val spark = SparkSession.builder
    .appName("Laba 08 - Dashboard")
    .getOrCreate

  // 1.0 Читаем модель из hdfs
  val model = PipelineModel.load("/user/denis.sidorenko/model")

  // 1.1 Читаем логи из hdfs (laba08)
  val webLogs = spark.read.json("/labs/laba08/laba08.json")

  val hdfsDf = webLogs.select("uid", "visits", "date")
    .filter(col("uid").isNotNull)
    .withColumn("visits", explode(col("visits")))

  val hdfsDf2 = hdfsDf.select("uid", "visits.*", "date")
    .withColumn("url",
      regexp_extract(col("url"), "^(?:https?:\\/\\/)?(?:www\\.)?([^:\\/\\n?]+)", 1))
    .drop("timestamp")

  // 1.2 Группируем url по uid, схлопывая повторяющиеся row с gender_age
  val weblogs2 = hdfsDf2.groupBy("uid", "date").agg(
    collect_list("url").as("domains"))

  val agg_log_predict = model.transform(weblogs2)

  val resultForKafka = agg_log_predict
    .withColumnRenamed("originalLabel", "gender_age")
    .select(col("uid").cast("string").alias("key"),
      to_json(struct("uid", "gender_age", "date")).alias("value"))

  // Опции подключения ElasticSearch
  val esOptions =
    Map(
      "es.nodes" -> "10.0.0.5",
      "es.port" -> "9200",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true",
      "es.net.http.auth.user" -> "denis.sidorenko",
      "es.net.http.auth.pass" -> "Zt3XPmOF"
    )

  resultForKafka
    .write
    .mode("overwrite")
    .format("org.elasticsearch.spark.sql")
    .options(esOptions)
    .save(s"denis_sidorenko_lab08/_doc")
}

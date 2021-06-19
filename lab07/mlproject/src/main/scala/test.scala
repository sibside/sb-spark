import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

// Инференс
object test extends App {
  val spark = SparkSession.builder.appName("Laba 07 Test").getOrCreate

  import spark.implicits._

  val kafkaParamsRead = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "denis_sidorenko"
  )

  // 1.0 Читаем модель из hdfs
  val model = PipelineModel.load("/user/denis.sidorenko/model")

  // 1.1 Читаем DataSet из Kafka Topik
  val dataKafka = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", "denis_sidorenko")
    .option("startingOffsets", """latest""")
    .load()

  // 1.2 Приводим их в подобающий вид:)
  val jsonString = dataKafka.select('value.cast("string")).as[String]
  val test = spark.read.json(jsonString)

  // 1.3 Регулярка на url, удаляем не нужную колонку timestamp
  val hdfsDf2 = test.select("uid", "visits.*")
    .withColumn("url", regexp_extract(
      col("url"), "^(?:https?:\\/\\/)?(?:www\\.)?([^:\\/\\n?]+)", 1))

  // 1.4 Группируем url по uid, схлопывая повторяющиеся row с gender_age
  val weblogs2 = hdfsDf2.groupBy("uid").agg(
    collect_list("url").as("domains"))

  val resultForKafka = model.transform(weblogs2)

  val kafkaOutputParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "topic" -> "denis_sidorenko_lab07_out"
  )

  resultForKafka
    .select(col("uid").cast("string").alias("key"),
      to_json(struct("uid", "gender_age")).alias("value"))
    .writeStream
    .format("kafka")
    .options(kafkaOutputParams)
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .outputMode("update")
    .start()
    .awaitTermination()
  spark.stop()
}

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

// Инференс
object test extends App {
  val spark = SparkSession.builder.appName("Laba 07 - Test").getOrCreate

  val kafkaReadParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "denis_sidorenko",
    "startingOffsets" -> "earliest"
  )

  val kafkaOutputParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "topic" -> "denis_sidorenko_lab07_out",
    "truncate" -> "false",
    "checkpointLocation" -> "/user/denis.sidorenko/tmp"
  )

  // 1.0 Читаем модель из hdfs
  val model = PipelineModel.load("/user/denis.sidorenko/model")

  // 1.1 Схема для Кафки
  val schema = StructType(
    List(
      StructField("uid", StringType),
      StructField("visits",
        ArrayType(
          StructType(
            List(
              StructField("url", StringType),
              StructField("timestamp", LongType)
            )
          )
        )
      )
    )
  )

  // 1.2 Читаем из Кафки
  val logDf = spark
    .readStream
    .format("kafka")
    .options(kafkaReadParams)
    .load

  // 1.3 Причёсываем
  val logDf2 = logDf
    .select(col("value").cast("String").as("data"))
    .withColumn("jsonCol", from_json(col("data"), schema))
    .select(col("jsonCol.*"))
    .select(col("uid"), explode(col("visits")))
    .select(col("uid"), col("col.*"))
    .withColumn("domain", lower(regexp_extract(col("url"), "^(?:https?:\\/\\/)?(?:www\\.)?([^:\\/\\n?]+)", 1)))

  // 1.4. Группируем по UID
  val agg_log = logDf2
    .groupBy("uid")
    .agg(collect_list("domain").alias("domains"))
  // String: uid, Array: domains -> String: element

  // 1.5. Трансформация
  val agg_log_predict = model.transform(agg_log)
  agg_log_predict.printSchema()

  val resultForKafka = agg_log_predict
    .withColumnRenamed("originalLabel", "gender_age")
    .select(col("uid").cast("string").alias("key"),
      to_json(struct("uid", "gender_age")).alias("value"))

  resultForKafka
    .writeStream
    .format("kafka")
    .options(kafkaOutputParams)
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .outputMode("update")
    .start()
    .awaitTermination()
}

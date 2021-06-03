import org.apache.spark.sql.SparkSession

/**
 * @author Sidorenko Denis
 */

object agg extends App {

  /*
   PART 1 - Prepare
   */
  val spark = SparkSession.builder()
    .appName("Lab_04b_Agg")
    .getOrCreate()

  val topic_name = "denis_sidorenko"
  var offset = "earliest"

  // Параметры подключения и чтения из Кафки
  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> topic_name,
    "startingOffsets" -> offset
  )

  /*
   PART 2 - Read data from Kafka
  */
  // Читаем данные из топика name_surname
  val dfIn = spark.readStream
    .format("kafka")
    .options(kafkaParams)
    .load
    .withWatermark("timestamp", "1 hours")

  // Пишем данные в топик name_surname_lab04b_out
  //  dfIn.selectExpr("CAST(uid AS STRING) AS key", "to_json(struct(*)) AS value")
  dfIn.writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("topic", "denis_sidorenko_lab04b_out")
    .start()
    .awaitTermination()
}

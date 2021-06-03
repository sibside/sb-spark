import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

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

  var offset = spark.conf.get("spark.filter.offset") // earliest
  if (offset != "earliest") {
    offset = s"""{"$topic_name":{"0":$offset}}"""
  }

  // Параметры подключения и чтения из Кафки
  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> topic_name,
    "startingOffsets" -> offset,
    "endingOffsets" -> "latest"
  )

  /*
   PART 2 - Read data from Kafka
  */
  // Читаем данные из топика name_surname
  val dfIn = spark
    .readStream
    .format("kafka")
    .options(kafkaParams)
    .load
    .withWatermark("timestamp", "1 hours")
    .dropDuplicates(Seq("uid", "timestamp"))

  // Создаем синк для записи данных в Kafka
  val kafkaSink = createKafkaSink("DSout4", dfIn)

  // Пишем данные в топик name_surname_lab04b_out
  val sq_kafka = kafkaSink.start

  // Создаем синк для записи в Kafka
  def createKafkaSink(chkName: String, df: DataFrame) = {
    df
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"/tmp/chk/$chkName")
      .option("truncate", "false")
      .option("numRows", "20")
  }
}

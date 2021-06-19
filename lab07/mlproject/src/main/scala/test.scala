import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, SparkSession}

// Инференс
object test extends App {
  val spark = SparkSession.builder.appName("Laba 07").getOrCreate

  import spark.implicits._

  val kafkaParamsRead = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "denis_sidorenko"
  )

  // 1.0 Читаем модель из hdfs
  val model = PipelineModel.load("/user/denis.sidorenko/model")

  // 1.1 Читаем DataSet из Kafka Topik
  val kafkaDf = spark.read.format("kafka").options(kafkaParamsRead).load
  val jsonString = kafkaDf.select('value.cast("string")).as[String]
  val test = spark.read.json(jsonString)

  val df = model.transform(test)

  // 1.2 Создаем метод для записи предсказаний в другой Kafka Topik
  def writeKafka[T](topic: String, data: Dataset[T]): Unit = {
    val kafkaParamsWrite = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
    )

    data.toJSON.withColumn("topic", lit(topic)).write.format("kafka")
      .options(kafkaParamsWrite)
      .save()
  }

  // 1.3 Пишем в результирущий топик
  writeKafka("denis_sidorenko_lab07_out", df)
}

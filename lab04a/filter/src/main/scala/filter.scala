import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, to_timestamp}

/**
 * @author Sidorenko Denis
 */
object filter extends App {

  import spark.implicits._

  val output_dir_prefix = "/user/denis.sidorenko/visits"

  val spark = SparkSession.builder()
    .appName("filter")
    .getOrCreate()

  // Параметры подключения и чтения из Кафки
  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "lab04_input_data",
    "offset" -> "earliest",
    "output_dir_prefix" -> output_dir_prefix
  )

  // Читаем данные из Кафки
  val df = spark.read.format("kafka").options(kafkaParams).load()

  val jsonString = df.select('value.cast("string")).as[String]

  val parsed = spark.read.json(jsonString)

  // View DF
  val viewDF = parsed
    .select("*")
    .where($"event_type" === "view")
    .withColumn("date",
      date_format(to_timestamp((col("timestamp") / 1000).cast("long")), "yyyyMMdd"))
    .withColumn("p_date", col("date"))

  // Buy DF
  val buyDF = parsed
    .select("*")
    .where($"event_type" === "buy")
    .withColumn("date",
      date_format(to_timestamp((col("timestamp") / 1000).cast("long")), "yyyyMMdd"))
    .withColumn("p_date", col("date"))

  // Пишем на hdfs
  viewDF.write
    .mode("overwrite")
    .partitionBy("p_date")
    .json(s"$output_dir_prefix/view/")

  buyDF.write
    .mode("overwrite")
    .partitionBy("p_date")
    .json(s"$output_dir_prefix/buy/")
}

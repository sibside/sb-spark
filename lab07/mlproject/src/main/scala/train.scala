import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Обучение
object train extends App {
  // 1.0 Создаем сессию Спарка
  val spark = SparkSession.builder.appName("Laba 07").getOrCreate

  // 1.1 Читаем логи из hdfs
  val webLogs = spark.read.json("/labs/laba07/laba07.json")

  // 1.2 Фильтруем на пустые Uid, взрываем колонку visits
  val hdfsDf = webLogs.select("uid", "visits", "gender_age")
    .filter(col("uid").isNotNull)
    .withColumn("visits", explode(col("visits")))

  // 1.3 Регулярка на url, удаляем не нужную колонку timestamp
  val hdfsDf2 = hdfsDf.select("uid", "visits.*", "gender_age")
    .withColumn("url", regexp_extract(
      col("url"), "^(?:https?:\\/\\/)?(?:www\\.)?([^:\\/\\n?]+)", 1))
    .drop("timestamp")

  // 1.4 Группируем url по uid, схлопывая повторяющиеся row с gender_age
  val weblogs2 = hdfsDf2.groupBy("uid").agg(
    collect_list("url").as("domains"), collect_set("gender_age")(0).as("gender_age"))

  // 1.5 Индексатор меток, который отображает строковый столбец меток в столбец ML индексов меток.
  val indexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("label")
    .fit(weblogs2)

  // 1.6 Создаем Преобразователь, который отображает столбец индексов обратно в новый столбец соответствующих строковых значений.
  val stringer = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("category")
    .setLabels(indexer.labels)

  // 1.7 Создаем Логическую регрессию
  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  // 1.8 Извлекаем словарь из коллекций документов и генерируем CountVectorizerMode
  val cv = new CountVectorizer()
    .setInputCol("domains")
    .setOutputCol("features")

  // 1.9 Создаем трубу для обучения (этап эстиматора)
  val pipeline = new Pipeline()
    .setStages(Array(cv, indexer, lr, stringer))

  // 2.0 Обучаем модель
  val model = pipeline.fit(weblogs2)

  // 2.1 Сохраняем модель
  model.write.overwrite().save("/user/denis.sidorenko/model")

  spark.stop()
}

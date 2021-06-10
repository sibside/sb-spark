import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

import java.net.URL
import scala.util.Try

object features {
  def main(args: Array[String]) = {
    val conf = new SparkConf(true)
    val session = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import org.apache.spark.sql.functions._

    val weblogs = session.read.json("/labs/laba03/weblogs.json")

    val getHost = udf { (value: String) =>
      Try {
        new URL(value).getHost
      }.toOption
    }
    val getVisits = udf { (hour: String, week_day: String) =>
      Try {
        if (Seq("9", "10", "11", "12", "13", "14", "15", "16", "17").exists(x => x == hour)) "word_hours"
        else if (Seq("18", "19", "20", "21", "22", "23").exists(x => x == hour)) "evening_hours"
        else "other"
      }.toOption
    }

    import session.implicits._

    val byUid = Window.partitionBy('uid)

    val usersItems = session.read.parquet("users-items/20200429")

    session.conf.set("spark.sql.session.timeZone", "UTC")

    val xSet = weblogs
      .withColumn("x", explode(col("visits")))
      .withColumn("host",
        regexp_replace(regexp_replace(lower(getHost(col("x.url"))), "www.", ""), "^[.]", ""))
      .withColumn("timestamp", to_timestamp(col("x.timestamp") / 1000))
      .withColumn("week_day", concat(lit("web_day_"), lower(date_format(to_timestamp(col("x.timestamp") / 1000), "E"))))
      .withColumn("hour", concat(lit("web_hour_"), date_format(to_timestamp(col("x.timestamp") / 1000), "H")))
      .withColumn("vit_type", getVisits(regexp_replace('hour, "web_hour_", ""), 'week_day))
      .withColumn("cnt_by_uid", count('*) over byUid)
      .drop("x").drop("visits")

    val xTop = xSet
      .groupBy('host)
      .count()
      .filter(col("host").isNotNull)
      .filter(col("host") =!= "62.ua" && col("host") =!= "just.ru")
      .sort(desc("count"))
      .limit(1000)

    val xSet1 = xSet.join(xTop, Seq("host"), "left")
    val xSet2 = xSet.select('uid).dropDuplicates().crossJoin(xTop).select('uid, 'host, 'count)

    val xVisits = xSet.select('uid, 'host, lit(1) as "visit")

    val xPrevFeatures = xSet2.join(xVisits, Seq("uid", "host"), "left")
    val xFeatures = xPrevFeatures.groupBy("uid", "host")
      .agg(sum('visit) as "cnt")
      .select(col("uid"),
        regexp_replace(col("host"), "interfax", "interpals") as "host", col("cnt"))
      .sort(asc("uid"), asc("host")).na.fill(0, Seq("cnt"))

    val domainFeatures = xFeatures.groupBy("uid").agg(collect_list("cnt") as "domain_features")

    val xSetWeek = xSet1.groupBy("uid")
      .pivot("week_day")
      .count()
      .na.fill(0)
      .select('uid, 'web_day_mon, 'web_day_tue,
        'web_day_wed, 'web_day_thu,
        'web_day_fri, 'web_day_sat, 'web_day_sun)

    val xSetHour = xSet1.groupBy("uid")
      .pivot("hour")
      .count
      .na.fill(0)

    val xSetVisits = xSet1.groupBy("uid", "cnt_by_uid")
      .pivot("vit_type")
      .count()
      .na.fill(0)
      .withColumn("web_fraction_work_hours", 'evening_hours / ('evening_hours + 'word_hours + 'other))
      .drop("evening_hours", "other", "word_hours")

    val results = usersItems
      .join(xSetWeek, Seq("uid"), "left")
      .join(xSetHour, Seq("uid"), "left")
      .join(xSetVisits, Seq("uid"), "left")
      .join(domainFeatures, Seq("uid"), "left")

    results.write.mode("overwrite").parquet(s"features")
  }
}
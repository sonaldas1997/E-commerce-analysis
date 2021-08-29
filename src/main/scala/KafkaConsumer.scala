import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{current_timestamp, dayofmonth, month, year}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaConsumer {

  def main(args : Array[String]) : Unit = {
    val topic = Seq("e-commerce")
    Logger.getLogger("org").setLevel(Level.ERROR)


    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "1",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val sparkconf = new SparkConf().setMaster("local").setAppName("kafkaConsumer")
    val ssc = new StreamingContext(sparkconf, Seconds(5))

    val message = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, kafkaParams)
    )

    val lines = message.map(_.value()).flatMap(_.split("\n"))
    //println(lines)
    //lines.foreachRDD(rdd => rdd.collect().foreach(println))

    val schema = new StructType()
      .add(StructField("id", StringType , nullable = false))
      .add(StructField("Date", StringType,nullable = false))
      .add(StructField("order_id", StringType,nullable = false))
      .add(StructField("order_status", StringType,nullable = false))

    lines.foreachRDD{ rdd =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      val data = rdd.map(_.split(",").to[List])
        .map(line => Row(line(0), line(1), line(2), line(3)))

      val rawDF = spark.createDataFrame(data, schema)

      val fin = rawDF
        .withColumn("etl_ts", current_timestamp)
        .withColumn("year", year(current_timestamp))
        .withColumn("month", month(current_timestamp))
        .withColumn("day", dayofmonth(current_timestamp))

      //fin.show()

      fin.write
        .format("parquet")
        .mode("append")
        .option("compression","snappy")
        .save("src/main/Data/out")

    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(10000)
  }
}

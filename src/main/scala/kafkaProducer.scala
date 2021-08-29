import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source

object kafkaProducer {

  def main(args : Array[String]) : Unit = {
    val topic = "e-commerce"
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](properties)
    val filename= "D:\\pycharm\\kafkaStreaming\\src\\main\\Data\\part-00000.txt"
    for (line <- Source.fromFile(filename).getLines) {

    val record = new ProducerRecord[String, String](topic, "key", line)
    Thread.sleep(1000)
    producer.send(record)}
    producer.close()
  }

}

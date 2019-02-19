package no.sysco

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import better.files._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


//#FileProcessor-companion
//#FileProcessor-messages
object FileProcessor {
  //#greeter-messages
  def props(kafkaWriter: ActorRef): Props = Props(new FileProcessor(kafkaWriter))
  //#greeter-messages
  final case class FileToProcess(fileRef: File)
}
//#FileProcessor-messages
//#FileProcessor-companion

//#FileProcessor-actor
class FileProcessor(kafkaWriter: ActorRef) extends Actor {
  import FileProcessor._
  import KafkaWriter._

  val bootstrapServers = "127.0.0.1:9092"

  val properties = new Properties
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  def receive = {
    case FileToProcess(fileRef) =>{
      val fileIterator = fileRef.lineIterator
      val csvToJson = CsvToJson(fileIterator.next())
      val producer = new KafkaProducer[String, String](properties)

      for {
        //line <- fileRef.lineIterator.drop(1)
        line <- fileIterator
      } kafkaWriter ! WriteLine(line, csvToJson, producer)

      producer.flush()
    }
  }

}
//#FileProcessor-actor

//#KafkaWriter-companion
//#KafkaWriter-messages
object KafkaWriter {
  //#KafkaWriter-messages
  def props: Props = Props[KafkaWriter]
  //#KafkaWriter-messages
  final case class WriteLine(line: String, csvToJson: CsvToJson, producer: KafkaProducer[String, String])
}
//#KafkaWriter-messages
//#KafkaWriter-companion

//#KafkaWriter-actor
class KafkaWriter extends Actor with ActorLogging {
  import KafkaWriter._

  val topicName = "first_topic"

  def receive = {
    case WriteLine(line, csvToJson, producer) => {
      //log.info("CSV line received (from " + sender() + "): " + line)
      val json = csvToJson.convertToJson(line)
      val jsonString = json.get.prettyPrint
      log.info(jsonString)

      val record = new ProducerRecord[String, String](topicName, jsonString)
      producer.send(record)
    }

  }
}
//#printer-actor

//#main-class
object FileListLoader extends App {
  import FileProcessor._

  // Create the 'helloAkka' actor system
  val system: ActorSystem = ActorSystem("akkaToKafka")

  println("*** Instantiating Actors and processing files ***")

  //val inputDir = File("/Users/cube/Documents/scala/AkkaToKafka/src/main/resources/in")
  val inputDir = File("resources")

  readDir(inputDir)



  def readDir(inputDir: File) = {
    for {
      inputFile <- inputDir.list(_.extension == Some(".csv")).toSeq.sorted(File.Order.bySize)
    } {
      val fileProcessor: ActorRef = system.actorOf(FileProcessor.props(system.actorOf(KafkaWriter.props, inputFile.name + "_ToKafka")), inputFile.name)
      fileProcessor ! FileToProcess(inputFile)
    }
  }

}
//#main-class
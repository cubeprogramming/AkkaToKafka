package no.sysco

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import better.files._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection._
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}
import scala.util._

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
class FileProcessor(kafkaWriter: ActorRef) extends Actor with ActorLogging {
  import FileProcessor._
  import KafkaWriter._

  implicit val timeout = Timeout(10 seconds)

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

      val futures = for {
        //line <- fileRef.lineIterator.drop(1)
        line <- fileIterator
      } yield kafkaWriter ? WriteLine(line, csvToJson, producer)

      for {
        future <- futures
      } log.info(Await.result(future, timeout.duration).asInstanceOf[String])

      producer.flush()

 //     context stop kafkaWriter
      sender ! s"Processed file: ${fileRef.name}"

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

  val topicName = "csv_lines"

  def receive = {
    case WriteLine(line, csvToJson, producer) => {
      //log.info("CSV line received (from " + sender() + "): " + line)
      val json = csvToJson.convertToJson(line)
      val jsonString = json.get.prettyPrint
      //log.info(jsonString)

      val record = new ProducerRecord[String, String](topicName, jsonString)
      producer.send(record)
      sender ! jsonString
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

  implicit val timeout = Timeout(10 seconds)
  implicit val ec = system.dispatchers.defaultGlobalDispatcher
  val actorNames = mutable.Set.empty[String]

  //val inputDir = File("/Users/cube/OneDrive - Sysco AS/source/scala/AkkaKafkaAkka/AkkaToKafka/resources")
  val inputDir = File("resources")

  (1 to 10).foreach (i => {
    readDir(inputDir,i)

    Thread.sleep(2000)
  })

  system.terminate()
  Await.ready(system.whenTerminated, Duration(30, TimeUnit.SECONDS))


  def readDir(inputDir: File, cycle: Int) = {
    for {
      inputFile <- inputDir.list(_.extension == Some(".csv")).toSeq.sorted(File.Order.bySize)
    } {
      println(inputFile.name)
      val actorName = actorNames.find(actName => actName == inputFile.name)

      system.actorSelection("user/" + actorName.getOrElse("NotFound")).resolveOne().onComplete {
        case Success(actorRef) => actorRef ? FileToProcess(inputFile)
        case Failure(ex) => {
            val fileProcessor = system.actorOf(FileProcessor.props(system.actorOf(KafkaWriter.props, s"${inputFile.name}_ToKafka$cycle")), inputFile.name)
          actorNames += inputFile.name
          fileProcessor ? FileToProcess(inputFile)
        }
      }
      //val fileProcessor: ActorRef = system.actorOf(FileProcessor.props(system.actorOf(KafkaWriter.props, inputFile.name + "_ToKafka")), inputFile.name)
    }
  }

}
//#main-class
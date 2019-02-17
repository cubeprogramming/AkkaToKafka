package no.sysco

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import better.files._


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

  def receive = {
    case FileToProcess(fileRef) =>{
      val fileIterator = fileRef.lineIterator
      val csvToJson = CsvToJson(fileIterator.next())

      for {
        //line <- fileRef.lineIterator.drop(1)
        line <- fileIterator
      } kafkaWriter ! WriteLine(line, csvToJson)
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
  final case class WriteLine(line: String, csvToJson: CsvToJson)
}
//#KafkaWriter-messages
//#KafkaWriter-companion

//#KafkaWriter-actor
class KafkaWriter extends Actor with ActorLogging {
  import KafkaWriter._

  def receive = {
    case WriteLine(line, csvToJson) => {
      //log.info("CSV line received (from " + sender() + "): " + line)
      val json = csvToJson.convertToJson(line)
      println(json.get.prettyPrint)
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
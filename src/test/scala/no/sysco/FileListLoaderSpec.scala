package no.sysco

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import better.files._
import FileProcessor._
import KafkaWriter._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class FileListLoaderSpec (_system: ActorSystem)
  extends TestKit(_system)
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll {
  //#test-classes

  def this() = this(ActorSystem("FileListLoaderSpec"))

  override def afterAll: Unit = {
    shutdown(system)
  }

  //#first-test
  //#specification-example
  "A FileProcessor Actor" should {
    "load file and pass on file line when instructed to" in {
      val testProbe = TestProbe()
      val testFile = File("resources/AccountingPoint2.csv")
      val fileIterator = testFile.lineIterator
      val csvToJson = CsvToJson(fileIterator.next())
      val line = fileIterator.next()
      val expectedObject = WriteLine(line, csvToJson)

      val fileProcessor: ActorRef = system.actorOf(FileProcessor.props(testProbe.ref))
      fileProcessor ! FileToProcess(testFile)
      //testProbe.expectMsg(5000 millis, expectedObject)
      testProbe.expectMsgClass(5000 millis, expectedObject.getClass)
    }
  }
  //#first-test
}

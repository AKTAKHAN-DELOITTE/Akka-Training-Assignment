import ProcessingCheckpoints.Record
import RecordProcessor.validation
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import akka.routing.RoundRobinPool
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory

import scala.util.{Failure, Success, Try}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object ProcessingCheckpoints extends App {
  import scala.io.Source

  case class Record(OrderDate: LocalDate, Shipdate: LocalDate, ShipMode: LocalDate, Customer: String, Segment: String,
                    Country: String, City: String, State: String, Region: String, Category: String,
                    SubCategory: String, Name: String, Sales: Double, Quanity: Int, Discount: Double,
                    Profit: Double)

  val applicationConf = ConfigFactory.load("application.conf")
  val filepath = applicationConf.getString("app.filepath")

  val system = ActorSystem("FileReaderSystem")

  val fileReaderActor = system.actorOf(Props[ReaderActor], "fileReaderActor")

  val source = Source.fromFile(filepath)
  val lines = source.getLines()

  for (line <- lines) {
    val record = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)(?<![ ])(?![ ])")
    fileReaderActor ! record
  }

  source.close()

}

class ReaderActor extends Actor with ActorLogging {

  val workerRouter: ActorRef = context.actorOf(RoundRobinPool(10).props(Props[ChildActor]), "workerRouter")
  val errorHandler: ActorRef = context.actorOf(Props[ErrorHandler], "errorHandler")

  override def receive: Receive = {

    case record: Array[String] =>
      workerRouter ! record

    case Terminated(child) =>
      log.info(s"Child actor ${child.path.name} terminated")

  }
}

class ChildActor extends Actor with ActorLogging {

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def receive: Receive = {

    case record: Array[String] =>

      val dateFormat1 = DateTimeFormatter.ofPattern("MM/dd/yyyy")
      val dateFormat2 = DateTimeFormatter.ofPattern("M/d/yyyy")
      val dateFormat3 = DateTimeFormatter.ofPattern("M/dd/yyyy")
      val dateFormat4 = DateTimeFormatter.ofPattern("MM/d/yyyy")

      val result = Try(
        Try(LocalDate.parse(record(0), dateFormat1)).orElse(Try(LocalDate.parse(record(0), dateFormat2)))
          .orElse(Try(LocalDate.parse(record(0), dateFormat3))).orElse(Try(LocalDate.parse(record(0), dateFormat4))),
        Try(LocalDate.parse(record(1), dateFormat1)).orElse(Try(LocalDate.parse(record(1), dateFormat2)))
          .orElse(Try(LocalDate.parse(record(1), dateFormat3))).orElse(Try(LocalDate.parse(record(1), dateFormat4))),
        record(2),
        record(3),
        record(4),
        record(5),
        record(6),
        record(7),
        record(8),
        record(9),
        record(10),
        record(11),
        record(12).toDouble,
        record(13).toInt,
        record(14).toDouble,
        record(15).toDouble
      )

      result match {

        case Success(data) =>
          if (validation(record)) {
            val source = Source.single(data)
            val sink = Sink.foreach(println)
            source.to(sink).run()
          }

        case Failure(ex) =>
          // Handle the validation error
          // ...
          log.error(s"An exception occurred during record processing: ${ex.getMessage}")
          context.actorSelection("/user/fileReaderActor/errorHandler") ! ex

      }

  }
}

class ErrorHandler extends Actor with ActorLogging {

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def receive: Receive = {

    case ex: Throwable =>
      // Log the error
      // ...
      log.error(s"An exception occurred: ${ex.getMessage}")
      Source.single(ex).runWith(Sink.foreach(println))

  }
}

object RecordProcessor {

  def validation(record: Array[String]): Boolean = {
    for (i <- record) {
      if (i == "" || i == null) {
        return false
      }
    }
    true
  }

}

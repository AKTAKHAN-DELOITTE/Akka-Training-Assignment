
import akka.stream.scaladsl._
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import scala.Businesscheckpoint5.applicationConf

import java.nio.file.StandardOpenOption._
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Date
import scala.concurrent.Future
import scala.util.Try

object BusinessCheckpoint6 extends App {

  val applicationConf = ConfigFactory.load("application.conf") //loading from configuration file
  val filepath = applicationConf.getString("app.filepath") //getting file path from configuration file
  //  val source = Source.fromFile(filepath)
  val fileSource: Source[ByteString, Future[IOResult]] =
    FileIO.fromPath(Paths.get(filepath))
  implicit val system: ActorSystem = ActorSystem("MyActorSystem")
  implicit val mat: Materializer = Materializer.createMaterializer(system)

  val sink = FinancialYearAggregatorFlow(mat, system).log("FinancialYearAggregatorFlow").to(CategorywiseFinancialYearSink(mat, system))

  fileSource
    .via(PurchaseParserFlow())
    .log("purchaseParserFlow")
    .to(sink)
    .run()


  def FinancialYearAggregatorFlow(implicit mat: Materializer, system: ActorSystem): Flow[ByteString, (String, Double, Int, Double, Double), NotUsed] = {
    val financialYear = system.settings.config.getInt("app.FinancialYear")
    val categoryFilter = applicationConf.getString("app.categoryFilter")
    //    val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val dateFormat1 = DateTimeFormatter.ofPattern("MM/dd/yyyy")
    val dateFormat2 = DateTimeFormatter.ofPattern("M/d/yyyy")
    val dateFormat3 = DateTimeFormatter.ofPattern("M/dd/yyyy")
    val dateFormat4 = DateTimeFormatter.ofPattern("MM/d/yyyy")


    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
      .filter(record => {

        val fields = record.utf8String.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).map(_.trim)
        try {
          fields(9) == categoryFilter
        }
        catch {
          case x: ArrayIndexOutOfBoundsException => false
        }

      })
      .map(record => {
        val fields = record.utf8String.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).map(_.trim)


        try {
          fields(12).toDouble

        }
        catch {
          case numberFormatException: NumberFormatException => {
            fields(11) = fields(11) + fields(12)
            fields(12) = fields(13)
            fields(13) = fields(14)
            fields(14) = fields(15)
            fields(15) = fields(16)
          }
        }

        Purchase(
          Try(LocalDate.parse(fields(0), dateFormat1)).orElse(Try(LocalDate.parse(fields(0), dateFormat2)))
            .orElse(Try(LocalDate.parse(fields(0), dateFormat3))).orElse(Try(LocalDate.parse(fields(0), dateFormat4))).getOrElse(LocalDate.now()),
          Try(LocalDate.parse(fields(1), dateFormat1)).orElse(Try(LocalDate.parse(fields(1), dateFormat2)))
            .orElse(Try(LocalDate.parse(fields(1), dateFormat3))).orElse(Try(LocalDate.parse(fields(1), dateFormat4))).getOrElse(LocalDate.now()),
          fields(2),
          fields(3),
          fields(4),
          fields(5),
          fields(6),
          fields(7),
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toDouble,
          fields(13).toInt,
          fields(14).toDouble,
          fields(15).toDouble

        )

      })

      .filter(p => {
        Try(LocalDate.parse(p.orderdate.toString, dateFormat1).getYear).orElse(Try(LocalDate.parse(p.orderdate.toString, dateFormat2).getYear))
          .orElse(Try(LocalDate.parse(p.orderdate.toString, dateFormat3).getYear)).orElse(Try(LocalDate.parse(p.orderdate.toString, dateFormat4).getYear)).getOrElse(LocalDate.now().getYear)
      } == financialYear)
      .groupBy(Int.MaxValue, _.category)
      .fold(("", 0.0, 0, 0.0, 0.0)) {
        case ((_, sumSales, sumQuantity, sumDiscount, sumProfit), p) =>
          (p.category, sumSales + p.sales, sumQuantity + p.quantity, sumDiscount + p.discount, sumProfit + p.profit)
      }
      .mergeSubstreams
  }

  def CategorywiseFinancialYearSink(implicit mat: Materializer, system: ActorSystem): Sink[(String, Double, Int, Double, Double), Future[IOResult]] = {
    val fileName = system.settings.config.getString("app.CategorywiseFinancialYearSinkFile")
    val fileSink = FileIO.toPath(Paths.get(fileName), Set(CREATE, WRITE, APPEND))

    Flow[(String, Double, Int, Double, Double)]
      .map {
        case (category, sumSales, sumQuantity, sumDiscount, sumProfit) =>
          val outputString = s"$category,$sumSales,$sumQuantity,$sumDiscount,$sumProfit\n"
          println(outputString) // added line to print output to console
          ByteString(outputString)
      }
      .toMat(fileSink)(Keep.right)
  }

  def PurchaseParserFlow(): Flow[ByteString, ByteString, NotUsed] = {
    Flow[ByteString]
      .map(_.utf8String.trim)
      .map(line => ByteString(line.replace("\"", "") + "\n"))
  }

}

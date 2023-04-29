import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Flow, Framing, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory


import java.nio.file.Paths
import scala.concurrent.Future

object BusinessCheckpoint7 extends App {
  val applicationConf = ConfigFactory.load("application.conf") //loading from configuration file
  val filepath = applicationConf.getString("app.filepath") //getting file path from configuration file

  implicit val system = ActorSystem("FileReaderSystem")


  val categoryFilter = applicationConf.getString("app.categoryFilter") //getting category filter from configuration file
  val FinancialYear = applicationConf.getString("app.year") //getting category filter from configuration file
  val categorySinkFile = applicationConf.getString("app.CategorySinkFile") //getting category sink file from configuration file
  val BulkQuantityValue = applicationConf.getString("app.bqv") //getting category sink file from configuration file
  val bulkcsv = applicationConf.getString("app.BulkProductInsightSinkFile")

  val fileSource: Source[ByteString, Future[IOResult]] =
    FileIO.fromPath(Paths.get(filepath))


  val categoryFilterFlow: Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
      .filter(record => {
        val fields = record.utf8String.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim)
        fields(9) == categoryFilter
      })

  val financialYearAggregatorFlow: Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
      .filter(record => {
        val fields = record.utf8String.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim)
        fields(1).contains(FinancialYear)
      })
      .fold((0.0, 0.0, 0.0, 0.0)) { (acc, record) =>
        val fields = record.utf8String.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim)
        val sales = fields(12).toDouble
        val quantity = fields(13).toDouble
        val discount = fields(14).toDouble
        val profit = fields(15).toDouble
        (acc._1 + sales, acc._2 + quantity, acc._3 + discount, acc._4 + profit)
      }
      .map { result =>
        ByteString(s"${FinancialYear},${result._1},${result._2},${result._3},${result._4}\n")
      }

  val bcsv = FileIO.toPath(Paths.get(bulkcsv))

  val bulkProductFilterFlow: Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
      .filter(record => {
        val fields = record.utf8String.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim)
        fields(2).toInt >= BulkQuantityValue.toInt
      })

  // Define the aggregator flow
  val bulkProductAggregatorFlow: Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
      .filter(record => {
        val fields = record.utf8String.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim)
        fields(1).contains(FinancialYear)
      })
      .fold((0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)) { (acc, record) =>
        val fields = record.utf8String.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim)
        val sales = fields(4).toDouble
        val quantity = fields(6).toDouble
        val discount = fields(7).toDouble
        val profit = fields(13).toDouble
        (
          acc._1 + profit,
          acc._2 + discount,
          acc._3 + quantity,
          acc._4 + sales,
          acc._5 + 1.0,
          acc._6 + profit / quantity,
          acc._7 + sales / quantity,
          acc._8 + 1.0
        )
      }
      .map { result =>
        ByteString(
          s"${FinancialYear}," +
            s"${result._1}," +
            s"${result._2}," +
            s"${result._1 / result._5}," +
            s"${result._2 / result._5}," +
            s"${result._3}," +
            s"${result._3 / result._5}," +
            s"${result._4}," +
            s"${result._4 / result._8}\n"
        )
      }


  fileSource
    .via(categoryFilterFlow)
    .via(financialYearAggregatorFlow)
    .via(bulkProductFilterFlow)
    .via(bulkProductAggregatorFlow)
    .to(bcsv).run()
}
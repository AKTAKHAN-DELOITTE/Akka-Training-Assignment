import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.stream.scaladsl.{FileIO, Flow, Framing, Source}
import akka.util.{ByteString, Timeout}

import java.nio.file.Paths
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

object BusinessCheckpoint8 extends App {
  case class ValidationFailureRecord(errorMessage: String, columnName: String, data: String)

  class ValidationSource(validationActor: ActorRef, filePath: String)(implicit ec: ExecutionContext) {

    implicit val system: ActorSystem = ActorSystem("mySystem")

    implicit val timeout: Timeout = Timeout(5.seconds)

    val fileSource = FileIO.fromPath(Paths.get(filePath))
    val lineSource = fileSource.via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))

    val validationFlow = Flow[String].mapAsyncUnordered(4) { line =>
      (validationActor ? line.split(",")).mapTo[Either[ValidationFailureRecord, String]]
    }

    val validationSource = lineSource
      .map(_.utf8String.trim)
      .via(validationFlow)
      .collect { case Left(validationFailureRecord) => validationFailureRecord }

  }


}
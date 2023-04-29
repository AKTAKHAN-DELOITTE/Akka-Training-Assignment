
import akka.actor.Actor
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}

object BusinessCheckpoint4 extends App {
  class ErrorHandler extends Actor {
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    override def receive: Receive = {
      case ex: Throwable =>
        // Log the error
        Source.single(ex).runWith(Sink.foreach(println))
    }
  }

}

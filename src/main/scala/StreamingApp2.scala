import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import akka.actor.Actor
import akka.actor.Props
import akka.actor.actorRef2Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.ActorHelper
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import org.apache.spark.streaming._

class HelloerMv extends Actor with ActorHelper {
  override def preStart() = {
    println("")
    println("=== HelloerMv is starting up ===")
    println(s"=== path=${context.self.path} ===")
    println("")
  }
  def receive = {
    case (k: String, v: BigDecimal) =>
      store((k, v))
  }
}

class HelloerAdj extends Actor with ActorHelper {
  override def preStart() = {
    println("")
    println("=== HelloerAdj is starting up ===")
    println(s"=== path=${context.self.path} ===")
    println("")
  }
  def receive = {
    case (k: String, v: BigDecimal) =>
      store((k, v))
  }
}

object StreamingApp2 {
  def main(args: Array[String]) {
    // Configuration for a Spark application.
    // Used to set various Spark parameters as key-value pairs.
    val driverPort = 7777
    val driverHost = "localhost"
    val conf = new SparkConf(false) // skip loading external settings
      .setMaster("local[*]") // run locally with enough threads
      .setAppName("Spark Streaming with Scala and Akka") // name in Spark web UI
      .set("spark.logConf", "true")
      .set("spark.driver.port", s"$driverPort")
      .set("spark.driver.host", s"$driverHost")
      .set("spark.akka.logLifecycleEvents", "true")
    val ssc = new StreamingContext(conf, Seconds(1))
    val actorNameMv = "helloerMv"
    val actorStreamMv: ReceiverInputDStream[(String, BigDecimal)] = ssc.actorStream[(String, BigDecimal)](Props[HelloerMv], actorNameMv)
    //    actorStreamMv.print()
    val actorNameAdj = "helloerAdj"
    val actorStreamAdj: ReceiverInputDStream[(String, BigDecimal)] = ssc.actorStream[(String, BigDecimal)](Props[HelloerAdj], actorNameAdj)
    //    actorStreamAdj.print()

    val actorStreamAll = new PairDStreamFunctions(actorStreamMv).cogroup(actorStreamAdj)
    val actorStreamLast = new PairDStreamFunctions(actorStreamAll).mapValues(x => (x._1.lastOption, x._2.lastOption))

    def updateFunc(newValuePairs: Seq[(Option[BigDecimal], Option[BigDecimal])], state: Option[(BigDecimal, BigDecimal)]): Option[(BigDecimal, BigDecimal)] = {
      val currentState = state match {
        case Some(v) => v
        case None => (BigDecimal(0), BigDecimal(0))
      }
      val newState = newValuePairs match {
        case x :: xs =>
          newValuePairs.last match {
            case (Some(v), None) => Some(v, currentState._2)
            case (None, Some(v)) => Some(currentState._1, v)
            case (None, None) => state
            case (Some(v1), Some(v2)) => Some(v1, v2)
          }
        case Nil => state
      }
      newState
    }

    def updateFunc2(newValuePairs: Seq[(Option[BigDecimal], Option[BigDecimal])], state: (Option[BigDecimal], Option[BigDecimal])): (Option[BigDecimal], Option[BigDecimal]) = {
      val newState = newValuePairs.last match {
        case (Some(v), None) => (Some(v), state._2)
        case (None, Some(v)) => (state._1, Some(v))
        case (None, None) => state
        case (Some(v1), Some(v2)) => (Some(v1), Some(v2))
      }
      newState
    }

    val actorStreamSum = new PairDStreamFunctions(actorStreamLast).updateStateByKey[(BigDecimal, BigDecimal)](updateFunc _)
    //cogroup(actorStreamAdj)

    //actorStreamLast.print()
    actorStreamSum.print()

    ssc.start()
    Thread.sleep(3 * 1000) // wish I knew a better way to handle the asynchrony

    import scala.concurrent.duration._
    val actorSystem = SparkEnv.get.actorSystem
    val urlMv = s"akka.tcp://spark@$driverHost:$driverPort/user/Supervisor0/$actorNameMv"
    val urlAdj = s"akka.tcp://spark@$driverHost:$driverPort/user/Supervisor1/$actorNameAdj"
    val timeout = 5 seconds
    val helloerMv = Await.result(actorSystem.actorSelection(urlMv).resolveOne(timeout), timeout)
    val helloerAdj = Await.result(actorSystem.actorSelection(urlAdj).resolveOne(timeout), timeout)

    helloerMv ! ("C123", BigDecimal(100))

    helloerAdj ! ("C123", BigDecimal(10))

    helloerMv ! ("C123", BigDecimal(120))

    helloerAdj ! ("C123", BigDecimal(20))

    val stopSparkContext = true
    val stopGracefully = true
    ssc.stop(stopSparkContext, stopGracefully)
  }
}
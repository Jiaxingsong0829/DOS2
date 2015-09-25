import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorSystem
import scala.util.Random
import scala.concurrent.duration.Duration
import scala.collection.mutable.ArrayBuffer
import akka.actor.ActorRef
import scala.math
import scala.concurrent.duration._




sealed trait GossipMessage
case class ReceiveGossip(id: Int)
case class ReceivePushSum(id: Int)
case class SendByGossip(rumor: String)
case class SendByPushSum(sendS: Double, sendW: Double)
case class ResendByGossip(rumor: String)
case class ResendByPushSum(sendS: Double, sendW: Double)
case object Start

object Project2{
  def main(args : Array[String]){
    if (args.length < 3){
      println("Please input the right parameters")
    }
    else{
      val system = ActorSystem("GossipSystem");
      val masterActor = system.actorOf(Props(new MasterActor(args(0).toInt, args(1), args(2))), "master");
      masterActor ! Start
    }
  }
}





//master Actor
class MasterActor(numNode: Int, topology : String, algorithm: String) extends Actor{
  var childDoneState = Array.fill[Boolean](numNode)(false)
  val rumor = "balalala"
  for (i <- 1 to numNode){
    context.actorOf(Props(new ClientActor(i, numNode, topology)),"Node"+ i.toString);
  }
  val firstSendNode = context.actorSelection("Node1")
  def receive = {
    case Start =>
      println(algorithm)
      algorithm.toLowerCase() match{
        case "gossip" =>
          println("Start using gossip")
          firstSendNode ! SendByGossip(rumor)
        case "pushsum" =>
          firstSendNode ! SendByPushSum(1.0, 1.0)
      }
    case ReceiveGossip(id) =>
      println("master receive message-----------------------------------------------" + id + "numNode = " + numNode)

      childDoneState.update(id - 1, true)
      receiveMessage(childDoneState, numNode)
    case ReceivePushSum(id) =>
      childDoneState.update(id - 1, true)
      receiveMessage(childDoneState, numNode)
  }
  def receiveMessage(childDoneState : Array[Boolean], numNode: Int) {
    println("master receive message")
    var count = 0
    for (x <- childDoneState)
      if (x == true) count = count + 1
    val percentCovered = (count.toDouble / numNode) * 100
    println("count =" + count +"......Percentage complete: " + percentCovered.toString)
    if (percentCovered > 95.0) {
      context.children.foreach(context.stop(_))
      context.stop(self)
      System.exit(0)
    }
  }
}


  //
  class ClientActor(id: Int, numNode: Int, topology: String) extends Actor {
    var send = ArrayBuffer[String]()
    var rumor = ""
    var sendAlgorithm = ""
    var gossipCount = 0
    var pushsumCount = 0
    var s = id.toDouble
    var w = 1.0
    var preRatio = 0.0
    var ratio = 0.0
    var absRatio = 0.0
    var ifResend = true
    var flagCount = 0
    println(self + "topoly building")
    println("3D".toLowerCase)
    topology.toLowerCase() match {
      case "line" =>
        if (id > 1) {
          send += (id - 1).toString
        }
        if (id < numNode) {
          send += (id + 1).toString
        }
      case "full" =>
        for (i <- 1 to numNode) {
          send += i.toString
        }
      case "3d" =>
        val num = Math.cbrt(numNode).ceil.toInt //
        println("num----" + num)
        val tempId = id - 1
        if (tempId >= num * num) {
            send += (tempId - num * num + 1).toString;
          }
        if (tempId < num * num * num - num * num && tempId + num * num < numNode) {
          send += (tempId + num * num + 1).toString
        }
        if (tempId % (num * num) >= num) {
          send += (tempId - num + 1).toString
        }
        if (tempId % (num * num) < num * num - num && tempId + num < numNode) {
          send += (tempId + num + 1).toString
        }
        if ((tempId % (num * num)) % num > 0  && tempId + 1 <= numNode) {
          send += (tempId - 1 + 1).toString
        }
        if ((tempId % (num * num)) % num < num - 1 && tempId + 1 <= numNode) {
          send += (tempId + 1 + 1).toString
        }
        println("topoly-----"+ id +":" + send)

      case "imperfect3d" =>
        val num = Math.cbrt(numNode).ceil.toInt //
        println("num----" + num)
        val tempId = id - 1
        if (tempId >= num * num) {
          send += (tempId - num * num + 1).toString;
        }
        if (tempId < num * num * num - num * num && tempId + num * num < numNode) {
          send += (tempId + num * num + 1).toString
        }
        if (tempId % (num * num) >= num) {
          send += (tempId - num + 1).toString
        }
        if (tempId % (num * num) < num * num - num && tempId + num < numNode) {
          send += (tempId + num + 1).toString
        }
        if ((tempId % (num * num)) % num > 0  && tempId + 1 <= numNode) {
          send += (tempId - 1 + 1).toString
        }
        if ((tempId % (num * num)) % num < num - 1 && tempId + 1 <= numNode) {
          send += (tempId + 1 + 1).toString
        }
        println("topoly-----"+ id +":" + send)
        var randomSendId = Int.MaxValue
        while (randomSendId > num * num * num || randomSendId < 0) {
          randomSendId = (Random.nextInt % (num * num * num)).abs
        }
        send += randomSendId.toString
    }
    println(self + "topology has been builded")
    def receive = {
      case SendByGossip(rumor: String) =>
        flagCount = 0
        val masterNode = context.parent
        gossipCount = gossipCount + 1
        println(self + "count = " + gossipCount)
        println(self + "receive rumor")
        if (gossipCount == 10) {
          masterNode ! ReceiveGossip(id)
          println(context.self + " is done.")
          context.stop(self)
        }
        if (gossipCount < 10) {
          val length = send.length
          var sendId = Int.MaxValue
          if (length == 1){
            sendId = 0
          }
          else{
            while (sendId > length || sendId < 0) {
              sendId = (Random.nextInt % length).abs
            }
          }
          println("send" + sendId)


          val targetNode = context.actorSelection("../Node" + send(sendId))
          targetNode ! SendByGossip(rumor)
          if (ifResend) {
            self ! ResendByGossip(rumor)
            ifResend = false
          }
        }
      case ResendByGossip(rumor) =>
        flagCount = flagCount + 1
        if (gossipCount < 10 && flagCount < numNode * 10) {
          println(self + "resend rumor")
          val length = send.length
          var sendId = Int.MaxValue
          if (length == 1){
            sendId = 0
          }
          else{
            while (sendId > length || sendId < 0) {
              sendId = (Random.nextInt % length).abs
            }
          }
          val targetNode = context.actorSelection("../Node" + send(sendId))
          targetNode ! SendByGossip(rumor)
          println("resend" + send(sendId))
          import context.dispatcher
          context.system.scheduler.scheduleOnce(50 milliseconds, self, ResendByGossip(rumor));
        }
        else{
          context.stop(self)
          println("resend fail")
        }


      case SendByPushSum(sendS: Double, sendW: Double) =>
        flagCount = 0
        val masterNode = context.actorSelection("../master")
        s = (s + sendS) / 2
        w = (w + sendW) / 2
        preRatio = ratio
        ratio = s / w
        absRatio = (ratio - preRatio).abs
        if (absRatio < 0.0000000001)
          pushsumCount = pushsumCount + 1
        if (pushsumCount == 3) {
          masterNode ! ReceivePushSum(id)
          println(context.self + " is done.")
          context.stop(self)
        }
        if (pushsumCount < 3) {
          val length = send.length
          var sendId = Int.MaxValue
          while (sendId > length || sendId < 0) {
            sendId = (Random.nextInt % length).abs
          }
          val targetNode = context.actorSelection("../Node" + send(sendId))
          targetNode ! SendByPushSum(s, w)
          //if (ifResend) {
            //ResendByPushSum(sendS, sendW)
            //ifResend = false
          //}
        }
      case ResendByPushSum(sendS: Double, sendW: Double) =>
        flagCount = flagCount + 1
        if (pushsumCount < 3 && flagCount < numNode * 10) {
          println(self + "resend by pushsum")
          val length = send.length
          var sendId = Int.MaxValue
          if (length == 1){
            sendId = 0
          }
          else{
            while (sendId > length || sendId < 0) {
              sendId = (Random.nextInt % length).abs
            }
          }
          val targetNode = context.actorSelection("../Node" + send(sendId))
          targetNode ! SendByPushSum(sendS, sendW)
          println("resend" + send(sendId))
          import context.dispatcher
          context.system.scheduler.scheduleOnce(50 milliseconds, self, ResendByPushSum(sendS, sendW));
        }
        else {
          context.stop(self)
        }
    }
  }

package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case object RetryAcks

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  val retryInterval = 100.milliseconds
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case replicateMsg@(Replicate(key, valueOption, id)) =>
      val expected = nextSeq
      acks = acks.updated(expected, (sender(), replicateMsg))
      replica ! Snapshot(key, valueOption, expected)
      context.system.scheduler.scheduleOnce(retryInterval, self, RetryAcks)
    case SnapshotAck(key, seq) =>
      acks.get(seq) match {
        case None =>
        case Some((senderRef, replicateMsg)) =>
          acks = acks - seq
          senderRef ! Replicated(key, replicateMsg.id)
      }
    case RetryAcks =>
      acks.isEmpty match {
        case true =>
        case false =>
          acks.foreach {
            case (seq, (requester, msg)) =>
              replica ! Snapshot(msg.key, msg.valueOption, seq)
          }
          context.system.scheduler.scheduleOnce(retryInterval, self, RetryAcks)
      }
  }
}

package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import kvstore.Persistence.Persist
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case object Retry
  case class AckTimeouted(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var expectedSeq: Long = 0
  val persistence = context.system.actorOf(persistenceProps)
  var nextMessageId: Long = 1000
  var pendingPersist = Map.empty[Long, (ActorRef, Long, Persist)]

  var acks = Map.empty[Long, (ActorRef, Long, Set[Long])]
  var pPersists = Map.empty[Long, (Persist, Boolean)]
  var pReplicates = Map.empty[Long, (ActorRef, Replicate)]
  var msgId2ReqId = Map.empty[Long, Long]

  val retryInterval = 100.milliseconds
  val timeout = 1.second
  var fakeReqId = -1L

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def nextMsgId: Long = {
    val id = nextMessageId
    nextMessageId += 1
    id
  }

  def nextFakeReqId: Long = {
    val id = fakeReqId
    fakeReqId -= 1
    id
  }

  def startReplicate(reqId: Long, rKey: String, rOpt: Option[String], reps: Set[ActorRef]): Set[Long] = {
    var rIds = Set.empty[Long]
    for (replicator <- reps) {
      val rId = nextMsgId
      val rMsg = Replicate(rKey, rOpt, rId)
      rIds = rIds + rId
      msgId2ReqId = msgId2ReqId.updated(rId, reqId)
      pReplicates = pReplicates.updated(rId, (replicator, rMsg))
      replicator ! rMsg
    }
    rIds
  }

  def startPersist(reqId: Long, pKey: String, pOpt: Option[String]): Long = {
    val pId = nextMsgId
    val pMsg = Persist(pKey, pOpt, pId)
    pPersists = pPersists.updated(pId, (pMsg, false))
    msgId2ReqId = msgId2ReqId.updated(pId, reqId)
    persistence ! pMsg
    pId
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv = kv.updated(key, value)
      val opt = Some(value)
      val pId = startPersist(id, key, opt)
      val rIds = startReplicate(id, key, opt, replicators)
      acks = acks.updated(id, (sender(), pId, rIds))
      //println(s"Need Ack($id) for ($pId, $rIds)")
      context.system.scheduler.scheduleOnce(retryInterval, self, Retry)
      context.system.scheduler.scheduleOnce(timeout, self, AckTimeouted(id))
    case Remove(key, id) =>
      kv = kv - key
      val opt = None
      val pId = startPersist(id, key, opt)
      val rIds = startReplicate(id, key, opt, replicators)
      acks = acks.updated(id, (sender(), pId, rIds))
      //println(s"Need Ack($id) for ($pId, $rIds)")
      context.system.scheduler.scheduleOnce(retryInterval, self, Retry)
      context.system.scheduler.scheduleOnce(timeout, self, AckTimeouted(id))
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Replicas(replicas) =>
      val newReplicas = (replicas - self) -- secondaries.keySet
      var newReplicators = Set.empty[ActorRef]
      for (replica <- newReplicas) {
        val replicator = context.system.actorOf(Replicator.props(replica))
        newReplicators = newReplicators + replicator
        secondaries = secondaries.updated(replica, replicator)
        replicators = replicators + replicator
      }
      for ((k, v) <- kv) {
        val frId = nextFakeReqId;
        val rIds = startReplicate(frId, k, Some(v), newReplicators)
        acks = acks.updated(frId, (sender(), -1L, rIds))
      }

      val removedReplicas = secondaries.keySet -- replicas
      val removedReplicators = for {
        replica <- removedReplicas
        replicator <- secondaries.get(replica)
      } yield replicator
      for (replica <- removedReplicas) {
        val replicator = secondaries(replica)
        replicator ! PoisonPill
        replicators = replicators - replicator
        secondaries = secondaries - replica
      }

      for ((rid, (replicator, msg)) <- pReplicates; if removedReplicators.contains(replicator))
        self ! Replicated("", rid)

    case Persisted(key, pid) =>
      //println(s"Received Persisted($pid)")
      val reqId = msgId2ReqId(pid)
      acks.get(reqId) match {
        case None =>
        case Some((ref, ackPid, rids)) =>
          //println(s"Ack state: ($ackPid, true, $rids)")
          ////assert(pid == ackPid)
          if (rids.isEmpty) {
            acks = acks - reqId
            pPersists = pPersists - pid
            if (reqId >= 0)
              ref ! OperationAck(reqId)
          } else {
            pPersists.get(pid) match {
              case None =>
              case Some((msg, acked)) =>
                pPersists = pPersists.updated(pid, (msg, true))
            }
          }
      }
    case Replicated(_, rid) =>
      //println(s"Received Replicated($rid)")
      val reqId = msgId2ReqId(rid)
      acks.get(reqId) match {
        case None =>
        case Some((ref, ackPid, rids)) =>
          //assert(rids.contains(rid))
          val remained = rids - rid
          val persistAcked = pPersists.get(ackPid) match {
            case None => true
            case Some((msg, acked)) => acked
          }
          //println(s"Ack state: ($ackPid, $persistAcked, $rids)")
          if (persistAcked && remained.isEmpty) {
            acks = acks - reqId
            pPersists = pPersists - ackPid
            if (reqId >= 0)
              ref ! OperationAck(reqId)
          } else {
            acks = acks.updated(reqId, (ref, ackPid, remained))
          }
      }
      pReplicates = pReplicates - rid
    case Retry =>
       if (pPersists.isEmpty && pReplicates.isEmpty) {
       } else {
         pPersists.foreach {
           case (pid, (msg, acked)) =>
             if (!acked)
               persistence ! msg
         }
         pReplicates.foreach {
           case (rid, (replicator, msg)) =>
             replicator ! msg
         }
         context.system.scheduler.scheduleOnce(retryInterval, self, Retry)
       }
    case AckTimeouted(id) =>
      acks.get(id) match {
        case None =>
        case Some((ref, _, _)) =>
          ref ! OperationFailed(id)
      }
  }

  /* TODO Behavior for the replica role. */
  val replica : Receive = {
    case Snapshot(key, valueOption, seq) =>
      if (seq < expectedSeq) {
        sender ! SnapshotAck(key, seq)
      } else if (seq == expectedSeq) {
        valueOption match {
          case Some(value) =>
            kv = kv.updated(key, value)
          case None =>
            kv = kv - key
        }
        expectedSeq += 1
        val persistId = nextMsgId
        val persistMsg = Persist(key, valueOption, persistId)
        pendingPersist = pendingPersist.updated(persistId, (sender(), seq, persistMsg))
        persistence ! persistMsg
        context.system.scheduler.scheduleOnce(retryInterval, self, Retry)
      }
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      pendingPersist.get(id) match {
        case None =>
        case Some((ref, seq, msg)) =>
          pendingPersist = pendingPersist - id
          ref ! SnapshotAck(key, seq)
      }
    case Retry =>
      pendingPersist.isEmpty match {
        case true =>
        case false =>
          pendingPersist.foreach {
            case (_, (ref, seq, msg)) =>
              persistence ! msg
          }
          context.system.scheduler.scheduleOnce(retryInterval, self, Retry)
      }
  }
}


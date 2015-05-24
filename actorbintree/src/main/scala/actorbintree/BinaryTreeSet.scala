/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case opt: Operation =>
      root ! opt
    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      pendingQueue = Queue.empty[Operation]
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished =>
      for (opt <- pendingQueue)
        newRoot ! opt
      root = newRoot
      context.become(normal)
      sender ! PoisonPill
    case opt: Operation =>
      pendingQueue = pendingQueue.enqueue(opt)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case opt@(Insert(requester, id, elem2)) => elem2 compareTo elem match {
      case 0 => removed match {
        case false => requester ! OperationFinished(id)
        case true =>
          removed = false
          requester ! OperationFinished(id)
      }
      case i if i < 0 => subtrees.get(Left) match {
        case None =>
          subtrees = subtrees.updated(Left, context.actorOf(BinaryTreeNode.props(elem2, initiallyRemoved = false)))
          requester ! OperationFinished(id)
        case Some(a) => a ! opt
      }
      case i if i > 0 => subtrees.get(Right) match {
        case None =>
          subtrees = subtrees.updated(Right, context.actorOf(BinaryTreeNode.props(elem2, initiallyRemoved = false)))
          requester ! OperationFinished(id)
        case Some(a) => a ! opt
      }
    }
    case opt@(Contains(requester, id, elem2)) => elem2 compareTo elem match {
      case 0 => requester ! ContainsResult(id, result = !removed)
      case i if i < 0 => subtrees.get(Left) match {
        case None => requester ! ContainsResult(id, result = false)
        case Some(a) => a ! opt
      }
      case i if i > 0 => subtrees.get(Right) match {
        case None => requester ! ContainsResult(id, result = false)
        case Some(a) => a ! opt
      }
    }
    case opt@(Remove(requester, id, elem2)) => elem2 compareTo elem match {
      case 0 =>
        removed = true
        requester ! OperationFinished(id)
      case i if i < 0 => subtrees.get(Left) match {
        case None => requester ! OperationFinished(id)
        case Some(a) => a ! opt
      }
      case i if i > 0 => subtrees.get(Right) match {
        case None => requester ! OperationFinished(id)
        case Some(a) => a ! opt
      }
    }
    case CopyTo(newRoot) =>
      if (!removed) {
        newRoot ! Insert(self, Int.MaxValue, elem)
      }

      val expected: Set[ActorRef] = (subtrees.get(Left), subtrees.get(Right)) match {
        case (None, None) =>
          Set()
        case (None, Some(r)) =>
          r ! CopyTo(newRoot)
          Set(r)
        case (Some(l), None) =>
          l ! CopyTo(newRoot)
          Set(l)
        case (Some(l), Some(r)) =>
          l ! CopyTo(newRoot)
          r ! CopyTo(newRoot)
          Set(l, r)
      }
      if (expected.isEmpty && removed)
        context.parent ! CopyFinished
      else
        context.become(copying(expected, insertConfirmed = removed))
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) if id == Int.MaxValue =>
      assert(!insertConfirmed)
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.become(normal)
      } else {
        context.become(copying(expected,  insertConfirmed = true))
      }
    case CopyFinished =>
      assert(expected contains sender() )
      val remainder = expected - sender
      if (remainder.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.become(normal)
      } else
        context.become(copying(remainder, insertConfirmed))
  }
}

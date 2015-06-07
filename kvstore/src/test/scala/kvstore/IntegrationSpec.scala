/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor._
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.duration._
import org.scalatest.FunSuiteLike
import org.scalactic.ConversionCheckedTripleEquals

import scala.util.Random

class IntegrationSpec(_system: ActorSystem) extends TestKit(_system)
with FunSuiteLike
with Matchers
with BeforeAndAfterAll
with ConversionCheckedTripleEquals
with ImplicitSender
with Tools {

  import Replica._
  import Replicator._
  import Arbiter._

  def this() = this(ActorSystem("ReplicatorSpec"))

  override def afterAll: Unit = system.shutdown()

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */
//  test("case3: Primary and secondaries must work in concert when persistence and communication to secondaries is unreliable") {
//
//    val arbiter = TestProbe()
//
//    val (primary, user) = createPrimary(arbiter, "case3-primary", flakyPersistence = true)
//
//    user.setAcked("k1", "v1")
//
//    val (secondary1, replica1) = createSecondary(arbiter, "case3-secondary1")
//    val (secondary2, replica2) = createSecondary(arbiter, "case3-secondary2")
//
//    arbiter.send(primary, Replicas(Set(primary, secondary1, secondary2)))
//
//    val options = Set(None, Some("v1"))
//    options should contain(replica1.get("k1"))
//    options should contain(replica2.get("k1"))
//
//    user.setAcked("k1", "v2")
//    assert(replica1.get("k1") === Some("v2"))
//    assert(replica2.get("k1") === Some("v2"))
//
//    val (secondary3, replica3) = createSecondary(arbiter, "case3-secondary3")
//
//    arbiter.send(primary, Replicas(Set(primary, secondary1, secondary3)))
//
//    replica3.nothingHappens(500.milliseconds)
//
//    assert(replica3.get("k1") === Some("v2"))
//
//    user.removeAcked("k1")
//    assert(replica1.get("k1") === None)
//    assert(replica2.get("k1") === Some("v2"))
//    assert(replica3.get("k1") === None)
//
//    user.setAcked("k1", "v4")
//    assert(replica1.get("k1") === Some("v4"))
//    assert(replica2.get("k1") === Some("v2"))
//    assert(replica3.get("k1") === Some("v4"))
//
//    user.setAcked("k2", "v1")
//    user.setAcked("k3", "v1")
//
//    user.setAcked("k1", "v5")
//    user.removeAcked("k1")
//    user.setAcked("k1", "v7")
//    user.removeAcked("k1")
//    user.setAcked("k1", "v9")
//    assert(replica1.get("k1") === Some("v9"))
//    assert(replica2.get("k1") === Some("v2"))
//    assert(replica3.get("k1") === Some("v9"))
//
//    assert(replica1.get("k2") === Some("v1"))
//    assert(replica2.get("k2") === None)
//    assert(replica3.get("k2") === Some("v1"))
//
//    assert(replica1.get("k3") === Some("v1"))
//    assert(replica2.get("k3") === None)
//    assert(replica3.get("k3") === Some("v1"))
//  }
//
//  def createPrimary(arbiter: TestProbe, name: String, flakyPersistence: Boolean) = {
//
//    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flakyPersistence)), name)
//
//    arbiter.expectMsg(Join)
//    arbiter.send(primary, JoinedPrimary)
//
//    (primary, session(primary))
//  }
//
//  def createSecondary(arbiter: TestProbe, name: String, successFactor: Int = 1) = {
//
//    val replica: Props = Replica.props(arbiter.ref, Persistence.props(flaky = false))
//
//    val secondary =
//      if (successFactor > 1) system.actorOf(Props(new FlakyForwarder(replica, name, successFactor)), s"flaky-$name")
//      else system.actorOf(replica, name)
//
//    arbiter.expectMsg(Join)
//    arbiter.send(secondary, JoinedSecondary)
//
//    (secondary, session(secondary))
//  }
//}

//class FlakyForwarder(targetProps: Props, name: String, successFactor: Int) extends Actor with ActorLogging {
//
//  import Replicator._
//  val child = context.actorOf(targetProps, name)
//
//  def receive = {
//
//    case msg if sender == child =>
//      context.parent forward msg
//
//    case msg:Snapshot =>
//      if (Random.nextInt(successFactor) > 0) child forward msg
//      else log.debug(s"Dropping $msg")
//
//    case msg =>
//      child forward msg
//  }
}
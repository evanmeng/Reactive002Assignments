package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min3") = forAll { (a1: Int, a2: Int, a3: Int) =>
    val amin = List(a1, a2, a3).min
    val h = insert(a3, insert(a2, insert(a1, empty)))
    findMin(h) == amin
  }

  property("del2") = forAll { (a1: Int, a2: Int, a3: Int) =>
    val amax = List(a1, a2, a3).max
    val h = insert(a3, insert(a2, insert(a1, empty)))
    val h1 = deleteMin(deleteMin(h))
    findMin(h1) == amax
  }

  property("larger") = forAll { (a1: Int, a2: Int) =>
    val bigger = a1 max a2
    val smaller = a1 min a2
    val h = insert(bigger, insert(smaller, empty))
    findMin(h) == smaller
  }

  property("smaller") = forAll { (a1: Int, a2: Int) =>
    val bigger = a1 max a2
    val smaller = a1 min a2
    val h = insert(smaller, insert(bigger, empty))
    findMin(h) == smaller
  }

  property("dup") = forAll { a: Int =>
    val h = insert(a, insert(a, insert(a, empty)))
    findMin(h) == a
  }

  lazy val genHeap: Gen[H] = {
    val isEmpty = oneOf(true, false).sample
    isEmpty match {
      case None => empty
      case Some(true) => empty
      case Some(false) =>
        for {
          elem <- choose[A](0, 10000)
          h <- genHeap
        } yield insert(elem, h)
    }
  }

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}

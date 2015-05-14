package calculator

import scala.math.sqrt

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    val delta = (va: Double, vb: Double, vc: Double) => vb * vb - 4 * va * vc
    Signal[Double](delta(a(), b(), c()))
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    val solution = (a: Double, b: Double, delta: Double) => delta match {
      case 0 => Set(-b / 2 / a)
      case x if x > 0 => Set((-b + sqrt(delta)) / 2 / a, (-b - sqrt(delta)) / 2 / a)
      case x if x < 0 => Set[Double]()
    }
    Signal[Set[Double]](solution(a(), b(), delta()))
  }
}

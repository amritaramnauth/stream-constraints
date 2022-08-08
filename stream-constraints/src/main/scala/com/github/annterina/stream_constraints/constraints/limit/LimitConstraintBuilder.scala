package com.github.annterina.stream_constraints.constraints.limit


class LimitConstraintBuilder[K, V] {

  def limit(limit: ((K, V) => Boolean, String)): NumberToLimitStep[K, V] = {
    new NumberToLimitStep[K, V](limit)
  }

  final class NumberToLimitStep[K, V](limit: ((K, V) => Boolean, String)) {
    def numberToLimit(numberToLimit: Int): LimitConstraint[K, V] = {
      new LimitConstraint[K, V](limit, numberToLimit)
    }
  }

}
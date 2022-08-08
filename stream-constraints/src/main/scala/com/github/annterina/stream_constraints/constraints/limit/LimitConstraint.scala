package com.github.annterina.stream_constraints.constraints.limit

case class LimitConstraint[K, V](limit: ((K, V) => Boolean, String),
                                  numberToLimit: Int) {}
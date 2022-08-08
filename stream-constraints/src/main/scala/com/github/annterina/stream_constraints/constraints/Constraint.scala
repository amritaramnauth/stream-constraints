package com.github.annterina.stream_constraints.constraints

import com.github.annterina.stream_constraints.constraints.deduplicate.DeduplicateConstraint
import com.github.annterina.stream_constraints.constraints.limit.LimitConstraint
import com.github.annterina.stream_constraints.constraints.window.WindowConstraint
import org.apache.kafka.common.serialization.Serde
import java.util.function.BiFunction
import java.time.Duration

case class Constraint[K, V, L](prerequisites : Set[Prerequisite[K, V]],
                                    windowConstraints: Set[WindowConstraint[K, V]],
                                    terminals: Set[Terminal[K, V]],
                                    deduplicates: Set[DeduplicateConstraint[K, V]],
                                    limits: Set[LimitConstraint[K, V]],
                                    names: Map[String, (K, V) => Boolean],
                                    redirectTopic: Option[String],
                                    withFullWindows: Boolean) {

  var link: (K, V) => L = _
  var linkSerde: Serde[L] = _

  var keySerde: Serde[K] = _
  var valueSerde: Serde[V] = _

  def withLink(f: (K, V) => L, serde: Serde[L]): Constraint[K, V, L] = {
    link = f
    linkSerde = serde
    this
  }

  def withKeySerde(keySerdes: Serde[K]): Constraint[K, V, L] = {
    keySerde = keySerdes
    this
  }

  def withValueSerde(valueSerdes: Serde[V]): Constraint[K, V, L] = {
    valueSerde = valueSerdes
    this
  }
}

case class Prerequisite[K, V](before: ((K, V) => Boolean, String), after: ((K, V) => Boolean, String)) {}

case class Terminal[K, V](terminal: ((K, V) => Boolean, String)) {}

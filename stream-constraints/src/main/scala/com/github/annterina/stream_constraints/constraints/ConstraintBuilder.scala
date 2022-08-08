package com.github.annterina.stream_constraints.constraints

import com.github.annterina.stream_constraints.constraints.deduplicate.DeduplicateConstraint
import com.github.annterina.stream_constraints.constraints.limit.LimitConstraint
import com.github.annterina.stream_constraints.constraints.window.WindowConstraint
import org.apache.kafka.common.serialization.Serde

import scala.collection.mutable
import java.util.concurrent.TimeUnit
import java.util.function.BiFunction
import java.time.Duration

class ConstraintBuilder[K, V, L] {

  private val prerequisites: mutable.Set[Prerequisite[K, V]] = mutable.Set.empty
  private val windowConstraints: mutable.Set[WindowConstraint[K, V]] = mutable.Set.empty
  private val terminals: mutable.Set[Terminal[K, V]] = mutable.Set.empty
  private val deduplicates: mutable.Set[DeduplicateConstraint[K, V]] = mutable.Set.empty
  private val limits: mutable.Set[LimitConstraint[K, V]] = mutable.Set.empty

  private val constraintNames: mutable.Map[String, (K, V) => Boolean] = mutable.Map.empty
  private var redirectTopic: Option[String] = None
  private var fullWindows: Boolean = false


  def prerequisite(before: ((K, V) => Boolean, String), after: ((K, V) => Boolean, String)): ConstraintBuilder[K, V, L] = {
    prerequisites.add(new Prerequisite[K, V](before, after))
    constraintNames += before.swap
    constraintNames += after.swap
    this
  }

  def windowConstraint(constraint: WindowConstraint[K, V]): ConstraintBuilder[K, V, L] = {
    windowConstraints.add(constraint)
    constraintNames += constraint.before.swap
    constraintNames += constraint.after.swap
    this
  }

  def terminal(terminal: ((K, V) => Boolean, String)): ConstraintBuilder[K, V, L] = {
    terminals.add(new Terminal[K, V](terminal))
    constraintNames += terminal.swap
    this
  }

  def deduplicate(constraint: DeduplicateConstraint[K, V]): ConstraintBuilder[K, V, L] = {
    deduplicates.add(constraint)
    this
  }

  def limitConstraint(constraint: LimitConstraint[K, V]): ConstraintBuilder[K, V, L] = {
    limits.add(constraint)
    this
  }

  def redirect(topic: String): ConstraintBuilder[K, V, L] = {
    redirectTopic = Some(topic)
    this
  }

  def withFullWindows(): ConstraintBuilder[K, V, L] = {
    fullWindows = true
    this
  }


  def link(f: (K, V) => L)(implicit serde: Serde[L]): ConditionConstraintBuilder[K, V, L] = {
    val constraint = Constraint[K, V, L](prerequisites.toSet, windowConstraints.toSet, terminals.toSet,
       deduplicates.toSet, limits.toSet, constraintNames.toMap, redirectTopic, fullWindows).withLink(f, serde)
    new ConditionConstraintBuilder[K, V, L](constraint)
  }

}

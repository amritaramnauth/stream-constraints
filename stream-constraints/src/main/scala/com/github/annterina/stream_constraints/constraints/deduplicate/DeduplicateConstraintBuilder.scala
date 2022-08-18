package com.github.annterina.stream_constraints.constraints.deduplicate

import java.time.Duration
import java.util.function.BiFunction

class DeduplicateConstraintBuilder[K, V] {

  /**
    *  The event type to deduplicate
    *
    * @param deduplicate
    * @return
    */
  def deduplicate(deduplicate: ((K, V) => Boolean, String)): RetentionPeriodStep[K, V] = {
    new RetentionPeriodStep[K, V](deduplicate)
  }

  final class RetentionPeriodStep[K, V](deduplicate: ((K, V) => Boolean, String)) {
  
    /**
      * The retention period for maintaining duplicates in buffer store in milliseconds
      *
      * @param retentionPeriodMs
      * @return a deduplicate constraint instance
      */
     def retentionPeriodMs(retentionPeriodMs: Long): ValueComparatorStep[K, V] = {
      new ValueComparatorStep[K, V](deduplicate, retentionPeriodMs)
     }
  }

  final class ValueComparatorStep[K, V](deduplicate: ((K, V) => Boolean, String), retentionPeriodMs: Long) {
    /**
      * 
      * The comparator function to check for duplicates;
      * @param comparator function to compare new value with previous
      * @return
      */
     def valueComparator(valueComparator: BiFunction[V, V, Boolean]): DeduplicateConstraint[K, V] = {
      new DeduplicateConstraint[K, V](deduplicate, retentionPeriodMs, valueComparator)
     }
  }

}

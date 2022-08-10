package com.github.annterina.stream_constraints.constraints.deduplicate

import java.time.Duration

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
     def retentionPeriodMs(retentionPeriodMs: Long): DeduplicateConstraint[K, V] = {
      new DeduplicateConstraint[K, V](deduplicate, retentionPeriodMs)
     }
  }

}

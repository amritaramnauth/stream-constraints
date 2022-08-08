package com.github.annterina.stream_constraints.constraints.deduplicate

import java.time.Duration

class DeduplicateConstraintBuilder[K, V] {

  /**
    *  The event type to deduplicate
    *
    * @param deduplicate
    * @return
    */
  def deduplicate(deduplicate: ((K, V) => Boolean, String)): MaintainDurationStep[K, V] = {
    new MaintainDurationStep[K, V](deduplicate)
  }

  final class MaintainDurationStep[K, V](deduplicate: ((K, V) => Boolean, String)) {
  
    /**
      * The threshold for maintaining duplicates in buffer store
      *
      * @param maintainDurationMs
      * @return an deduplicate constraint instance
      */
     def maintainDurationMs(maintainDurationMs: Long): DeduplicateConstraint[K, V] = {
      new DeduplicateConstraint[K, V](deduplicate, maintainDurationMs)
     }
  }

}

package com.github.annterina.stream_constraints.constraints.deduplicate

import java.time.Duration

class DeduplicateConstraintBuilder[K, V] {

  def deduplicate(deduplicate: ((K, V) => Boolean, String)): MaintainDurationStep[K, V] = {
    new MaintainDurationStep[K, V](deduplicate)
  }

  final class MaintainDurationStep[K, V](deduplicate: ((K, V) => Boolean, String)) {
     def maintainDurationMs(maintainDurationMs: Long): DeduplicateConstraint[K, V] = {
      new DeduplicateConstraint[K, V](deduplicate, maintainDurationMs)
     }
  }

}

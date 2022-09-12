package com.github.annterina.stream_constraints.constraints.deduplicate

import java.util.function.BiFunction

/**
  * 
  * @param deduplicate the event type to deduplicate
  * @param retentionPeriodMs the retention period for maintaining duplicates in buffer store in milliseconds
  * @param valueComparator the comparator function to check for duplicates; compares new value with previous
  * 
  */
case class DeduplicateConstraint[K, V](deduplicate: ((K, V) => Boolean, String),
                                       retentionPeriodMs: Long, 
                                       valueComparator: BiFunction[V, V, Boolean] ) {}
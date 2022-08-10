package com.github.annterina.stream_constraints.constraints.deduplicate

import java.util.function.BiFunction

/**
  * 
  *
  * @param deduplicate the event type to deduplicate
  * @param retentionPeriodMs the retention period for maintaining duplicates in buffer store in milliseconds
  */
case class DeduplicateConstraint[K, V](deduplicate: ((K, V) => Boolean, String),
                                        retentionPeriodMs: Long) {}
package com.github.annterina.stream_constraints.constraints.deduplicate

import java.util.function.BiFunction

/**
  * 
  *
  * @param deduplicate the event type to deduplicate
  * @param maintainDurationMS the threshold for maintaining duplicates in buffer store
  */
case class DeduplicateConstraint[K, V](deduplicate: ((K, V) => Boolean, String),
                                        maintainDurationMS: Long) {}
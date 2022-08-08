package com.github.annterina.stream_constraints.constraints.deduplicate

import java.util.function.BiFunction

case class DeduplicateConstraint[K, V](deduplicate: ((K, V) => Boolean, String),
                                        maintainDurationMS: Long) {}
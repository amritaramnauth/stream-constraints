package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Constraint
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{PunctuationType, Punctuator, ProcessorContext}
import org.apache.kafka.streams.state.{TimestampedKeyValueStore, ValueAndTimestamp, KeyValueIterator}
import org.apache.kafka.streams.KeyValue

import java.time.Duration
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.function.BiFunction
import com.github.annterina.stream_constraints.constraints.deduplicate.DeduplicateConstraint

import org.slf4j.{Logger, LoggerFactory}


class DeduplicateTransformer[K, V, L](constraint: Constraint[K, V, L]) extends Transformer[K, V, KeyValue[Redirect[K], V]] {
  
  var context: ProcessorContext = _
  val logger: Logger = LoggerFactory.getLogger(getClass)

    /**
      *  Key: eventId
      *  Value: timestamp (event-time) when the event was seen for the first time
      */
    var deduplicateStore: TimestampedKeyValueStore[K, V] = _
    var retentionPeriodMs: Long = _
    var comparator: BiFunction[V, V, Boolean] = _
    
    val CLEAR_INTERVAL_MILLIS: Long = TimeUnit.MINUTES.toMillis(1)

    override def init(context: ProcessorContext): Unit = {
        this.context = context
        this.deduplicateStore = context.getStateStore[TimestampedKeyValueStore[K,V]]("Deduplicate")

        this.context.schedule(CLEAR_INTERVAL_MILLIS, PunctuationType.WALL_CLOCK_TIME, new Punctuator {
          override def punctuate(currentStreamTimeMs: Long): Unit = {
             try {
              var iterator: KeyValueIterator[K, ValueAndTimestamp[V]] = deduplicateStore.all()

              while(iterator.hasNext()) {
                val entry: KeyValue[K, ValueAndTimestamp[V]] = iterator.next()
                val eventTimestamp: Long = entry.value.timestamp()

                // delete from store if timestamp expired
                if((currentStreamTimeMs - eventTimestamp) > retentionPeriodMs) {
                  deduplicateStore.delete(entry.key)
                  
                }
              }
             } 
             catch {
              case e: IOException => e.printStackTrace
             }
          }
        })

    }

    override def transform(key: K, value: V): KeyValue[Redirect[K],V] = {

      // forward as-is if deduplicate constraint DOES NOT apply
      if (constraintsNotApplicable(constraint, key, value)) {
        context.forward(Redirect(key, redirect = false), value)
        return null
      }

      if (value == null) {
        null
        
      } else {

        val constraintDefinition = find(constraint, key, value)

        if (constraintDefinition.isDefined && constraintDefinition.get.retentionPeriodMs < 1) {
          throw new IllegalArgumentException("retention period must be >= 1");
          null
        }

        retentionPeriodMs = constraintDefinition.get.retentionPeriodMs
        comparator = constraintDefinition.get.valueComparator
        
        var output: KeyValue[K, V] = KeyValue.pair(key, value)
        if (isDuplicate(key, value)) {
          output = null;
          // update timestamp to prevent expiry
          deduplicateStore.put(key, ValueAndTimestamp.make(value, context.timestamp()));
          context.forward(Redirect(key, redirect = true), value)

        } else {
          output = KeyValue.pair(key, value)
          deduplicateStore.put(key, ValueAndTimestamp.make(value, context.timestamp()));
          context.forward(Redirect(key, redirect = false), value)

        }
        null
      }
      null
    }


    override def close(): Unit = {}

    private def isDuplicate(key: K, value: V): Boolean = {
      var isDuplicate: Boolean = false
      val storedValue: ValueAndTimestamp[V] = deduplicateStore.get(key)

      // handle already processed
      if(storedValue != null) {
        val previous: V = storedValue.value
        isDuplicate = (comparator.apply(previous, value) == true);
      }
      return isDuplicate
    }

    private def constraintsNotApplicable(constraint: Constraint[K, V, L], key: K, value: V): Boolean = {
      !constraint.deduplicates.exists(p => p.deduplicate._1.apply(key, value))
    }

    private def find(constraint: Constraint[K, V, L], key: K, value: V): Option[DeduplicateConstraint[K,V]] = {
      constraint.deduplicates.find(p => p.deduplicate._1.apply(key, value))
    }

}

package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Constraint
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{PunctuationType, Punctuator, ProcessorContext}
import org.apache.kafka.streams.state.{TimestampedKeyValueStore, ValueAndTimestamp, KeyValueIterator}
import org.apache.kafka.streams.KeyValue

import org.slf4j.{Logger, LoggerFactory}
import java.time.Duration
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.function.BiFunction

class DeduplicateTransformer[K, V, L](constraint: Constraint[K, V, L], valueComparator: BiFunction[V, V, Boolean]) 
extends Transformer[K, V, KeyValue[Redirect[K], V]] {
    
    var context: ProcessorContext = _

    /**
      *  Key: eventId
      *  Value: timestamp (event-time) when the event was seen for the first time
      */
    var deduplicateStore: TimestampedKeyValueStore[K, V] = _
    val CLEAR_INTERVAL_MILLIS: Long = TimeUnit.MINUTES.toMillis(1)
    
    val maintainDurationMS = CLEAR_INTERVAL_MILLIS

    var logger: Logger = LoggerFactory.getLogger(this.getClass)

    override def init(context: ProcessorContext): Unit = {
        this.context = context
        this.deduplicateStore = context.getStateStore[TimestampedKeyValueStore[K,V]]("Deduplicate")

        this.context.schedule(CLEAR_INTERVAL_MILLIS, PunctuationType.WALL_CLOCK_TIME, new Punctuator {
          override def punctuate(currentStreamTimeMs: Long): Unit = {
             logger.info("calling deduplicate punctuate")
             
             try {
              var iterator: KeyValueIterator[K, ValueAndTimestamp[V]] = deduplicateStore.all()

              while(iterator.hasNext()) {
                val entry: KeyValue[K, ValueAndTimestamp[V]] = iterator.next()
                val eventTimestamp: Long = entry.value.timestamp()

                // delete from store if timestamp expired
                if((currentStreamTimeMs - eventTimestamp) > maintainDurationMS) {
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
      if (value == null) {
        logger.info("calling transform on null value")
        // return KeyValue.pair(key, null);
        null
        
      } else {
        var output: KeyValue[K, V] = KeyValue.pair(key, value)
        if (isDuplicate(key, value)) {
          output = null;
          // update timestamp to prevent expiry
          logger.info("found duplicate")
          deduplicateStore.put(key, ValueAndTimestamp.make(value, context.timestamp()));
          context.forward(Redirect(key, redirect = true), value)

        } else {
          output = KeyValue.pair(key, value)
          logger.info("remembering new event")
          deduplicateStore.put(key, ValueAndTimestamp.make(value, context.timestamp()));
          context.forward(Redirect(key, redirect = false), value)
        }
        null
      }
    }


    override def close(): Unit = {}

    private def isDuplicate(key: K, value: V): Boolean = {
      var isDuplicate: Boolean = false
      val storedValue: ValueAndTimestamp[V] = deduplicateStore.get(key)

      // handle already processed
      if(storedValue != null) {
        val previous: V = storedValue.value
        isDuplicate = (valueComparator.apply(previous, value) == true)
      }
      return isDuplicate
    }

}

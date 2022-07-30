package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Constraint
import com.github.annterina.stream_constraints.graphs.ConstraintNode
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.state.KeyValueStore

import scalax.collection.edge.LDiEdge
import scalax.collection.mutable.Graph

import java.util.concurrent.TimeUnit

class DeduplicationConstraintTransformer[K, V, L](constraint: Constraint[K, V, L], graph: Graph[ConstraintNode, LDiEdge])
  extends Transformer[K, V, KeyValue[Redirect[K], V]] {

  var maintainDurationMs = TimeUnit.MINUTES.toMillis(5)

  var context: ProcessorContext = _
  var deduplicateStore: KeyValueStore[L, Long] = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
    this.deduplicateStore = context.getStateStore[KeyValueStore[L, Long]]("Deduplicated")

    // schedule a punctuate() method every minute to remove older records than a configurable threshold called maintainDurationMs
    this.context.schedule(TimeUnit.MINUTES.toMillis(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator {
        override def punctuate(currentStreamTimeMs: Long): Unit = {
          val iter = deduplicateStore.all();
          while (iter.hasNext) {
            val entry = iter.next
            val eventTimestamp = entry.value

            if(expired(eventTimestamp, currentStreamTimeMs)) {
              // expiring key
              deduplicateStore.delete(entry.key)
            }
          }
          iter.close()
        }
      })
  }

  override def transform(key: K, value: V): KeyValue[Redirect[K], V] = {
    val link = constraint.link.apply(key, value)

    // check store: if event is processed already then decide whether to drop or redirect
    if (Option(deduplicateStore.get(link)).isDefined) {
      context.forward(Redirect(key, redirect = true), value)
      return null

    } else {
      // otherwise forward event and add it as processed record in store
      deduplicateStore.put(link, context.timestamp())
    }

    null
  }

  override def close(): Unit = {}

  private def expired(eventTimestamp: Long, currentStreamTimeMs: Long): Boolean =
  (currentStreamTimeMs - eventTimestamp) > maintainDurationMs


}

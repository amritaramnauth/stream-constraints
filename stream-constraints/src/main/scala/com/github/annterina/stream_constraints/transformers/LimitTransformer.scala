package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Constraint
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, ValueAndTimestamp, KeyValueIterator}
import org.apache.kafka.streams.KeyValue

import com.github.annterina.stream_constraints.constraints.limit.LimitConstraint

class LimitTransformer[K, V, L](constraint: Constraint[K, V, L]) extends Transformer[K, V, KeyValue[Redirect[K], V]] {
    
    var context: ProcessorContext = _

    /**
      *  Key: eventId
      *  Value: number of times the event was processed
      */
    var limitStore: KeyValueStore[K, Int] = _

    override def init(context: ProcessorContext): Unit = {
        this.context = context
        this.limitStore = context.getStateStore[KeyValueStore[K, Int]]("Limit")

    }

    override def transform(key: K, value: V): KeyValue[Redirect[K], V] = {

      // forward as-is if limit constraint DOES NOT apply
      if (constraintsNotApplicable(constraint, key, value)) {
        context.forward(Redirect(key, redirect = false), value)
        return null
      }

      // initialize limit
      if (!Option(limitStore.get(key)).isDefined) {
        limitStore.put(key, 1)

        context.forward(Redirect(key, redirect = false), value)
        return null

      } else {
        // exists with limit
        val existingLimitConstraint = constraint.limits.find(p => p.limit._1.apply(key, value))
        val currentLimit = limitStore.get(key)

        if (currentLimit < existingLimitConstraint.get.numberToLimit) {
          limitStore.put(key, currentLimit + 1)

          context.forward(Redirect(key, redirect = false), value)

        } else {
          
          context.forward(Redirect(key, redirect = true), value)
        }
        null
      }
      null
    }


    override def close(): Unit = {}

    private def constraintsNotApplicable(constraint: Constraint[K, V, L], key: K, value: V): Boolean = {
      !constraint.limits.exists(p => p.limit._1.apply(key, value))
    }

}

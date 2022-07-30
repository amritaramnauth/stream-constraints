package com.github.annterina.stream_constraints.stores

import java.time.Duration

import com.github.annterina.stream_constraints.constraints.Constraint
import com.github.annterina.stream_constraints.graphs.ConstraintNode
import com.github.annterina.stream_constraints.serdes.{GraphSerde, TimestampedKeyValuesSerde, KeyValueSerde, KeyValuesSerde}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores, ValueAndTimestamp, WindowStore}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.mutable.Graph

case class PrerequisiteStores[K, V, L](constraint: Constraint[K, V, L]) {

  val keyValueSerde = new KeyValueSerde[K, V](constraint.keySerde, constraint.valueSerde)
  val keyValuesSerde = new KeyValuesSerde[K, V](keyValueSerde)

  def graphStore(): StoreBuilder[KeyValueStore[L, Graph[ConstraintNode, DiEdge]]] = {
    val name = "Graph"
    val graphStoreSupplier = Stores.persistentKeyValueStore(name)
    Stores.keyValueStoreBuilder(graphStoreSupplier, constraint.linkSerde, GraphSerde)
  }

  def bufferStore(name: String): StoreBuilder[KeyValueStore[L, List[ValueAndTimestamp[KeyValue[K, V]]]]] = {
    val storeSupplier = Stores.persistentKeyValueStore(name)
    Stores.keyValueStoreBuilder(storeSupplier, constraint.linkSerde, new TimestampedKeyValuesSerde[K, V](keyValueSerde))
  }

  def windowedStore(name: String, window: Duration): StoreBuilder[WindowStore[L, List[KeyValue[K, V]]]] = {
    val supplier = Stores.persistentWindowStore(name, window.multipliedBy(2), window, false)
    Stores.windowStoreBuilder(supplier, constraint.linkSerde, keyValuesSerde)
  }

  def terminatedStore(): StoreBuilder[KeyValueStore[L, Long]] = {
    val name = "Terminated"
    val storeSupplier = Stores.persistentKeyValueStore(name)
    Stores.keyValueStoreBuilder(storeSupplier, constraint.linkSerde, Serdes.longSerde)
  }

   def deduplicatedStore(): StoreBuilder[KeyValueStore[L, Long]] = {
    val name = "Deduplicated"
    val storeSupplier = Stores.persistentKeyValueStore(name)
    Stores.keyValueStoreBuilder(storeSupplier, constraint.linkSerde, Serdes.longSerde)
  }
}

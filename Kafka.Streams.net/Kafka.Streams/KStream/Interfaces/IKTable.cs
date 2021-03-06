using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.KeyValues;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * {@code KTable} is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
     * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
     * <p>
     * A {@code KTable} is either {@link StreamsBuilder#table(string) defined from a single Kafka topic} that is
     * consumed message by message or the result of a {@code KTable} transformation.
     * An aggregation of a {@link KStream} also yields a {@code KTable}.
     * <p>
     * A {@code KTable} can be transformed record by record, joined with another {@code KTable} or {@link KStream}, or
     * can be re-partitioned and aggregated into a new {@code KTable}.
     * <p>
     * Some {@code KTable}s have an internal state (a {@link IReadOnlyKeyValueStore}) and are therefore queryable via the
     * interactive queries API.
     * For example:
     * <pre>{@code
     *      KTable table = ...
     *     ...
     *      KafkaStreams streams = ...;
     *     streams.Start()
     *     ...
     *      string queryableStoreName = table.queryableStoreName(); // returns null if KTable is not queryable
     *     IReadOnlyKeyValueStore view = streams.Store(queryableStoreName, QueryableStoreTypes.KeyValueStore);
     *     view[key];
     *}</pre>
     *<p>
     * Records from the source topic that have null keys are dropped.
     *
     * @param Type of primary keys
     * @param Type of value changes
     * @see KStream
     * @see KGroupedTable
     * @see GlobalKTable
     * @see StreamsBuilder#table(string)
     */
    public interface IKTable<K, V>
    {
        public string Name { get; }
        /**
         * Create a new {@code KTable} that consists of All records of this {@code KTable} which satisfy the given
         * predicate, with default serializers, deserializers, and state store.
         * All records that do not satisfy the predicate are dropped.
         * For each {@code KTable} update, the filter is evaluated based on the current update
         * record and then an update record is produced for the result {@code KTable}.
         * This is a stateless record-by-record operation.
         * <p>
         * Note that {@code filter} for a <i>changelog stream</i> works differently than {@link KStream#filter(Predicate)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
         * directly if required (i.e., if there is anything to be deleted).
         * Furthermore, for each record that gets dropped (i.e., does not satisfy the given predicate) a tombstone record
         * is forwarded.
         *
         * @param predicate a filter {@link Predicate} that is applied to each record
         * @return a {@code KTable} that contains only those records that satisfy the given predicate
         * @see #filterNot(Predicate)
         */
        IKTable<K, V> Filter(Func<K, V, bool> predicate);

        /**
         * Create a new {@code KTable} that consists of All records of this {@code KTable} which satisfy the given
         * predicate, with default serializers, deserializers, and state store.
         * All records that do not satisfy the predicate are dropped.
         * For each {@code KTable} update, the filter is evaluated based on the current update
         * record and then an update record is produced for the result {@code KTable}.
         * This is a stateless record-by-record operation.
         * <p>
         * Note that {@code filter} for a <i>changelog stream</i> works differently than {@link KStream#filter(Predicate)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
         * directly if required (i.e., if there is anything to be deleted).
         * Furthermore, for each record that gets dropped (i.e., does not satisfy the given predicate) a tombstone record
         * is forwarded.
         *
         * @param predicate a filter {@link Predicate} that is applied to each record
         * @param named     a {@link Named} config used to Name the processor in the topology
         * @return a {@code KTable} that contains only those records that satisfy the given predicate
         * @see #filterNot(Predicate)
         */
        IKTable<K, V> Filter(Func<K, V, bool> predicate, Named named);

        /**
         * Create a new {@code KTable} that consists of All records of this {@code KTable} which satisfy the given
         * predicate, with the {@link Serde key serde}, {@link Serde value serde}, and the underlying
         * {@link KeyValueStore materialized state storage} configured in the {@link Materialized} instance.
         * All records that do not satisfy the predicate are dropped.
         * For each {@code KTable} update, the filter is evaluated based on the current update
         * record and then an update record is produced for the result {@code KTable}.
         * This is a stateless record-by-record operation.
         * <p>
         * Note that {@code filter} for a <i>changelog stream</i> works differently than {@link KStream#filter(Predicate)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
         * directly if required (i.e., if there is anything to be deleted).
         * Furthermore, for each record that gets dropped (i.e., does not satisfy the given predicate) a tombstone record
         * is forwarded.
         * <p>
         * To query the local {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = [] // filtering words
         * IReadOnlyKeyValueStore<K,V> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<K, V>KeyValueStore);
         * K key = "some-word";
         * V valueForKey = localStore[key); // key must be local (application state is shared over All running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * The store Name to query with is specified by {@link Materialized#As(string)} or {@link Materialized#As(KeyValueBytesStoreSupplier)}.
         * <p>
         *
         * @param predicate     a filter {@link Predicate} that is applied to each record
         * @param materialized  a {@link Materialized} that describes how the {@link IStateStore} for the resulting {@code KTable}
         *                      should be materialized. Cannot be {@code null}
         * @return a {@code KTable} that contains only those records that satisfy the given predicate
         * @see #filterNot(Predicate, Materialized)
         */
        IKTable<K, V> Filter(
            Func<K, V, bool> predicate,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized);

        IKTableValueGetterSupplier<K, V> ValueGetterSupplier<VR>();

        /**
         * Create a new {@code KTable} that consists of All records of this {@code KTable} which satisfy the given
         * predicate, with the {@link Serde key serde}, {@link Serde value serde}, and the underlying
         * {@link KeyValueStore materialized state storage} configured in the {@link Materialized} instance.
         * All records that do not satisfy the predicate are dropped.
         * For each {@code KTable} update, the filter is evaluated based on the current update
         * record and then an update record is produced for the result {@code KTable}.
         * This is a stateless record-by-record operation.
         * <p>
         * Note that {@code filter} for a <i>changelog stream</i> works differently than {@link KStream#filter(Predicate)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
         * directly if required (i.e., if there is anything to be deleted).
         * Furthermore, for each record that gets dropped (i.e., does not satisfy the given predicate) a tombstone record
         * is forwarded.
         * <p>
         * To query the local {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = [] // filtering words
         * IReadOnlyKeyValueStore<K,V> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<K, V>KeyValueStore);
         * K key = "some-word";
         * V valueForKey = localStore[key); // key must be local (application state is shared over All running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * The store Name to query with is specified by {@link Materialized#As(string)} or {@link Materialized#As(KeyValueBytesStoreSupplier)}.
         * <p>
         *
         * @param predicate     a filter {@link Predicate} that is applied to each record
         * @param named         a {@link Named} config used to Name the processor in the topology
         * @param materialized  a {@link Materialized} that describes how the {@link IStateStore} for the resulting {@code KTable}
         *                      should be materialized. Cannot be {@code null}
         * @return a {@code KTable} that contains only those records that satisfy the given predicate
         * @see #filterNot(Predicate, Materialized)
         */
        IKTable<K, V> Filter(
            Func<K, V, bool> predicate,
            Named named,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Create a new {@code KTable} that consists All records of this {@code KTable} which do <em>not</em> satisfy the
         * given predicate, with default serializers, deserializers, and state store.
         * All records that <em>do</em> satisfy the predicate are dropped.
         * For each {@code KTable} update, the filter is evaluated based on the current update
         * record and then an update record is produced for the result {@code KTable}.
         * This is a stateless record-by-record operation.
         * <p>
         * Note that {@code filterNot} for a <i>changelog stream</i> works differently than {@link KStream#filterNot(Predicate)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
         * directly if required (i.e., if there is anything to be deleted).
         * Furthermore, for each record that gets dropped (i.e., does satisfy the given predicate) a tombstone record is
         * forwarded.
         *
         * @param predicate a filter {@link Predicate} that is applied to each record
         * @return a {@code KTable} that contains only those records that do <em>not</em> satisfy the given predicate
         * @see #filter(Predicate)
         */
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate);

        /**
         * Create a new {@code KTable} that consists All records of this {@code KTable} which do <em>not</em> satisfy the
         * given predicate, with default serializers, deserializers, and state store.
         * All records that <em>do</em> satisfy the predicate are dropped.
         * For each {@code KTable} update, the filter is evaluated based on the current update
         * record and then an update record is produced for the result {@code KTable}.
         * This is a stateless record-by-record operation.
         * <p>
         * Note that {@code filterNot} for a <i>changelog stream</i> works differently than {@link KStream#filterNot(Predicate)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
         * directly if required (i.e., if there is anything to be deleted).
         * Furthermore, for each record that gets dropped (i.e., does satisfy the given predicate) a tombstone record is
         * forwarded.
         *
         * @param predicate a filter {@link Predicate} that is applied to each record
         * @param named     a {@link Named} config used to Name the processor in the topology
         * @return a {@code KTable} that contains only those records that do <em>not</em> satisfy the given predicate
         * @see #filter(Predicate)
         */
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate, Named named);

        /**
         * Create a new {@code KTable} that consists All records of this {@code KTable} which do <em>not</em> satisfy the
         * given predicate, with the {@link Serde key serde}, {@link Serde value serde}, and the underlying
         * {@link KeyValueStore materialized state storage} configured in the {@link Materialized} instance.
         * All records that <em>do</em> satisfy the predicate are dropped.
         * For each {@code KTable} update, the filter is evaluated based on the current update
         * record and then an update record is produced for the result {@code KTable}.
         * This is a stateless record-by-record operation.
         * <p>
         * Note that {@code filterNot} for a <i>changelog stream</i> works differently than {@link KStream#filterNot(Predicate)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
         * directly if required (i.e., if there is anything to be deleted).
         * Furthermore, for each record that gets dropped (i.e., does satisfy the given predicate) a tombstone record is
         * forwarded.
         * <p>
         * To query the local {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = [] // filtering words
         * IReadOnlyKeyValueStore<K,V> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<K, V>KeyValueStore);
         * K key = "some-word";
         * V valueForKey = localStore[key); // key must be local (application state is shared over All running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * The store Name to query with is specified by {@link Materialized#As(string)} or {@link Materialized#As(KeyValueBytesStoreSupplier)}.
         * <p>
         * @param predicate a filter {@link Predicate} that is applied to each record
         * @param materialized  a {@link Materialized} that describes how the {@link IStateStore} for the resulting {@code KTable}
         *                      should be materialized. Cannot be {@code null}
         * @return a {@code KTable} that contains only those records that do <em>not</em> satisfy the given predicate
         * @see #filter(Predicate, Materialized)
         */
        IKTable<K, V> FilterNot(
            Func<K, V, bool> predicate,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Create a new {@code KTable} that consists All records of this {@code KTable} which do <em>not</em> satisfy the
         * given predicate, with the {@link Serde key serde}, {@link Serde value serde}, and the underlying
         * {@link KeyValueStore materialized state storage} configured in the {@link Materialized} instance.
         * All records that <em>do</em> satisfy the predicate are dropped.
         * For each {@code KTable} update, the filter is evaluated based on the current update
         * record and then an update record is produced for the result {@code KTable}.
         * This is a stateless record-by-record operation.
         * <p>
         * Note that {@code filterNot} for a <i>changelog stream</i> works differently than {@link KStream#filterNot(Predicate)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
         * directly if required (i.e., if there is anything to be deleted).
         * Furthermore, for each record that gets dropped (i.e., does satisfy the given predicate) a tombstone record is
         * forwarded.
         * <p>
         * To query the local {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = [] // filtering words
         * IReadOnlyKeyValueStore<K,V> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<K, V>KeyValueStore);
         * K key = "some-word";
         * V valueForKey = localStore[key); // key must be local (application state is shared over All running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * The store Name to query with is specified by {@link Materialized#As(string)} or {@link Materialized#As(KeyValueBytesStoreSupplier)}.
         * <p>
         * @param predicate a filter {@link Predicate} that is applied to each record
         * @param named     a {@link Named} config used to Name the processor in the topology
         * @param materialized  a {@link Materialized} that describes how the {@link IStateStore} for the resulting {@code KTable}
         *                      should be materialized. Cannot be {@code null}
         * @return a {@code KTable} that contains only those records that do <em>not</em> satisfy the given predicate
         * @see #filter(Predicate, Materialized)
         */
        IKTable<K, V> FilterNot(
            Func<K, V, bool> predicate,
            Named named,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type) in the new {@code KTable}, with default serializers, deserializers, and state store.
         * For each {@code KTable} update the provided {@link ValueMapper} is applied to the value of the updated record and
         * computes a new value for it, resulting in an updated record for the result {@code KTable}.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is a stateless record-by-record operation.
         * <p>
         * The example below counts the number of token of the value string.
         * <pre>{@code
         * KTable<string, string> inputTable = builder.table("topic");
         * KTable<string, int> outputTable = inputTable.MapValues(value => value.Split(" ").Length);
         * }</pre>
         * <p>
         * This operation preserves data co-location with respect to the key.
         * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
         * the result {@code KTable}.
         * <p>
         * Note that {@code mapValues} for a <i>changelog stream</i> works differently than {@link KStream#mapValues(ValueMapper)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
         * delete the corresponding record in the result {@code KTable}.
         *
         * @param mapper a {@link ValueMapper} that computes a new output value
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
         */
        IKTable<K, VR> MapValues<VR>(ValueMapper<V, VR> mapper);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type) in the new {@code KTable}, with default serializers, deserializers, and state store.
         * For each {@code KTable} update the provided {@link ValueMapper} is applied to the value of the updated record and
         * computes a new value for it, resulting in an updated record for the result {@code KTable}.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is a stateless record-by-record operation.
         * <p>
         * The example below counts the number of token of the value string.
         * <pre>{@code
         * KTable<string, string> inputTable = builder.table("topic");
         * KTable<string, int> outputTable = inputTable.MapValues(value => value.Split(" ").Length, Named.As("countTokenValue"));
         * }</pre>
         * <p>
         * This operation preserves data co-location with respect to the key.
         * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
         * the result {@code KTable}.
         * <p>
         * Note that {@code mapValues} for a <i>changelog stream</i> works differently than {@link KStream#mapValues(ValueMapper)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
         * delete the corresponding record in the result {@code KTable}.
         *
         * @param mapper a {@link ValueMapper} that computes a new output value
         * @param named  a {@link Named} config used to Name the processor in the topology
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
         */
        IKTable<K, VR> MapValues<VR>(ValueMapper<V, VR> mapper, Named named);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type) in the new {@code KTable}, with default serializers, deserializers, and state store.
         * For each {@code KTable} update the provided {@link ValueMapperWithKey} is applied to the value of the update
         * record and computes a new value for it, resulting in an updated record for the result {@code KTable}.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is a stateless record-by-record operation.
         * <p>
         * The example below counts the number of token of value and key strings.
         * <pre>{@code
         * KTable<string, string> inputTable = builder.table("topic");
         * KTable<string, int> outputTable =
         *  inputTable.MapValues((readOnlyKey, value) => readOnlyKey.Split(" ").Length + value.Split(" ").Length);
         * }</pre>
         * <p>
         * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
         * This operation preserves data co-location with respect to the key.
         * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
         * the result {@code KTable}.
         * <p>
         * Note that {@code mapValues} for a <i>changelog stream</i> works differently than {@link KStream#mapValues(ValueMapperWithKey)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
         * delete the corresponding record in the result {@code KTable}.
         *
         * @param mapper a {@link ValueMapperWithKey} that computes a new output value
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
         */
        IKTable<K, VR> MapValues<VR>(ValueMapperWithKey<K, V, VR> mapper);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type) in the new {@code KTable}, with default serializers, deserializers, and state store.
         * For each {@code KTable} update the provided {@link ValueMapperWithKey} is applied to the value of the update
         * record and computes a new value for it, resulting in an updated record for the result {@code KTable}.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is a stateless record-by-record operation.
         * <p>
         * The example below counts the number of token of value and key strings.
         * <pre>{@code
         * KTable<string, string> inputTable = builder.table("topic");
         * KTable<string, int> outputTable =
         *  inputTable.MapValues((readOnlyKey, value) => readOnlyKey.Split(" ").Length + value.Split(" ").Length, Named.As("countTokenValueAndKey"));
         * }</pre>
         * <p>
         * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
         * This operation preserves data co-location with respect to the key.
         * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
         * the result {@code KTable}.
         * <p>
         * Note that {@code mapValues} for a <i>changelog stream</i> works differently than {@link KStream#mapValues(ValueMapperWithKey)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
         * delete the corresponding record in the result {@code KTable}.
         *
         * @param mapper a {@link ValueMapperWithKey} that computes a new output value
         * @param named  a {@link Named} config used to Name the processor in the topology
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
         */
        IKTable<K, VR> MapValues<VR>(ValueMapperWithKey<K, V, VR> mapper, Named named);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type) in the new {@code KTable}, with the {@link Serde key serde}, {@link Serde value serde},
         * and the underlying {@link KeyValueStore materialized state storage} configured in the {@link Materialized}
         * instance.
         * For each {@code KTable} update the provided {@link ValueMapper} is applied to the value of the updated record and
         * computes a new value for it, resulting in an updated record for the result {@code KTable}.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is a stateless record-by-record operation.
         * <p>
         * The example below counts the number of token of the value string.
         * <pre>{@code
         * KTable<string, string> inputTable = builder.table("topic");
         * KTable<string, int> outputTable = inputTable.mapValue(new ValueMapper<string, int> {
         *     int apply(string value)
{
         *         return value.Split(" ").Length;
         *     }
         * });
         * }</pre>
         * <p>
         * To query the local {@link KeyValueStore} representing outputTable above it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * The store Name to query with is specified by {@link Materialized#As(string)} or {@link Materialized#As(KeyValueBytesStoreSupplier)}.
         * <p>
         * This operation preserves data co-location with respect to the key.
         * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
         * the result {@code KTable}.
         * <p>
         * Note that {@code mapValues} for a <i>changelog stream</i> works differently than {@link KStream#mapValues(ValueMapper)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
         * delete the corresponding record in the result {@code KTable}.
         *
         * @param mapper a {@link ValueMapper} that computes a new output value
         * @param materialized  a {@link Materialized} that describes how the {@link IStateStore} for the resulting {@code KTable}
         *                      should be materialized. Cannot be {@code null}
         * @param   the value type of the result {@code KTable}
         *
         * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
         */
        IKTable<K, VR> MapValues<VR>(
            ValueMapper<V, VR> mapper,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        void EnableSendingOldValues();

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type) in the new {@code KTable}, with the {@link Serde key serde}, {@link Serde value serde},
         * and the underlying {@link KeyValueStore materialized state storage} configured in the {@link Materialized}
         * instance.
         * For each {@code KTable} update the provided {@link ValueMapper} is applied to the value of the updated record and
         * computes a new value for it, resulting in an updated record for the result {@code KTable}.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is a stateless record-by-record operation.
         * <p>
         * The example below counts the number of token of the value string.
         * <pre>{@code
         * KTable<string, string> inputTable = builder.table("topic");
         * KTable<string, int> outputTable = inputTable.mapValue(new ValueMapper<string, int> {
         *     int apply(string value)
{
         *         return value.Split(" ").Length;
         *     }
         * });
         * }</pre>
         * <p>
         * To query the local {@link KeyValueStore} representing outputTable above it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * The store Name to query with is specified by {@link Materialized#As(string)} or {@link Materialized#As(KeyValueBytesStoreSupplier)}.
         * <p>
         * This operation preserves data co-location with respect to the key.
         * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
         * the result {@code KTable}.
         * <p>
         * Note that {@code mapValues} for a <i>changelog stream</i> works differently than {@link KStream#mapValues(ValueMapper)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
         * delete the corresponding record in the result {@code KTable}.
         *
         * @param mapper a {@link ValueMapper} that computes a new output value
         * @param named  a {@link Named} config used to Name the processor in the topology
         * @param materialized  a {@link Materialized} that describes how the {@link IStateStore} for the resulting {@code KTable}
         *                      should be materialized. Cannot be {@code null}
         * @param   the value type of the result {@code KTable}
         *
         * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
         */
        IKTable<K, VR> MapValues<VR>(
            ValueMapper<V, VR> mapper,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type) in the new {@code KTable}, with the {@link Serde key serde}, {@link Serde value serde},
         * and the underlying {@link KeyValueStore materialized state storage} configured in the {@link Materialized}
         * instance.
         * For each {@code KTable} update the provided {@link ValueMapperWithKey} is applied to the value of the update
         * record and computes a new value for it, resulting in an updated record for the result {@code KTable}.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is a stateless record-by-record operation.
         * <p>
         * The example below counts the number of token of value and key strings.
         * <pre>{@code
         * KTable<string, string> inputTable = builder.table("topic");
         * KTable<string, int> outputTable = inputTable.mapValue(new ValueMapperWithKey<string, string, int> {
         *     int apply(string readOnlyKey, string value)
{
         *          return readOnlyKey.Split(" ").Length + value.Split(" ").Length;
         *     }
         * });
         * }</pre>
         * <p>
         * To query the local {@link KeyValueStore} representing outputTable above it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * The store Name to query with is specified by {@link Materialized#As(string)} or {@link Materialized#As(KeyValueBytesStoreSupplier)}.
         * <p>
         * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
         * This operation preserves data co-location with respect to the key.
         * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
         * the result {@code KTable}.
         * <p>
         * Note that {@code mapValues} for a <i>changelog stream</i> works differently than {@link KStream#mapValues(ValueMapper)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
         * delete the corresponding record in the result {@code KTable}.
         *
         * @param mapper a {@link ValueMapperWithKey} that computes a new output value
         * @param materialized  a {@link Materialized} that describes how the {@link IStateStore} for the resulting {@code KTable}
         *                      should be materialized. Cannot be {@code null}
         * @param   the value type of the result {@code KTable}
         *
         * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
         */
        IKTable<K, VR> MapValues<VR>(
            ValueMapperWithKey<K, V, VR> mapper,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type) in the new {@code KTable}, with the {@link Serde key serde}, {@link Serde value serde},
         * and the underlying {@link KeyValueStore materialized state storage} configured in the {@link Materialized}
         * instance.
         * For each {@code KTable} update the provided {@link ValueMapperWithKey} is applied to the value of the update
         * record and computes a new value for it, resulting in an updated record for the result {@code KTable}.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is a stateless record-by-record operation.
         * <p>
         * The example below counts the number of token of value and key strings.
         * <pre>{@code
         * KTable<string, string> inputTable = builder.table("topic");
         * KTable<string, int> outputTable = inputTable.mapValue(new ValueMapperWithKey<string, string, int> {
         *     int apply(string readOnlyKey, string value)
{
         *          return readOnlyKey.Split(" ").Length + value.Split(" ").Length;
         *     }
         * });
         * }</pre>
         * <p>
         * To query the local {@link KeyValueStore} representing outputTable above it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * The store Name to query with is specified by {@link Materialized#As(string)} or {@link Materialized#As(KeyValueBytesStoreSupplier)}.
         * <p>
         * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
         * This operation preserves data co-location with respect to the key.
         * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
         * the result {@code KTable}.
         * <p>
         * Note that {@code mapValues} for a <i>changelog stream</i> works differently than {@link KStream#mapValues(ValueMapper)
         * record stream filters}, because {@link KeyValuePair records} with {@code null} values (so-called tombstone records)
         * have delete semantics.
         * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
         * delete the corresponding record in the result {@code KTable}.
         *
         * @param mapper a {@link ValueMapperWithKey} that computes a new output value
         * @param named  a {@link Named} config used to Name the processor in the topology
         * @param materialized  a {@link Materialized} that describes how the {@link IStateStore} for the resulting {@code KTable}
         *                      should be materialized. Cannot be {@code null}
         * @param   the value type of the result {@code KTable}
         *
         * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
         */
        IKTable<K, VR> MapValues<VR>(
            ValueMapperWithKey<K, V, VR> mapper,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Convert this changelog stream to a {@link KStream}.
         * <p>
         * Note that this is a logical operation and only changes the "interpretation" of the stream, i.e., each record of
         * this changelog stream is no longer treated as an updated record (cf. {@link KStream} vs {@code KTable}).
         *
         * @return a {@link KStream} that contains the same records as this {@code KTable}
         */
        IKStream<K, V> ToStream();

        /**
         * Convert this changelog stream to a {@link KStream}.
         * <p>
         * Note that this is a logical operation and only changes the "interpretation" of the stream, i.e., each record of
         * this changelog stream is no longer treated as an updated record (cf. {@link KStream} vs {@code KTable}).
         *
         * @param named  a {@link Named} config used to Name the processor in the topology
         *
         * @return a {@link KStream} that contains the same records as this {@code KTable}
         */
        IKStream<K, V> ToStream(Named named);

        /**
         * Convert this changelog stream to a {@link KStream} using the given {@link KeyValueMapper} to select the new key.
         * <p>
         * For example, you can compute the new key as the.Length of the value string.
         * <pre>{@code
         * KTable<string, string> table = builder.table("topic");
         * KTable<int, string> keyedStream = table.toStream(new KeyValueMapper<string, string, int> {
         *     int apply(string key, string value)
{
         *         return value.Length;
         *     }
         * });
         * }</pre>
         * Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
         * join) is applied to the result {@link KStream}.
         * <p>
         * This operation is equivalent to calling
         * {@code table.}{@link #toStream() toStream}{@code ().}{@link KStream#selectKey(KeyValueMapper) selectKey(KeyValueMapper)}.
         * <p>
         * Note that {@link #toStream()} is a logical operation and only changes the "interpretation" of the stream, i.e.,
         * each record of this changelog stream is no longer treated as an updated record (cf. {@link KStream} vs {@code KTable}).
         *
         * @param mapper a {@link KeyValueMapper} that computes a new key for each record
         * @param the new key type of the result stream
         * @return a {@link KStream} that contains the same records as this {@code KTable}
         */
        IKStream<KR, V> ToStream<KR>(KeyValueMapper<K, V, KR> mapper);

        /**
         * Convert this changelog stream to a {@link KStream} using the given {@link KeyValueMapper} to select the new key.
         * <p>
         * For example, you can compute the new key as the.Length of the value string.
         * <pre>{@code
         * KTable<string, string> table = builder.table("topic");
         * KTable<int, string> keyedStream = table.toStream(new KeyValueMapper<string, string, int> {
         *     int apply(string key, string value)
{
         *         return value.Length;
         *     }
         * });
         * }</pre>
         * Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
         * join) is applied to the result {@link KStream}.
         * <p>
         * This operation is equivalent to calling
         * {@code table.}{@link #toStream() toStream}{@code ().}{@link KStream#selectKey(KeyValueMapper) selectKey(KeyValueMapper)}.
         * <p>
         * Note that {@link #toStream()} is a logical operation and only changes the "interpretation" of the stream, i.e.,
         * each record of this changelog stream is no longer treated as an updated record (cf. {@link KStream} vs {@code KTable}).
         *
         * @param mapper a {@link KeyValueMapper} that computes a new key for each record
         * @param named  a {@link Named} config used to Name the processor in the topology
         * @param the new key type of the result stream
         * @return a {@link KStream} that contains the same records as this {@code KTable}
         */
        IKStream<KR, V> ToStream<KR>(KeyValueMapper<K, V, KR> mapper,
                                      Named named);

        /**
         * Suppress some updates from this changelog stream, determined by the supplied {@link Suppressed} configuration.
         *
         * This controls what updates downstream table and stream operations will receive.
         *
         * @param suppressed Configuration object determining what, if any, updates to suppress
         * @return A new KTable with the desired suppression characteristics.
         */
        IKTable<K, V> Suppress(ISuppressed<K> suppressed);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type), with default serializers, deserializers, and state store.
         * A {@link ValueTransformerWithKey} (provided by the given {@link ValueTransformerWithKeySupplier}) is applied to each input
         * record value and computes a new value for it.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is similar to {@link #mapValues(ValueMapperWithKey)}, but more flexible, allowing access to.Additional state-stores,
         * and access to the {@link IProcessorContext}.
         * Furthermore, via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long)} the processing progress can be observed and.Additional
         * periodic actions can be performed.
         * <p>
         * If the downstream topology uses aggregation functions, (e.g. {@link KGroupedTable#reduce}, {@link KGroupedTable#aggregate}, etc),
         * care must be taken when dealing with state, (either held in state-stores or transformer instances), to ensure correct aggregate results.
         * In contrast, if the resulting KTable is materialized, (cf. {@link #transformValues(ValueTransformerWithKeySupplier, Materialized, string...)}),
         * such concerns are handled for you.
         * <p>
         * In order to assign a state, the state must be created and registered beforehand:
         * <pre>{@code
         * // create store
         * StoreBuilder<KeyValueStore<string,string>> keyValueStoreBuilder =
         *         Stores.keyValueStoreBuilder(Stores.PersistentKeyValueStore("myValueTransformState"),
         *                 Serdes.Serdes.String(),
         *                 Serdes.Serdes.String());
         * // register store
         * builder.AddStateStore(keyValueStoreBuilder);
         *
         * KTable outputTable = inputTable.transformValues(new ValueTransformerWithKeySupplier() { [] }, "myValueTransformState");
         * }</pre>
         * <p>
         * Within the {@link ValueTransformerWithKey}, the state is obtained via the
         * {@link IProcessorContext}.
         * To trigger periodic actions via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long) punctuate()},
         * a schedule must be registered.
         * <pre>{@code
         * new ValueTransformerWithKeySupplier()
{
         *     ValueTransformerWithKey get()
{
         *         return new ValueTransformerWithKey()
{
         *             private KeyValueStore<string, string> state;
         *
         *             void Init(IProcessorContext context)
{
         *                 this.state = (KeyValueStore<string, string>)context.getStateStore("myValueTransformState");
         *                 context.schedule(TimeSpan.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator(..)); // punctuate each 1000ms, can access this.state
         *             }
         *
         *             NewValueType transform(K readOnlyKey, V value)
{
         *                 // can access this.state and use read-only key
         *                 return new NewValueType(readOnlyKey); // or null
         *             }
         *
         *             void Close()
{
         *                 // can access this.state
         *             }
         *         }
         *     }
         * }
         * }</pre>
         * <p>
         * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
         * Setting a new value preserves data co-location with respect to the key.
         *
         * @param transformerSupplier a instance of {@link ValueTransformerWithKeySupplier} that generates a
         *                            {@link ValueTransformerWithKey}.
         *                            At least one transformer instance will be created per streaming task.
         *                            Transformers do not need to be thread-safe.
         * @param stateStoreNames     the names of the state stores used by the processor
         * @param                the value type of the result table
         * @return a {@code KTable} that contains records with unmodified key and new values (possibly of different type)
         * @see #mapValues(ValueMapper)
         * @see #mapValues(ValueMapperWithKey)
         */
        IKTable<K, VR> TransformValues<VR>(
            IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
            params string[] stateStoreNames);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type), with default serializers, deserializers, and state store.
         * A {@link ValueTransformerWithKey} (provided by the given {@link ValueTransformerWithKeySupplier}) is applied to each input
         * record value and computes a new value for it.
         * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
         * This is similar to {@link #mapValues(ValueMapperWithKey)}, but more flexible, allowing access to.Additional state-stores,
         * and access to the {@link IProcessorContext}.
         * Furthermore, via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long)} the processing progress can be observed and.Additional
         * periodic actions can be performed.
         * <p>
         * If the downstream topology uses aggregation functions, (e.g. {@link KGroupedTable#reduce}, {@link KGroupedTable#aggregate}, etc),
         * care must be taken when dealing with state, (either held in state-stores or transformer instances), to ensure correct aggregate results.
         * In contrast, if the resulting KTable is materialized, (cf. {@link #transformValues(ValueTransformerWithKeySupplier, Materialized, string...)}),
         * such concerns are handled for you.
         * <p>
         * In order to assign a state, the state must be created and registered beforehand:
         * <pre>{@code
         * // create store
         * StoreBuilder<KeyValueStore<string,string>> keyValueStoreBuilder =
         *         Stores.keyValueStoreBuilder(Stores.PersistentKeyValueStore("myValueTransformState"),
         *                 Serdes.Serdes.String(),
         *                 Serdes.Serdes.String());
         * // register store
         * builder.AddStateStore(keyValueStoreBuilder);
         *
         * KTable outputTable = inputTable.transformValues(new ValueTransformerWithKeySupplier() { [] }, "myValueTransformState");
         * }</pre>
         * <p>
         * Within the {@link ValueTransformerWithKey}, the state is obtained via the
         * {@link IProcessorContext}.
         * To trigger periodic actions via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long) punctuate()},
         * a schedule must be registered.
         * <pre>{@code
         * new ValueTransformerWithKeySupplier()
{
         *     ValueTransformerWithKey get()
{
         *         return new ValueTransformerWithKey()
{
         *             private KeyValueStore<string, string> state;
         *
         *             void Init(IProcessorContext context)
{
         *                 this.state = (KeyValueStore<string, string>)context.getStateStore("myValueTransformState");
         *                 context.schedule(TimeSpan.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator(..)); // punctuate each 1000ms, can access this.state
         *             }
         *
         *             NewValueType transform(K readOnlyKey, V value)
{
         *                 // can access this.state and use read-only key
         *                 return new NewValueType(readOnlyKey); // or null
         *             }
         *
         *             void Close()
{
         *                 // can access this.state
         *             }
         *         }
         *     }
         * }
         * }</pre>
         * <p>
         * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
         * Setting a new value preserves data co-location with respect to the key.
         *
         * @param transformerSupplier a instance of {@link ValueTransformerWithKeySupplier} that generates a
         *                            {@link ValueTransformerWithKey}.
         *                            At least one transformer instance will be created per streaming task.
         *                            Transformers do not need to be thread-safe.
         * @param named               a {@link Named} config used to Name the processor in the topology
         * @param stateStoreNames     the names of the state stores used by the processor
         * @param                the value type of the result table
         * @return a {@code KTable} that contains records with unmodified key and new values (possibly of different type)
         * @see #mapValues(ValueMapper)
         * @see #mapValues(ValueMapperWithKey)
         */
        IKTable<K, VR> TransformValues<VR>(IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                            Named named,
                                            string[] stateStoreNames);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type), with the {@link Serde key serde}, {@link Serde value serde}, and the underlying
         * {@link KeyValueStore materialized state storage} configured in the {@link Materialized} instance.
         * A {@link ValueTransformerWithKey} (provided by the given {@link ValueTransformerWithKeySupplier}) is applied to each input
         * record value and computes a new value for it.
         * This is similar to {@link #mapValues(ValueMapperWithKey)}, but more flexible, allowing stateful, rather than stateless,
         * record-by-record operation, access to.Additional state-stores, and access to the {@link IProcessorContext}.
         * Furthermore, via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long)} the processing progress can be observed and.Additional
         * periodic actions can be performed.
         * The resulting {@code KTable} is materialized into another state store .Additional to the provided state store names)
         * as specified by the user via {@link Materialized} parameter, and is queryable through its given Name.
         * <p>
         * In order to assign a state, the state must be created and registered beforehand:
         * <pre>{@code
         * // create store
         * StoreBuilder<KeyValueStore<string,string>> keyValueStoreBuilder =
         *         Stores.keyValueStoreBuilder(Stores.PersistentKeyValueStore("myValueTransformState"),
         *                 Serdes.Serdes.String(),
         *                 Serdes.Serdes.String());
         * // register store
         * builder.AddStateStore(keyValueStoreBuilder);
         *
         * KTable outputTable = inputTable.transformValues(
         *     new ValueTransformerWithKeySupplier() { [] },
         *     Materialized.<string, string, KeyValueStore<Bytes, byte[]>>As("outputTable")
         *                                 .withKeySerde(Serdes.Serdes.String())
         *                                 .WithValueSerde(Serdes.Serdes.String()),
         *     "myValueTransformState");
         * }</pre>
         * <p>
         * Within the {@link ValueTransformerWithKey}, the state is obtained via the
         * {@link IProcessorContext}.
         * To trigger periodic actions via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long) punctuate()},
         * a schedule must be registered.
         * <pre>{@code
         * new ValueTransformerWithKeySupplier()
{
         *     ValueTransformerWithKey get()
{
         *         return new ValueTransformerWithKey()
{
         *             private KeyValueStore<string, string> state;
         *
         *             void Init(IProcessorContext context)
{
         *                 this.state = (KeyValueStore<string, string>)context.getStateStore("myValueTransformState");
         *                 context.schedule(TimeSpan.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator(..)); // punctuate each 1000ms, can access this.state
         *             }
         *
         *             NewValueType transform(K readOnlyKey, V value)
{
         *                 // can access this.state and use read-only key
         *                 return new NewValueType(readOnlyKey); // or null
         *             }
         *
         *             void Close()
{
         *                 // can access this.state
         *             }
         *         }
         *     }
         * }
         * }</pre>
         * <p>
         * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
         * Setting a new value preserves data co-location with respect to the key.
         *
         * @param transformerSupplier a instance of {@link ValueTransformerWithKeySupplier} that generates a
         *                            {@link ValueTransformerWithKey}.
         *                            At least one transformer instance will be created per streaming task.
         *                            Transformers do not need to be thread-safe.
         * @param materialized        an instance of {@link Materialized} used to describe how the state store of the
         *                            resulting table should be materialized.
         *                            Cannot be {@code null}
         * @param stateStoreNames     the names of the state stores used by the processor
         * @param                the value type of the result table
         * @return a {@code KTable} that contains records with unmodified key and new values (possibly of different type)
         * @see #mapValues(ValueMapper)
         * @see #mapValues(ValueMapperWithKey)
         */
        IKTable<K, VR> TransformValues<VR>(IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
                                            string[] stateStoreNames);

        /**
         * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
         * (with possibly a new type), with the {@link Serde key serde}, {@link Serde value serde}, and the underlying
         * {@link KeyValueStore materialized state storage} configured in the {@link Materialized} instance.
         * A {@link ValueTransformerWithKey} (provided by the given {@link ValueTransformerWithKeySupplier}) is applied to each input
         * record value and computes a new value for it.
         * This is similar to {@link #mapValues(ValueMapperWithKey)}, but more flexible, allowing stateful, rather than stateless,
         * record-by-record operation, access to.Additional state-stores, and access to the {@link IProcessorContext}.
         * Furthermore, via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long)} the processing progress can be observed and.Additional
         * periodic actions can be performed.
         * The resulting {@code KTable} is materialized into another state store .Additional to the provided state store names)
         * as specified by the user via {@link Materialized} parameter, and is queryable through its given Name.
         * <p>
         * In order to assign a state, the state must be created and registered beforehand:
         * <pre>{@code
         * // create store
         * StoreBuilder<KeyValueStore<string,string>> keyValueStoreBuilder =
         *         Stores.keyValueStoreBuilder(Stores.PersistentKeyValueStore("myValueTransformState"),
         *                 Serdes.Serdes.String(),
         *                 Serdes.Serdes.String());
         * // register store
         * builder.AddStateStore(keyValueStoreBuilder);
         *
         * KTable outputTable = inputTable.transformValues(
         *     new ValueTransformerWithKeySupplier() { [] },
         *     Materialized.<string, string, KeyValueStore<Bytes, byte[]>>As("outputTable")
         *                                 .withKeySerde(Serdes.Serdes.String())
         *                                 .WithValueSerde(Serdes.Serdes.String()),
         *     "myValueTransformState");
         * }</pre>
         * <p>
         * Within the {@link ValueTransformerWithKey}, the state is obtained via the
         * {@link IProcessorContext}.
         * To trigger periodic actions via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long) punctuate()},
         * a schedule must be registered.
         * <pre>{@code
         * new ValueTransformerWithKeySupplier()
{
         *     ValueTransformerWithKey get()
{
         *         return new ValueTransformerWithKey()
{
         *             private KeyValueStore<string, string> state;
         *
         *             void Init(IProcessorContext context)
{
         *                 this.state = (KeyValueStore<string, string>)context.getStateStore("myValueTransformState");
         *                 context.schedule(TimeSpan.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator(..)); // punctuate each 1000ms, can access this.state
         *             }
         *
         *             NewValueType transform(K readOnlyKey, V value)
{
         *                 // can access this.state and use read-only key
         *                 return new NewValueType(readOnlyKey); // or null
         *             }
         *
         *             void Close()
{
         *                 // can access this.state
         *             }
         *         }
         *     }
         * }
         * }</pre>
         * <p>
         * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
         * Setting a new value preserves data co-location with respect to the key.
         *
         * @param transformerSupplier a instance of {@link ValueTransformerWithKeySupplier} that generates a
         *                            {@link ValueTransformerWithKey}.
         *                            At least one transformer instance will be created per streaming task.
         *                            Transformers do not need to be thread-safe.
         * @param materialized        an instance of {@link Materialized} used to describe how the state store of the
         *                            resulting table should be materialized.
         *                            Cannot be {@code null}
         * @param named               a {@link Named} config used to Name the processor in the topology
         * @param stateStoreNames     the names of the state stores used by the processor
         * @param                the value type of the result table
         * @return a {@code KTable} that contains records with unmodified key and new values (possibly of different type)
         * @see #mapValues(ValueMapper)
         * @see #mapValues(ValueMapperWithKey)
         */
        IKTable<K, VR> TransformValues<VR>(IValueTransformerWithKeySupplier<K, V, VR> transformerSupplier,
                                            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
                                            Named named,
                                            string[] stateStoreNames);

        /**
         * Re-groups the records of this {@code KTable} using the provided {@link KeyValueMapper} and default serializers
         * and deserializers.
         * Each {@link KeyValuePair} pair of this {@code KTable} is mapped to a new {@link KeyValuePair} pair by applying the
         * provided {@link KeyValueMapper}.
         * Re-grouping a {@code KTable} is required before an aggregation operator can be applied to the data
         * (cf. {@link KGroupedTable}).
         * The {@link KeyValueMapper} selects a new key and value (with should both have unmodified type).
         * If the new record key is {@code null} the record will not be included in the resulting {@link KGroupedTable}
         * <p>
         * Because a new key is selected, an internal repartitioning topic will be created in Kafka.
         * This topic will be named "${applicationId}-&lt;Name&gt;-repartition", where "applicationId" is user-specified in
         * {@link  StreamsConfig} via parameter {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "&lt;Name&gt;" is
         * an internally generated Name, and "-repartition" is a fixed suffix.
         *
         * You can retrieve All generated internal topic names via {@link Topology#describe()}.
         *
         * <p>
         * All data of this {@code KTable} will be redistributed through the repartitioning topic by writing All update
         * records to and rereading All updated records from it, such that the resulting {@link KGroupedTable} is partitioned
         * on the new key.
         * <p>
         * If the key or value type is changed, it is recommended to use {@link #groupBy(KeyValueMapper, Grouped)}
         * instead.
         *
         * @param selector a {@link KeyValueMapper} that computes a new grouping key and value to be aggregated
         * @param     the key type of the result {@link KGroupedTable}
         * @param     the value type of the result {@link KGroupedTable}
         * @return a {@link KGroupedTable} that contains the re-grouped records of the original {@code KTable}
         */
        IKGroupedTable<KR, VR> GroupBy<KR, VR>(KeyValueMapper<K, V, KeyValuePair<KR, VR>> selector);

        /**
         * Re-groups the records of this {@code KTable} using the provided {@link KeyValueMapper}
         * and {@link Serde}s as specified by {@link Serialized}.
         * Each {@link KeyValuePair} pair of this {@code KTable} is mapped to a new {@link KeyValuePair} pair by applying the
         * provided {@link KeyValueMapper}.
         * Re-grouping a {@code KTable} is required before an aggregation operator can be applied to the data
         * (cf. {@link KGroupedTable}).
         * The {@link KeyValueMapper} selects a new key and value (with both maybe being the same type or a new type).
         * If the new record key is {@code null} the record will not be included in the resulting {@link KGroupedTable}
         * <p>
         * Because a new key is selected, an internal repartitioning topic will be created in Kafka.
         * This topic will be named "${applicationId}-&lt;Name&gt;-repartition", where "applicationId" is user-specified in
         * {@link  StreamsConfig} via parameter {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "&lt;Name&gt;" is
         * an internally generated Name, and "-repartition" is a fixed suffix.
         *
         * You can retrieve All generated internal topic names via {@link Topology#describe()}.
         *
         * <p>
         * All data of this {@code KTable} will be redistributed through the repartitioning topic by writing All update
         * records to and rereading All updated records from it, such that the resulting {@link KGroupedTable} is partitioned
         * on the new key.
         *
         * @param selector      a {@link KeyValueMapper} that computes a new grouping key and value to be aggregated
         * @param serialized    the {@link Serialized} instance used to specify {@link org.apache.kafka.common.serialization.Serdes}
         * @param          the key type of the result {@link KGroupedTable}
         * @param          the value type of the result {@link KGroupedTable}
         * @return a {@link KGroupedTable} that contains the re-grouped records of the original {@code KTable}
         *
         * @deprecated since 2.1. Use {@link org.apache.kafka.streams.kstream.KTable#groupBy(KeyValueMapper, Grouped)} instead
         */
        [Obsolete]
        IKGroupedTable<KR, VR> GroupBy<KR, VR>(
            KeyValueMapper<K, V, KeyValuePair<KR, VR>> selector,
            ISerialized<KR, VR> serialized);

        /**
         * Re-groups the records of this {@code KTable} using the provided {@link KeyValueMapper}
         * and {@link Serde}s as specified by {@link Grouped}.
         * Each {@link KeyValuePair} pair of this {@code KTable} is mapped to a new {@link KeyValuePair} pair by applying the
         * provided {@link KeyValueMapper}.
         * Re-grouping a {@code KTable} is required before an aggregation operator can be applied to the data
         * (cf. {@link KGroupedTable}).
         * The {@link KeyValueMapper} selects a new key and value (where both could the same type or a new type).
         * If the new record key is {@code null} the record will not be included in the resulting {@link KGroupedTable}
         * <p>
         * Because a new key is selected, an internal repartitioning topic will be created in Kafka.
         * This topic will be named "${applicationId}-&lt;Name&gt;-repartition", where "applicationId" is user-specified in
         * {@link  StreamsConfig} via parameter {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig},  "&lt;Name&gt;" is
         * either provided via {@link org.apache.kafka.streams.kstream.Grouped#As(string)} or an internally generated Name.
         *
         * <p>
         * You can retrieve All generated internal topic names via {@link Topology#describe()}.
         *
         * <p>
         * All data of this {@code KTable} will be redistributed through the repartitioning topic by writing All update
         * records to and rereading All updated records from it, such that the resulting {@link KGroupedTable} is partitioned
         * on the new key.
         *
         * @param selector      a {@link KeyValueMapper} that computes a new grouping key and value to be aggregated
         * @param grouped       the {@link Grouped} instance used to specify {@link org.apache.kafka.common.serialization.Serdes}
         *                      and the Name for a repartition topic if repartitioning is required.
         * @param          the key type of the result {@link KGroupedTable}
         * @param          the value type of the result {@link KGroupedTable}
         * @return a {@link KGroupedTable} that contains the re-grouped records of the original {@code KTable}
         */
        IKGroupedTable<KR, VR> GroupBy<KR, VR>(KeyValueMapper<K, V, KeyValuePair<KR, VR>> selector,
                                                Grouped<KR, VR> grouped);

        /**
         * Join records of this {@code KTable} with another {@code KTable}'s records using non-windowed inner equi join,
         * with default serializers, deserializers, and state store.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable} the provided
         * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded
         * directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td></td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:C&gt;</td>
         * <td>&lt;K1:C&gt;</td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:C&gt;</td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other  the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param   the value type of the other {@code KTable}
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key
         * @see #leftJoin(KTable, ValueJoiner)
         * @see #outerJoin(KTable, ValueJoiner)
         */
        IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner);

        /**
         * Join records of this {@code KTable} with another {@code KTable}'s records using non-windowed inner equi join,
         * with default serializers, deserializers, and state store.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable} the provided
         * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded
         * directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td></td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:C&gt;</td>
         * <td>&lt;K1:C&gt;</td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:C&gt;</td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other  the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param named  a {@link Named} config used to Name the processor in the topology
         * @param   the value type of the other {@code KTable}
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key
         * @see #leftJoin(KTable, ValueJoiner)
         * @see #outerJoin(KTable, ValueJoiner)
         */
        IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named);

        /**
         * Join records of this {@code KTable} with another {@code KTable}'s records using non-windowed inner equi join,
         * with the {@link Materialized} instance for configuration of the {@link Serde key serde},
         * {@link Serde the result table's value serde}, and {@link KeyValueStore state store}.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable} the provided
         * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded
         * directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td></td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:C&gt;</td>
         * <td>&lt;K1:C&gt;</td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:C&gt;</td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other         the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner        a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param materialized  an instance of {@link Materialized} used to describe how the state store should be materialized.
         *                      Cannot be {@code null}
         * @param          the value type of the other {@code KTable}
         * @param          the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key
         * @see #leftJoin(KTable, ValueJoiner, Materialized)
         * @see #outerJoin(KTable, ValueJoiner, Materialized)
         */
        IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Join records of this {@code KTable} with another {@code KTable}'s records using non-windowed inner equi join,
         * with the {@link Materialized} instance for configuration of the {@link Serde key serde},
         * {@link Serde the result table's value serde}, and {@link KeyValueStore state store}.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable} the provided
         * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded
         * directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td></td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:C&gt;</td>
         * <td>&lt;K1:C&gt;</td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:C&gt;</td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other         the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner        a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param named         a {@link Named} config used to Name the processor in the topology
         * @param materialized  an instance of {@link Materialized} used to describe how the state store should be materialized.
         *                      Cannot be {@code null}
         * @param          the value type of the other {@code KTable}
         * @param          the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key
         * @see #leftJoin(KTable, ValueJoiner, Materialized)
         * @see #outerJoin(KTable, ValueJoiner, Materialized)
         */
        IKTable<K, VR> Join<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
         * non-windowed left equi join, with default serializers, deserializers, and state store.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, All records from left {@code KTable} will produce
         * an output record (cf. below).
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
         * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * Additionally, for each record of left {@code KTable} that does not find a corresponding record in the
         * right {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code rightValue =
         * null} to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * For example, for left input tombstones the provided value-joiner is not called but a tombstone record is
         * forwarded directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be
         * deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other  the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param   the value type of the other {@code KTable}
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
         * left {@code KTable}
         * @see #join(KTable, ValueJoiner)
         * @see #outerJoin(KTable, ValueJoiner)
         */
        IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner);

        /**
         * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
         * non-windowed left equi join, with default serializers, deserializers, and state store.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, All records from left {@code KTable} will produce
         * an output record (cf. below).
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
         * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * Additionally, for each record of left {@code KTable} that does not find a corresponding record in the
         * right {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code rightValue =
         * null} to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * For example, for left input tombstones the provided value-joiner is not called but a tombstone record is
         * forwarded directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be
         * deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other  the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param named  a {@link Named} config used to Name the processor in the topology
         * @param   the value type of the other {@code KTable}
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
         * left {@code KTable}
         * @see #join(KTable, ValueJoiner)
         * @see #outerJoin(KTable, ValueJoiner)
         */
        IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named);

        /**
         * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
         * non-windowed left equi join, with the {@link Materialized} instance for configuration of the {@link Serde key serde},
         * {@link Serde the result table's value serde}, and {@link KeyValueStore state store}.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, All records from left {@code KTable} will produce
         * an output record (cf. below).
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
         * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * Additionally, for each record of left {@code KTable} that does not find a corresponding record in the
         * right {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code rightValue =
         * null} to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * For example, for left input tombstones the provided value-joiner is not called but a tombstone record is
         * forwarded directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be
         * deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other         the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner        a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param materialized  an instance of {@link Materialized} used to describe how the state store should be materialized.
         *                      Cannot be {@code null}
         * @param          the value type of the other {@code KTable}
         * @param          the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
         * left {@code KTable}
         * @see #join(KTable, ValueJoiner, Materialized)
         * @see #outerJoin(KTable, ValueJoiner, Materialized)
         */
        IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
         * non-windowed left equi join, with the {@link Materialized} instance for configuration of the {@link Serde key serde},
         * {@link Serde the result table's value serde}, and {@link KeyValueStore state store}.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, All records from left {@code KTable} will produce
         * an output record (cf. below).
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
         * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * Additionally, for each record of left {@code KTable} that does not find a corresponding record in the
         * right {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code rightValue =
         * null} to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * For example, for left input tombstones the provided value-joiner is not called but a tombstone record is
         * forwarded directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be
         * deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other         the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner        a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param named         a {@link Named} config used to Name the processor in the topology
         * @param materialized  an instance of {@link Materialized} used to describe how the state store should be materialized.
         *                      Cannot be {@code null}
         * @param          the value type of the other {@code KTable}
         * @param          the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
         * left {@code KTable}
         * @see #join(KTable, ValueJoiner, Materialized)
         * @see #outerJoin(KTable, ValueJoiner, Materialized)
         */
        IKTable<K, VR> LeftJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
         * non-windowed outer equi join, with default serializers, deserializers, and state store.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * In contrast to {@link #join(KTable, ValueJoiner) inner-join} or {@link #leftJoin(KTable, ValueJoiner) left-join},
         * All records from both input {@code KTable}s will produce an output record (cf. below).
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
         * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * Additionally, for each record that does not find a corresponding record in the corresponding other
         * {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code null} value for the
         * corresponding other value to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded directly
         * to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(null,b)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other  the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param   the value type of the other {@code KTable}
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
         * both {@code KTable}s
         * @see #join(KTable, ValueJoiner)
         * @see #leftJoin(KTable, ValueJoiner)
         */
        IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner);

        /**
         * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
         * non-windowed outer equi join, with default serializers, deserializers, and state store.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * In contrast to {@link #join(KTable, ValueJoiner) inner-join} or {@link #leftJoin(KTable, ValueJoiner) left-join},
         * All records from both input {@code KTable}s will produce an output record (cf. below).
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
         * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * Additionally, for each record that does not find a corresponding record in the corresponding other
         * {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code null} value for the
         * corresponding other value to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded directly
         * to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(null,b)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other  the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param named  a {@link Named} config used to Name the processor in the topology
         * @param   the value type of the other {@code KTable}
         * @param   the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
         * both {@code KTable}s
         * @see #join(KTable, ValueJoiner)
         * @see #leftJoin(KTable, ValueJoiner)
         */
        IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named);

        /**
         * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
         * non-windowed outer equi join, with the {@link Materialized} instance for configuration of the {@link Serde key serde},
         * {@link Serde the result table's value serde}, and {@link KeyValueStore state store}.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * In contrast to {@link #join(KTable, ValueJoiner) inner-join} or {@link #leftJoin(KTable, ValueJoiner) left-join},
         * All records from both input {@code KTable}s will produce an output record (cf. below).
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
         * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * Additionally, for each record that does not find a corresponding record in the corresponding other
         * {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code null} value for the
         * corresponding other value to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded directly
         * to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(null,b)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other         the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner        a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param materialized  an instance of {@link Materialized} used to describe how the state store should be materialized.
         *                      Cannot be {@code null}
         * @param          the value type of the other {@code KTable}
         * @param          the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
         * both {@code KTable}s
         * @see #join(KTable, ValueJoiner)
         * @see #leftJoin(KTable, ValueJoiner)
         */
        IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);


        /**
         * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
         * non-windowed outer equi join, with the {@link Materialized} instance for configuration of the {@link Serde key serde},
         * {@link Serde the result table's value serde}, and {@link KeyValueStore state store}.
         * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
         * In contrast to {@link #join(KTable, ValueJoiner) inner-join} or {@link #leftJoin(KTable, ValueJoiner) left-join},
         * All records from both input {@code KTable}s will produce an output record (cf. below).
         * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
         * of the join.
         * <p>
         * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
         * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
         * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
         * {@code KTable} the result gets updated.
         * <p>
         * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
         * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
         * Additionally, for each record that does not find a corresponding record in the corresponding other
         * {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code null} value for the
         * corresponding other value to compute a value (with arbitrary type) for the result record.
         * The key of the result record is the same as for both joining input records.
         * <p>
         * Note that {@link KeyValuePair records} with {@code null} values (so-called tombstone records) have delete semantics.
         * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded directly
         * to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
         * <p>
         * Input records with {@code null} key will be dropped and no join computation is performed.
         * <p>
         * Example:
         * <table border='1'>
         * <tr>
         * <th>thisKTable</th>
         * <th>thisState</th>
         * <th>otherKTable</th>
         * <th>otherState</th>
         * <th>result updated record</th>
         * </tr>
         * <tr>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:A&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td>&lt;K1:A&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
         * </tr>
         * <tr>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:b&gt;</td>
         * <td>&lt;K1:ValueJoiner(null,b)&gt;</td>
         * </tr>
         * <tr>
         * <td></td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * <td></td>
         * <td>&lt;K1:null&gt;</td>
         * </tr>
         * </table>
         * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
         * partitions.
         *
         * @param other         the other {@code KTable} to be joined with this {@code KTable}
         * @param joiner        a {@link ValueJoiner} that computes the join result for a pair of matching records
         * @param named         a {@link Named} config used to Name the processor in the topology
         * @param materialized  an instance of {@link Materialized} used to describe how the state store should be materialized.
         *                      Cannot be {@code null}
         * @param          the value type of the other {@code KTable}
         * @param          the value type of the result {@code KTable}
         * @return a {@code KTable} that contains join-records for each key and values computed by the given
         * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
         * both {@code KTable}s
         * @see #join(KTable, ValueJoiner)
         * @see #leftJoin(KTable, ValueJoiner)
         */
        IKTable<K, VR> OuterJoin<VO, VR>(
            IKTable<K, VO> other,
            ValueJoiner<V, VO, VR> joiner,
            Named named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Get the Name of the local state store used that can be used to query this {@code KTable}.
         *
         * @return the underlying state store Name, or {@code null} if this {@code KTable} cannot be queried.
         */
        string? QueryableStoreName { get; }
    }
}

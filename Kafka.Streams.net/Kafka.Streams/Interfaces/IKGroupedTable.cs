﻿using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.Interfaces
{
    /**
    * {@code KGroupedTable} is an abstraction of a <i>re-grouped changelog stream</i> from a primary-keyed table,
    * usually on a different grouping key than the original primary key.
    * <p>
    * It is an intermediate representation after a re-grouping of a {@link KTable} before an aggregation is applied to the
    * new partitions resulting in a new {@link KTable}.
    * <p>
    * A {@code KGroupedTable} must be obtained from a {@link KTable} via {@link KTable#groupBy(KeyValueMapper)
    * groupBy(...)}.
    *
    * @param <K> Type of keys
    * @param <V> Type of values
    * @see KTable
    */
    public interface IKGroupedTable<K, V>
    {
        /**
         * Count number of records of the original {@link KTable} that got {@link KTable#groupBy(KeyValueMapper) mapped} to
         * the same key into a new instance of {@link KTable}.
         * Records with {@code null} key are ignored.
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * that can be queried using the provided {@code queryableStoreName}.
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         * <p>
         * To query the local {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = ... // counting words
         * ReadOnlyKeyValueStore<String, long> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<String, long>keyValueStore());
         * String key = "some-word";
         * long countForWord = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         *
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
         * alphanumerics, '.', '_' and '-'.
         * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "storeName" is the
         * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param materialized the instance of {@link Materialized} used to materialize the state store. Cannot be {@code null}
         * @return a {@link KTable} that contains "update" records with unmodified keys and {@link long} values that
         * represent the latest (rolling) count (i.e., number of records) for each key
         */
        IKTable<K, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Count number of records of the original {@link KTable} that got {@link KTable#groupBy(KeyValueMapper) mapped} to
         * the same key into a new instance of {@link KTable}.
         * Records with {@code null} key are ignored.
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "internalStoreName" is an internal name
         * and "-changelog" is a fixed suffix.
         * Note that the internal store name may not be queriable through Interactive Queries.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @return a {@link KTable} that contains "update" records with unmodified keys and {@link long} values that
         * represent the latest (rolling) count (i.e., number of records) for each key
         */
        IKTable<K, long> Count();

        /**
         * Combine the value of records of the original {@link KTable} that got {@link KTable#groupBy(KeyValueMapper)
         * mapped} to the same key into a new instance of {@link KTable}.
         * Records with {@code null} key are ignored.
         * Combining implies that the type of the aggregate result is the same as the type of the input value
         * (c.f. {@link #aggregate(Initializer, Aggregator, Aggregator, Materialized)}).
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * that can be queried using the provided {@code queryableStoreName}.
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * Each update to the original {@link KTable} results in a two step update of the result {@link KTable}.
         * The specified {@link Reducer adder} is applied for each update record and computes a new aggregate using the
         * current aggregate (first argument) and the record's value (second argument) by adding the new record to the
         * aggregate.
         * The specified {@link Reducer subtractor} is applied for each "replaced" record of the original {@link KTable}
         * and computes a new aggregate using the current aggregate (first argument) and the record's value (second
         * argument) by "removing" the "replaced" record from the aggregate.
         * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
         * value as-is.
         * Thus, {@code reduce(Reducer, Reducer, String)} can be used to compute aggregate functions like sum.
         * For sum, the adder and subtractor would work as follows:
         * <pre>{@code
         * public class SumAdder implements Reducer<Integer> {
         *   public Integer apply(Integer currentAgg, Integer newValue) {
         *     return currentAgg + newValue;
         *   }
         * }
         *
         * public class SumSubtractor implements Reducer<Integer> {
         *   public Integer apply(Integer currentAgg, Integer oldValue) {
         *     return currentAgg - oldValue;
         *   }
         * }
         * }</pre>
         * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         * <p>
         * To query the local {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = ... // counting words
         * ReadOnlyKeyValueStore<String, long> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<String, long>keyValueStore());
         * String key = "some-word";
         * long countForWord = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
         * alphanumerics, '.', '_' and '-'.
         * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "storeName" is the
         * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param adder         a {@link Reducer} that adds a new value to the aggregate result
         * @param subtractor    a {@link Reducer} that removed an old value from the aggregate result
         * @param materialized  the instance of {@link Materialized} used to materialize the state store. Cannot be {@code null}
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key
         */
        IKTable<K, V> Reduce(
            Reducer<V> adder,
            Reducer<V> subtractor,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Combine the value of records of the original {@link KTable} that got {@link KTable#groupBy(KeyValueMapper)
         * mapped} to the same key into a new instance of {@link KTable}.
         * Records with {@code null} key are ignored.
         * Combining implies that the type of the aggregate result is the same as the type of the input value
         * (c.f. {@link #aggregate(Initializer, Aggregator, Aggregator)}).
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * Each update to the original {@link KTable} results in a two step update of the result {@link KTable}.
         * The specified {@link Reducer adder} is applied for each update record and computes a new aggregate using the
         * current aggregate and the record's value by adding the new record to the aggregate.
         * The specified {@link Reducer subtractor} is applied for each "replaced" record of the original {@link KTable}
         * and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
         * record from the aggregate.
         * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
         * value as-is.
         * Thus, {@code reduce(Reducer, Reducer)} can be used to compute aggregate functions like sum.
         * For sum, the adder and subtractor would work as follows:
         * <pre>{@code
         * public class SumAdder implements Reducer<Integer> {
         *   public Integer apply(Integer currentAgg, Integer newValue) {
         *     return currentAgg + newValue;
         *   }
         * }
         *
         * public class SumSubtractor implements Reducer<Integer> {
         *   public Integer apply(Integer currentAgg, Integer oldValue) {
         *     return currentAgg - oldValue;
         *   }
         * }
         * }</pre>
         * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "internalStoreName" is an internal name
         * and "-changelog" is a fixed suffix.
         * Note that the internal store name may not be queriable through Interactive Queries.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param adder      a {@link Reducer} that adds a new value to the aggregate result
         * @param subtractor a {@link Reducer} that removed an old value from the aggregate result
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key
         */
        IKTable<K, V> Reduce(Reducer<V> adder, Reducer<V> subtractor);

        /**
         * Aggregate the value of records of the original {@link KTable} that got {@link KTable#groupBy(KeyValueMapper)
         * mapped} to the same key into a new instance of {@link KTable}.
         * Records with {@code null} key are ignored.
         * Aggregating is a generalization of {@link #reduce(Reducer, Reducer, Materialized) combining via reduce(...)} as it,
         * for example, allows the result to have a different type than the input values.
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * that can be queried using the provided {@code queryableStoreName}.
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * The specified {@link Initializer} is applied once directly before the first input record is processed to
         * provide an initial intermediate aggregation result that is used to process the first record.
         * Each update to the original {@link KTable} results in a two step update of the result {@link KTable}.
         * The specified {@link Aggregator adder} is applied for each update record and computes a new aggregate using the
         * current aggregate (or for the very first record using the intermediate aggregation result provided via the
         * {@link Initializer}) and the record's value by adding the new record to the aggregate.
         * The specified {@link Aggregator subtractor} is applied for each "replaced" record of the original {@link KTable}
         * and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
         * record from the aggregate.
         * Thus, {@code aggregate(Initializer, Aggregator, Aggregator, Materialized)} can be used to compute aggregate functions
         * like sum.
         * For sum, the initializer, adder, and subtractor would work as follows:
         * <pre>{@code
         * // in this example, LongSerde.class must be set as value serde in Materialized#withValueSerde
         * public class SumInitializer implements Initializer<long> {
         *   public long apply() {
         *     return 0L;
         *   }
         * }
         *
         * public class SumAdder implements Aggregator<String, Integer, long> {
         *   public long apply(String key, Integer newValue, long aggregate) {
         *     return aggregate + newValue;
         *   }
         * }
         *
         * public class SumSubtractor implements Aggregator<String, Integer, long> {
         *   public long apply(String key, Integer oldValue, long aggregate) {
         *     return aggregate - oldValue;
         *   }
         * }
         * }</pre>
         * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         * <p>
         * To query the local {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = ... // counting words
         * ReadOnlyKeyValueStore<String, long> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<String, long>keyValueStore());
         * String key = "some-word";
         * long countForWord = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
         * alphanumerics, '.', '_' and '-'.
         * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "storeName" is the
         * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param initializer   an {@link Initializer} that provides an initial aggregate result value
         * @param adder         an {@link Aggregator} that adds a new record to the aggregate result
         * @param subtractor    an {@link Aggregator} that removed an old record from the aggregate result
         * @param materialized  the instance of {@link Materialized} used to materialize the state store. Cannot be {@code null}
         * @param <VR>          the value type of the aggregated {@link KTable}
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key
         */
        IKTable<K, VR> Aggregate<VR>(
            Initializer<VR> initializer,
            Aggregator<K, V, VR> adder,
            Aggregator<K, V, VR> subtractor,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Aggregate the value of records of the original {@link KTable} that got {@link KTable#groupBy(KeyValueMapper)
         * mapped} to the same key into a new instance of {@link KTable} using default serializers and deserializers.
         * Records with {@code null} key are ignored.
         * Aggregating is a generalization of {@link #reduce(Reducer, Reducer) combining via reduce(...)} as it,
         * for example, allows the result to have a different type than the input values.
         * If the result value type does not match the {@link StreamsConfig#DefaultValueSerdeClassConfig default value
         * serde} you should use {@link #aggregate(Initializer, Aggregator, Aggregator, Materialized)}.
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * The specified {@link Initializer} is applied once directly before the first input record is processed to
         * provide an initial intermediate aggregation result that is used to process the first record.
         * Each update to the original {@link KTable} results in a two step update of the result {@link KTable}.
         * The specified {@link Aggregator adder} is applied for each update record and computes a new aggregate using the
         * current aggregate (or for the very first record using the intermediate aggregation result provided via the
         * {@link Initializer}) and the record's value by adding the new record to the aggregate.
         * The specified {@link Aggregator subtractor} is applied for each "replaced" record of the original {@link KTable}
         * and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
         * record from the aggregate.
         * Thus, {@code aggregate(Initializer, Aggregator, Aggregator, String)} can be used to compute aggregate functions
         * like sum.
         * For sum, the initializer, adder, and subtractor would work as follows:
         * <pre>{@code
         * // in this example, LongSerde.class must be set as default value serde in StreamsConfig
         * public class SumInitializer implements Initializer<long> {
         *   public long apply() {
         *     return 0L;
         *   }
         * }
         *
         * public class SumAdder implements Aggregator<String, Integer, long> {
         *   public long apply(String key, Integer newValue, long aggregate) {
         *     return aggregate + newValue;
         *   }
         * }
         *
         * public class SumSubtractor implements Aggregator<String, Integer, long> {
         *   public long apply(String key, Integer oldValue, long aggregate) {
         *     return aggregate - oldValue;
         *   }
         * }
         * }</pre>
         * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "internalStoreName" is an internal name
         * and "-changelog" is a fixed suffix.
         * Note that the internal store name may not be queriable through Interactive Queries.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param initializer a {@link Initializer} that provides an initial aggregate result value
         * @param adder       a {@link Aggregator} that adds a new record to the aggregate result
         * @param subtractor  a {@link Aggregator} that removed an old record from the aggregate result
         * @param <VR>        the value type of the aggregated {@link KTable}
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key
         */
        IKTable<K, VR> Aggregate<VR>(
            Initializer<VR> initializer,
            Aggregator<K, V, VR> adder,
            Aggregator<K, V, VR> subtractor);
    }
}

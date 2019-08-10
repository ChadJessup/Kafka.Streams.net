/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Kafka.Common.Utils;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.KStream
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
     * @param Type of keys
     * @param Type of values
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
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = [] // counting words
         * ReadOnlyKeyValueStore<string, long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<string, long>keyValueStore());
         * string key = "some-word";
         * long countForWord = localStore[key); // key must be local (application state is shared over all running Kafka Streams instances)
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
         * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
         * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param materialized the instance of {@link Materialized} used to materialize the state store. Cannot be {@code null}
         * @return a {@link KTable} that contains "update" records with unmodified keys and {@link long} values that
         * represent the latest (rolling) count (i.e., number of records) for each key
         */
        IKTable<K, long> count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized);

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
         * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
         * and "-changelog" is a fixed suffix.
         * Note that the internal store name may not be queriable through Interactive Queries.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @return a {@link KTable} that contains "update" records with unmodified keys and {@link long} values that
         * represent the latest (rolling) count (i.e., number of records) for each key
         */
        IKTable<K, long> count();

        /**
         * Combine the value of records of the original {@link KTable} that got {@link KTable#groupBy(KeyValueMapper)
         * mapped} to the same key into a new instance of {@link KTable}.
         * Records with {@code null} key are ignored.
         * Combining implies that the type of the aggregate result is the same as the type of the input value
         * (c.f. {@link #aggregate(Initializer, IAggregator, IAggregator, Materialized)}).
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * that can be queried using the provided {@code queryableStoreName}.
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * Each update to the original {@link KTable} results in a two step update of the result {@link KTable}.
         * The specified {@link Reducer.Adder} is applied for each update record and computes a new aggregate using the
         * current aggregate (first argument) and the record's value (second argument) by.Adding the new record to the
         * aggregate.
         * The specified {@link Reducer subtractor} is applied for each "replaced" record of the original {@link KTable}
         * and computes a new aggregate using the current aggregate (first argument) and the record's value (second
         * argument) by "removing" the "replaced" record from the aggregate.
         * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
         * value as-is.
         * Thus, {@code reduce(Reducer, Reducer, string)} can be used to compute aggregate functions like sum.
         * For sum, the.Adder and subtractor would work as follows:
         * <pre>{@code
         * public SumAdder : Reducer<int> {
         *   public int apply(int currentAgg, int newValue)
{
         *     return currentAgg + newValue;
         *   }
         * }
         *
         * public SumSubtractor : Reducer<int> {
         *   public int apply(int currentAgg, int oldValue)
{
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
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = [] // counting words
         * ReadOnlyKeyValueStore<string, long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<string, long>keyValueStore());
         * string key = "some-word";
         * long countForWord = localStore[key); // key must be local (application state is shared over all running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
         * alphanumerics, '.', '_' and '-'.
         * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
         * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param.Adder         a {@link Reducer} that.Adds a new value to the aggregate result
         * @param subtractor    a {@link Reducer} that removed an old value from the aggregate result
         * @param materialized  the instance of {@link Materialized} used to materialize the state store. Cannot be {@code null}
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key
         */
        IKTable<K, V> reduce(
            IReducer<V> adder,
            IReducer<V> subtractor,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized);
        /**
         * Combine the value of records of the original {@link KTable} that got {@link KTable#groupBy(KeyValueMapper)
         * mapped} to the same key into a new instance of {@link KTable}.
         * Records with {@code null} key are ignored.
         * Combining implies that the type of the aggregate result is the same as the type of the input value
         * (c.f. {@link #aggregate(Initializer, IAggregator, IAggregator)}).
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * Each update to the original {@link KTable} results in a two step update of the result {@link KTable}.
         * The specified {@link Reducer.Adder} is applied for each update record and computes a new aggregate using the
         * current aggregate and the record's value by.Adding the new record to the aggregate.
         * The specified {@link Reducer subtractor} is applied for each "replaced" record of the original {@link KTable}
         * and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
         * record from the aggregate.
         * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
         * value as-is.
         * Thus, {@code reduce(Reducer, Reducer)} can be used to compute aggregate functions like sum.
         * For sum, the.Adder and subtractor would work as follows:
         * <pre>{@code
         * public SumAdder : Reducer<int> {
         *   public int apply(int currentAgg, int newValue)
{
         *     return currentAgg + newValue;
         *   }
         * }
         *
         * public SumSubtractor : Reducer<int> {
         *   public int apply(int currentAgg, int oldValue)
{
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
         * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
         * and "-changelog" is a fixed suffix.
         * Note that the internal store name may not be queriable through Interactive Queries.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param.Adder      a {@link Reducer} that.Adds a new value to the aggregate result
         * @param subtractor a {@link Reducer} that removed an old value from the aggregate result
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key
         */
        IKTable<K, V> reduce(
            IReducer<V> adder,
            IReducer<V> subtractor);

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
         * The specified {@link IAggregator.Adder} is applied for each update record and computes a new aggregate using the
         * current aggregate (or for the very first record using the intermediate aggregation result provided via the
         * {@link Initializer}) and the record's value by.Adding the new record to the aggregate.
         * The specified {@link IAggregator subtractor} is applied for each "replaced" record of the original {@link KTable}
         * and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
         * record from the aggregate.
         * Thus, {@code aggregate(Initializer, IAggregator, IAggregator, Materialized)} can be used to compute aggregate functions
         * like sum.
         * For sum, the initializer,.Adder, and subtractor would work as follows:
         * <pre>{@code
         * // in this example, LongSerde must be set as value serde in Materialized#withValueSerde
         * public SumInitializer : Initializer<long> {
         *   public long apply()
{
         *     return 0L;
         *   }
         * }
         *
         * public SumAdder : IAggregator<string, int, long> {
         *   public long apply(string key, int newValue, long aggregate)
{
         *     return aggregate + newValue;
         *   }
         * }
         *
         * public SumSubtractor : IAggregator<string, int, long> {
         *   public long apply(string key, int oldValue, long aggregate)
{
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
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}:
         * <pre>{@code
         * KafkaStreams streams = [] // counting words
         * ReadOnlyKeyValueStore<string, long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<string, long>keyValueStore());
         * string key = "some-word";
         * long countForWord = localStore[key); // key must be local (application state is shared over all running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
         * alphanumerics, '.', '_' and '-'.
         * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
         * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param initializer   an {@link Initializer} that provides an initial aggregate result value
         * @param.Adder         an {@link IAggregator} that.Adds a new record to the aggregate result
         * @param subtractor    an {@link IAggregator} that removed an old record from the aggregate result
         * @param materialized  the instance of {@link Materialized} used to materialize the state store. Cannot be {@code null}
         * @param          the value type of the aggregated {@link KTable}
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key
         */
        IKTable<K, VR> aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> adder,
            IAggregator<K, V, VR> subtractor,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Aggregate the value of records of the original {@link KTable} that got {@link KTable#groupBy(KeyValueMapper)
         * mapped} to the same key into a new instance of {@link KTable} using default serializers and deserializers.
         * Records with {@code null} key are ignored.
         * Aggregating is a generalization of {@link #reduce(Reducer, Reducer) combining via reduce(...)} as it,
         * for example, allows the result to have a different type than the input values.
         * If the result value type does not match the {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value
         * serde} you should use {@link #aggregate(Initializer, IAggregator, IAggregator, Materialized)}.
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * The specified {@link Initializer} is applied once directly before the first input record is processed to
         * provide an initial intermediate aggregation result that is used to process the first record.
         * Each update to the original {@link KTable} results in a two step update of the result {@link KTable}.
         * The specified {@link IAggregator.Adder} is applied for each update record and computes a new aggregate using the
         * current aggregate (or for the very first record using the intermediate aggregation result provided via the
         * {@link Initializer}) and the record's value by.Adding the new record to the aggregate.
         * The specified {@link IAggregator subtractor} is applied for each "replaced" record of the original {@link KTable}
         * and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
         * record from the aggregate.
         * Thus, {@code aggregate(Initializer, IAggregator, IAggregator, string)} can be used to compute aggregate functions
         * like sum.
         * For sum, the initializer,.Adder, and subtractor would work as follows:
         * <pre>{@code
         * // in this example, LongSerde must be set as default value serde in StreamsConfig
         * public SumInitializer : Initializer<long> {
         *   public long apply()
{
         *     return 0L;
         *   }
         * }
         *
         * public SumAdder : IAggregator<string, int, long> {
         *   public long apply(string key, int newValue, long aggregate)
{
         *     return aggregate + newValue;
         *   }
         * }
         *
         * public SumSubtractor : IAggregator<string, int, long> {
         *   public long apply(string key, int oldValue, long aggregate)
{
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
         * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
         * and "-changelog" is a fixed suffix.
         * Note that the internal store name may not be queriable through Interactive Queries.
         *
         * You can retrieve all generated internal topic names via {@link Topology#describe()}.
         *
         * @param initializer a {@link Initializer} that provides an initial aggregate result value
         * @param.Adder       a {@link IAggregator} that.Adds a new record to the aggregate result
         * @param subtractor  a {@link IAggregator} that removed an old record from the aggregate result
         * @param        the value type of the aggregated {@link KTable}
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key
         */
        IKTable<K, VR> aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> adder,
            IAggregator<K, V, VR> subtractor);

    }
}
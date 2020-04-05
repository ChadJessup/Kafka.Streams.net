using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * {@code KGroupedStream} is an abstraction of a <i>grouped</i> record stream of {@link KeyValuePair} pairs.
     * It is an intermediate representation of a {@link KStream} in order to apply an aggregation operation on the original
     * {@link KStream} records.
     * <p>
     * It is an intermediate representation after a grouping of a {@link KStream} before an aggregation is applied to the
     * new partitions resulting in a {@link KTable}.
     * <p>
     * A {@code KGroupedStream} must be obtained from a {@link KStream} via {@link KStream#groupByKey() groupByKey()} or
     * {@link KStream#groupBy(KeyValueMapper) groupBy(...)}.
     *
     * @param Type of keys
     * @param Type of values
     * @see KStream
     */
    public interface IKGroupedStream<K, V>
    {
        /**
         * Count the number of records in this stream by the grouped key.
         * Records with {@code null} key or value are ignored.
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view).
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
        IKTable<K, long> Count();

        /**
         * Count the number of records in this stream by the grouped key.
         * Records with {@code null} key or value are ignored.
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * provided by the given store name in {@code materialized}.
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
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}.
         * <pre>{@code
         * KafkaStreams streams = [] // counting words
         * string queryableStoreName = "storeName"; // the store name should be the name of the store as defined by the Materialized instance
         * IReadOnlyKeyValueStore<string,long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<string, long>KeyValueStore());
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
         * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
         *                      Note: the valueSerde will be automatically set to {@link org.apache.kafka.common.serialization.Serdes#long() Serdes#long()}
         *                      if there is no valueSerde provided
         * @return a {@link KTable} that contains "update" records with unmodified keys and {@link long} values that
         * represent the latest (rolling) count (i.e., number of records) for each key
         */
        IKTable<K, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized);

        /**
         * Combine the values of records in this stream by the grouped key.
         * Records with {@code null} key or value are ignored.
         * Combining implies that the type of the aggregate result is the same as the type of the input value
         * (c.f. {@link #aggregate(Initializer, IAggregator)}).
         * <p>
         * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
         * aggregate and the record's value.
         * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
         * value as-is.
         * Thus, {@code reduce(Reducer)} can be used to compute aggregate functions like sum, min, or max.
         * <p>
         * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         *
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
         * @param reducer   a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key. If the reduce function returns {@code null}, it is then interpreted as
         * deletion for the key, and future messages of the same key coming from upstream operators
         * will be handled as newly initialized value.
         */
        IKTable<K, V> Reduce(IReducer<V> reducer);

        /**
         * Combine the value of records in this stream by the grouped key.
         * Records with {@code null} key or value are ignored.
         * Combining implies that the type of the aggregate result is the same as the type of the input value
         * (c.f. {@link #aggregate(Initializer, IAggregator, Materialized)}).
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * provided by the given store name in {@code materialized}.
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
         * aggregate (first argument) and the record's value (second argument):
         * <pre>{@code
         * // At the example of a Reducer<long>
         * new Reducer<long>()
{
         *   public long apply(long aggValue, long currValue)
{
         *     return aggValue + currValue;
         *   }
         * }
         * }</pre>
         * <p>
         * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
         * value as-is.
         * Thus, {@code reduce(Reducer, Materialized)} can be used to compute aggregate functions like sum, min, or
         * max.
         * <p>
         * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         * <p>
         * To query the local {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}.
         * <pre>{@code
         * KafkaStreams streams = [] // compute sum
         * string queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
         * IReadOnlyKeyValueStore<string, long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<string, long>KeyValueStore());
         * string key = "some-key";
         * long sumForKey = localStore[key); // key must be local (application state is shared over all running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         *
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
         * @param reducer       a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
         * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key. If the reduce function returns {@code null}, it is then interpreted as
         * deletion for the key, and future messages of the same key coming from upstream operators
         * will be handled as newly initialized value.
         */
        IKTable<K, V> Reduce(
            IReducer<V> reducer,
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized);

        IKTable<K, V> Reduce(
            IReducer<V> reducer,
            Materialized<K, V> materialized);

        /**
         * Aggregate the values of records in this stream by the grouped key.
         * Records with {@code null} key or value are ignored.
         * Aggregating is a generalization of {@link #reduce(Reducer) combining via reduce(...)} as it, for example,
         * allows the result to have a different type than the input values.
         * <p>
         * The specified {@link Initializer} is applied once directly before the first input record is processed to
         * provide an initial intermediate aggregation result that is used to process the first record.
         * The specified {@link IAggregator} is applied for each input record and computes a new aggregate using the current
         * aggregate (or for the very first record using the intermediate aggregation result provided via the
         * {@link Initializer}) and the record's value.
         * Thus, {@code aggregate(Initializer, IAggregator)} can be used to compute aggregate functions like
         * count (c.f. {@link #count()}).
         * <p>
         * The default value serde from config will be used for serializing the result.
         * If a different serde is required then you should use {@link #aggregate(Initializer, IAggregator, Materialized)}.
         * <p>
         * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         *
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
         * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result
         * @param aggregator    an {@link IAggregator} that computes a new aggregate result
         * @param          the value type of the resulting {@link KTable}
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
         * deletion for the key, and future messages of the same key coming from upstream operators
         * will be handled as newly initialized value.
         */
        IKTable<K, VR> Aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> aggregator);

        /**
         * Aggregate the values of records in this stream by the grouped key.
         * Records with {@code null} key or value are ignored.
         * Aggregating is a generalization of {@link #reduce(Reducer) combining via reduce(...)} as it, for example,
         * allows the result to have a different type than the input values.
         * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
         * that can be queried by the given store name in {@code materialized}.
         * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
         * <p>
         * The specified {@link Initializer} is applied once directly before the first input record is processed to
         * provide an initial intermediate aggregation result that is used to process the first record.
         * The specified {@link IAggregator} is applied for each input record and computes a new aggregate using the current
         * aggregate (or for the very first record using the intermediate aggregation result provided via the
         * {@link Initializer}) and the record's value.
         * Thus, {@code aggregate(Initializer, IAggregator, Materialized)} can be used to compute aggregate functions like
         * count (c.f. {@link #count()}).
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
         * KafkaStreams streams = [] // some aggregation on value type double
         * string queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
         * IReadOnlyKeyValueStore<string, long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<string, long>KeyValueStore());
         * string key = "some-key";
         * long aggForKey = localStore[key); // key must be local (application state is shared over all running Kafka Streams instances)
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
         * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result
         * @param aggregator    an {@link IAggregator} that computes a new aggregate result
         * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
         * @param          the value type of the resulting {@link KTable}
         * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
         * latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
         * deletion for the key, and future messages of the same key coming from upstream operators
         * will be handled as newly initialized value.
         */
        IKTable<K, VR> Aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> aggregator,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized);

        IKTable<K, VR> Aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> aggregator,
            Materialized<K, VR> materialized);

        /**
         * Create a new {@link TimeWindowedKStream} instance that can be used to perform windowed aggregations.
         * @param windows the specification of the aggregation {@link Windows}
         * @param     the window type
         * @return an instance of {@link TimeWindowedKStream}
         */
        //TimeWindowedKStream<K, V> windowedBy<W>(Windows<W> windows)
        //    where W : Window;

        /**
         * Create a new {@link SessionWindowedKStream} instance that can be used to perform session windowed aggregations.
         * @param windows the specification of the aggregation {@link SessionWindows}
         * @return an instance of {@link TimeWindowedKStream}
         */
        //SessionWindowedKStream<K, V> windowedBy(SessionWindows windows);
    }
}
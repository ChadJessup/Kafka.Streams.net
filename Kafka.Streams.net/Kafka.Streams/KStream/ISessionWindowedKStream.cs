using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Sessions;

namespace Kafka.Streams.KStream
{
    /**
     * {@code SessionWindowedKStream} is an abstraction of a <i>windowed</i> record stream of {@link KeyValuePair} pairs.
     * It is an intermediate representation after a grouping and windowing of a {@link KStream} before an aggregation is applied to the
     * new (partitioned) windows resulting in a windowed {@link KTable}
     * (a <emph>windowed</emph> {@code KTable} is a {@link KTable} with key type {@link Windowed IWindowed<K>}.
     * <p>
     * {@link SessionWindows} are dynamic data driven windows.
     * They have no fixed time boundaries, rather the size of the window is determined by the records.
     * Please see {@link SessionWindows} for more details.
     * <p>
     * New events are.Added to {@link SessionWindows} until their grace period ends (see {@link SessionWindows#grace(TimeSpan)}).
     *
     * Furthermore, updates are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * A {@code SessionWindowedKStream} must be obtained from a {@link KGroupedStream} via {@link KGroupedStream#windowedBy(SessionWindows)} .
     *
     * @param Type of keys
     * @param Type of values
     * @see KStream
     * @see KGroupedStream
     * @see SessionWindows
     */
    public interface ISessionWindowedKStream<K, V>
    {
        /**
         * Count the number of records in this stream by the grouped key into {@link SessionWindows}.
         * Records with {@code null} key or value are ignored.
         * <p>
         * Not All updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same window and key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         *
         * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link long} values
         * that represent the latest (rolling) count (i.e., number of records) for each key within a window
         */
        IKTable<IWindowed<K>, long> Count();

        /**
         * Count the number of records in this stream by the grouped key into {@link SessionWindows}.
         * Records with {@code null} key or value are ignored.
         * The result is written into a local {@link ISessionStore} (which is basically an ever-updating
         * materialized view) that can be queried using the Name provided with {@link Materialized}.
         * <p>
         * Not All updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
         * the same window and key if caching is enabled on the {@link Materialized} instance.
         * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
         * <p>
         * To query the local windowed {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}.
         * <pre>{@code
         * KafkaStreams streams = [] // compute sum
         * Sting queryableStoreName = [] // the queryableStoreName should be the Name of the store as defined by the Materialized instance
         * ReadOnlySessionStore<string,long> localWindowStore = streams.Store(queryableStoreName, QueryableStoreTypes.<string, long>ReadOnlySessionStore<string, long>);
         * string key = "some-key";
         * KeyValueIterator<IWindowed<string>, long> sumForKeyForWindows = localWindowStore.Fetch(key); // key must be local (application state is shared over All running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         *
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * Therefore, the store Name defined by the Materialized instance must be a valid Kafka topic Name and cannot contain characters other than ASCII
         * alphanumerics, '.', '_' and '-'.
         * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "storeName" is the
         * provide store Name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
         *
         * You can retrieve All generated internal topic names via {@link Topology#describe()}.
         *
         * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
         *                      Note: the valueSerde will be automatically set to {@link Serdes#Serdes.Long()} if there is no valueSerde provided
         * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link long} values
         * that represent the latest (rolling) count (i.e., number of records) for each key within a window
         */
        IKTable<IWindowed<K>, long> Count(Materialized<K, long, ISessionStore<Bytes, byte[]>> materialized);

        /**
         * Aggregate the values of records in this stream by the grouped key and defined {@link SessionWindows}.
         * Records with {@code null} key or value are ignored.
         * Aggregating is a generalization of {@link #reduce(Reducer) combining via
         * reduce(...)} as it, for example, allows the result to have a different type than the input values.
         * <p>
         * The specified {@link Initializer} is applied once per session directly before the first input record is
         * processed to provide an initial intermediate aggregation result that is used to process the first record.
         * The specified {@link IAggregator} is applied for each input record and computes a new aggregate using the current
         * aggregate (or for the very first record using the intermediate aggregation result provided via the
         * {@link Initializer}) and the record's value.
         * The specified {@link Merger} is used to merge 2 existing sessions into one, i.e., when the windows overlap,
         * they are merged into a single session and the old sessions are discarded.
         * Thus, {@code aggregate(Initializer, IAggregator, Merger)} can be used to compute
         * aggregate functions like count (c.f. {@link #count()})
         * <p>
         * The default value serde from config will be used for serializing the result.
         * If a different serde is required then you should use {@link #aggregate(Initializer, IAggregator, Merger, Materialized)}.
         * <p>
         * Not All updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same window and key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         * <p>
         * @param initializer    the instance of {@link Initializer}
         * @param aggregator     the instance of {@link IAggregator}
         * @param sessionMerger  the instance of {@link Merger}
         * @param            the value type of the resulting {@link KTable}
         * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
         * the latest (rolling) aggregate for each key within a window
         */
        IKTable<IWindowed<K>, VR> Aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> aggregator,
            Merger<K, VR> sessionMerger);

        IKTable<IWindowed<K>, VR> Aggregate<VR>(
            Initializer<VR> initializer,
            IAggregator<K, V, VR> aggregator,
            Merger<K, VR> sessionMerger)
            => this.Aggregate(new WrappedInitializer<VR>(initializer), aggregator, sessionMerger);

        IKTable<IWindowed<K>, VR> Aggregate<VR>(
            Initializer<VR> initializer,
            Aggregator<K, V, VR> aggregator,
            Merger<K, VR> sessionMerger)
            => this.Aggregate(
                new WrappedInitializer<VR>(initializer),
                new WrappedAggregator<K, V, VR>(aggregator),
                sessionMerger);

        /**
         * Aggregate the values of records in this stream by the grouped key and defined {@link SessionWindows}.
         * Records with {@code null} key or value are ignored.
         * The result is written into a local {@link ISessionStore} (which is basically an ever-updating
         * materialized view) that can be queried using the Name provided with {@link Materialized}.
         * Aggregating is a generalization of {@link #reduce(Reducer) combining via
         * reduce(...)} as it, for example, allows the result to have a different type than the input values.
         * <p>
         * The specified {@link Initializer} is applied once per session directly before the first input record is
         * processed to provide an initial intermediate aggregation result that is used to process the first record.
         * The specified {@link IAggregator} is applied for each input record and computes a new aggregate using the current
         * aggregate (or for the very first record using the intermediate aggregation result provided via the
         * {@link Initializer}) and the record's value.
         * * The specified {@link Merger} is used to merge 2 existing sessions into one, i.e., when the windows overlap,
         * they are merged into a single session and the old sessions are discarded.
         * Thus, {@code aggregate(Initializer, IAggregator, Merger)} can be used to compute
         * aggregate functions like count (c.f. {@link #count()})
         * <p>
         * Not All updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
         * the same window and key if caching is enabled on the {@link Materialized} instance.
         * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
         * <p>
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}.
         * <pre>{@code
         * KafkaStreams streams = [] // some windowed aggregation on value type double
         * Sting queryableStoreName = [] // the queryableStoreName should be the Name of the store as defined by the Materialized instance
         * ReadOnlySessionStore<string, long> sessionStore = streams.Store(queryableStoreName, QueryableStoreTypes.<string, long>sessionStore());
         * string key = "some-key";
         * KeyValueIterator<IWindowed<string>, long> aggForKeyForSession = localWindowStore.Fetch(key); // key must be local (application state is shared over All running Kafka Streams instances)
         * }</pre>
         *
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * Therefore, the store Name defined by the Materialized instance must be a valid Kafka topic Name and cannot contain characters other than ASCII
         * alphanumerics, '.', '_' and '-'.
         * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "storeName" is the
         * provide store Name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
         *
         * You can retrieve All generated internal topic names via {@link Topology#describe()}.
         *
         * @param initializer    the instance of {@link Initializer}
         * @param aggregator     the instance of {@link IAggregator}
         * @param sessionMerger  the instance of {@link Merger}
         * @param materialized   an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}
         * @param           the value type of the resulting {@link KTable}
         * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
         * the latest (rolling) aggregate for each key within a window
         */
        IKTable<IWindowed<K>, VR> Aggregate<VR>(
            IInitializer<VR> initializer,
            IAggregator<K, V, VR> aggregator,
            Merger<K, VR> sessionMerger,
            Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized);

        IKTable<IWindowed<K>, VR> Aggregate<VR>(
            Initializer<VR> initializer,
            IAggregator<K, V, VR> aggregator,
            Merger<K, VR> sessionMerger,
            Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized)
            => this.Aggregate<VR>(
                new WrappedInitializer<VR>(initializer),
                aggregator,
                sessionMerger,
                materialized);

        IKTable<IWindowed<K>, VR> Aggregate<VR>(
            Initializer<VR> initializer,
            Aggregator<K, V, VR> aggregator,
            Merger<K, VR> sessionMerger,
            Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized);

        /**
         * Combine values of this stream by the grouped key into {@link SessionWindows}.
         * Records with {@code null} key or value are ignored.
         * Combining implies that the type of the aggregate result is the same as the type of the input value
         * (c.f. {@link #aggregate(Initializer, IAggregator, Merger)}).
         * The result is written into a local {@link ISessionStore} (which is basically an ever-updating
         * materialized view).
         * <p>
         * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
         * aggregate and the record's value.
         * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
         * value as-is.
         * Thus, {@code reduce(Reducer)} can be used to compute aggregate functions like sum, min,
         * or max.
         * <p>
         * Not All updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
         * the same window and key.
         * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
         *
         * @param reducer           a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
         * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
         * the latest (rolling) aggregate for each key within a window
         */
        IKTable<IWindowed<K>, V> Reduce(IReducer<V> reducer);
        IKTable<IWindowed<K>, V> Reduce(Reducer<V> reducer);

        /**
         * Combine values of this stream by the grouped key into {@link SessionWindows}.
         * Records with {@code null} key or value are ignored.
         * Combining implies that the type of the aggregate result is the same as the type of the input value
         * (c.f. {@link #aggregate(Initializer, IAggregator, Merger)}).
         * The result is written into a local {@link ISessionStore} (which is basically an ever-updating materialized view)
         * provided by the given {@link Materialized} instance.
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
         * Thus, {@code reduce(Reducer, Materialized)} can be used to compute aggregate functions like
         * sum, min, or max.
         * <p>
         * Not All updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
         * the same window and key if caching is enabled on the {@link Materialized} instance.
         * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
         * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
         * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
         * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
         * <p>
         * To query the local windowed {@link KeyValueStore} it must be obtained via
         * {@link KafkaStreams#store(string, QueryableStoreType) KafkaStreams#store(...)}.
         * <pre>{@code
         * KafkaStreams streams = [] // compute sum
         * Sting queryableStoreName = [] // the queryableStoreName should be the Name of the store as defined by the Materialized instance
         * ReadOnlySessionStore<string,long> localWindowStore = streams.Store(queryableStoreName, QueryableStoreTypes.<string, long>ReadOnlySessionStore<string, long>);
         * string key = "some-key";
         * KeyValueIterator<IWindowed<string>, long> sumForKeyForWindows = localWindowStore.Fetch(key); // key must be local (application state is shared over All running Kafka Streams instances)
         * }</pre>
         * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
         * query the value of the key on a parallel running instance of your Kafka Streams application.
         *
         * <p>
         * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
         * Therefore, the store Name defined by the Materialized instance must be a valid Kafka topic Name and cannot contain characters other than ASCII
         * alphanumerics, '.', '_' and '-'.
         * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
         * user-specified in {@link StreamsConfig} via parameter
         * {@link StreamsConfig#ApplicationIdConfig ApplicationIdConfig}, "storeName" is the
         * provide store Name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
         * You can retrieve All generated internal topic names via {@link Topology#describe()}.
         *
         *
         * @param reducer a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
         * @param materializedAs an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}
         * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
         * the latest (rolling) aggregate for each key within a window
         */
        IKTable<IWindowed<K>, V> Reduce(
            IReducer<V> reducer,
            Materialized<K, V, ISessionStore<Bytes, byte[]>> materializedAs);

        IKTable<IWindowed<K>, V> Reduce(
            Reducer<V> reducer,
            Materialized<K, V, ISessionStore<Bytes, byte[]>> materializedAs);
    }
}

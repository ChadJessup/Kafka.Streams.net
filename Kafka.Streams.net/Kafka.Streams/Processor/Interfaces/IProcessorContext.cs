using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.Processor.Interfaces
{
    /**
     * Processor context interface.
     */
    public interface IProcessorContext<K, V>
    {
        /**
         * Returns the application id
         *
         * @return the application id
         */
        string applicationId();

        /**
         * Returns the task id
         *
         * @return the task id
         */
        TaskId taskId();

        /**
         * Returns the default key serde
         *
         * @return the key serializer
         */
        ISerde<K> keySerde();

        /**
         * Returns the default value serde
         *
         * @return the value serializer
         */
        ISerde<V> valueSerde();

        /**
         * Returns the state directory for the partition.
         *
         * @return the state directory
         */
        File stateDir();

        /**
         * Returns Metrics instance
         *
         * @return StreamsMetrics
         */
        IStreamsMetrics metrics { get; }

        /**
         * Registers and possibly restores the specified storage engine.
         *
         * @param store the storage engine
         * @param stateRestoreCallback the restoration callback logic for log-backed state stores upon restart
         *
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         */
        void register(IStateStore store,
                      StateRestoreCallback stateRestoreCallback);

        /**
         * Get the state store given the store name.
         *
         * @param name The store name
         * @return The state store instance
         */
        IStateStore getStateStore(string name);

        /**
         * Schedules a periodic operation for processors. A processor may call this method during
         * {@link Processor#init(IProcessorContext) initialization} or
         * {@link Processor#process(object, object) processing} to
         * schedule a periodic callback &mdash; called a punctuation &mdash; to {@link Punctuator#punctuate(long)}.
         * The type parameter controls what notion of time is used for punctuation:
         * <ul>
         *   <li>{@link PunctuationType#STREAM_TIME} &mdash; uses "stream time", which is advanced by the processing of messages
         *   in accordance with the timestamp as extracted by the {@link TimestampExtractor} in use.
         *   The first punctuation will be triggered by the first record that is processed.
         *   <b>NOTE:</b> Only advanced if messages arrive</li>
         *   <li>{@link PunctuationType#WALL_CLOCK_TIME} &mdash; uses system time (the wall-clock time),
         *   which is advanced independent of whether new messages arrive.
         *   The first punctuation will be triggered after interval has elapsed.
         *   <b>NOTE:</b> This is best effort only as its granularity is limited by how long an iteration of the
         *   processing loop takes to complete</li>
         * </ul>
         *
         * <b>Skipping punctuations:</b> Punctuations will not be triggered more than once at any given timestamp.
         * This means that "missed" punctuation will be skipped.
         * It's possible to "miss" a punctuation if:
         * <ul>
         *   <li>with {@link PunctuationType#STREAM_TIME}, when stream time advances more than interval</li>
         *   <li>with {@link PunctuationType#WALL_CLOCK_TIME}, on GC pause, too short interval, ...</li>
         * </ul>
         *
         * @param interval the time interval between punctuations (supported minimum is 1 millisecond)
         * @param type one of: {@link PunctuationType#STREAM_TIME}, {@link PunctuationType#WALL_CLOCK_TIME}
         * @param callback a function consuming timestamps representing the current stream or system time
         * @return a handle allowing cancellation of the punctuation schedule established by this method
         */
        ICancellable schedule(
            TimeSpan interval,
            PunctuationType type,
            Punctuator callback);

    /**
     * Forwards a key/value pair to all downstream processors.
     * Used the input record's timestamp as timestamp for the output record.
     *
     * @param key key
     * @param value value
     */
    void forward(K key, V value);

    /**
     * Forwards a key/value pair to the specified downstream processors.
     * Can be used to set the timestamp of the output record.
     *
     * @param key key
     * @param value value
     * @param to the options to use when forwarding
     */
        void forward(K key, V value, To to);

        /**
         * Forwards a key/value pair to one of the downstream processors designated by the downstream processor name
         * @param key key
         * @param value value
         * @param childName name of downstream processor
         * @deprecated please use {@link #forward(object, object, To)} instead
         */
        void forward(K key, V value, string childName);

        /**
         * Requests a commit
         */
        void commit();

        /**
         * Returns the topic name of the current input record; could be null if it is not
         * available (for example, if this method is invoked from the punctuate call)
         *
         * @return the topic name
         */
        string Topic { get; }

        /**
         * Returns the partition id of the current input record; could be -1 if it is not
         * available (for example, if this method is invoked from the punctuate call)
         *
         * @return the partition id
         */
        int partition();

        /**
         * Returns the offset of the current input record; could be -1 if it is not
         * available (for example, if this method is invoked from the punctuate call)
         *
         * @return the offset
         */
        long offset();

        /**
         * Returns the headers of the current input record; could be null if it is not available
         * @return the headers
         */
        Headers headers();

        /**
         * Returns the current timestamp.
         *
         * If it is triggered while processing a record streamed from the source processor, timestamp is defined as the timestamp of the current input record; the timestamp is extracted from
         * {@link org.apache.kafka.clients.consumer.ConsumeResult ConsumeResult} by {@link TimestampExtractor}.
         *
         * If it is triggered while processing a record generated not from the source processor (for example,
         * if this method is invoked from the punctuate call), timestamp is defined as the current
         * task's stream time, which is defined as the smallest among all its input stream partition timestamps.
         *
         * @return the timestamp
         */
        long timestamp();

        /**
         * Returns all the application config properties as key/value pairs.
         *
         * The config properties are defined in the {@link org.apache.kafka.streams.StreamsConfig}
         * object and associated to the IProcessorContext.
         * <p>
         * The type of the values is dependent on the {@link org.apache.kafka.common.config.ConfigDef.Type type} of the property
         * (e.g. the value of {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG DEFAULT_KEY_SERDE_CLASS_CONFIG}
         * will be of type {@link Class}, even if it was specified as a string to
         * {@link org.apache.kafka.streams.StreamsConfig#StreamsConfig(Map) StreamsConfig(Map)}).
         *
         * @return all the key/values from the StreamsConfig properties
         */
        Dictionary<string, object> appConfigs();

        /**
         * Returns all the application config properties with the given key prefix, as key/value pairs
         * stripping the prefix.
         *
         * The config properties are defined in the {@link org.apache.kafka.streams.StreamsConfig}
         * object and associated to the IProcessorContext.
         *
         * @param prefix the properties prefix
         * @return the key/values matching the given prefix from the StreamsConfig properties.
         *
         */
        Dictionary<string, object> appConfigsWithPrefix(string prefix);
    }
}
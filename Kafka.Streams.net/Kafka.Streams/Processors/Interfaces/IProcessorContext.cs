using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.Processors.Interfaces
{
    /**
     * IProcessor context interface.
     */
    public interface IProcessorContext
    {
        /**
         * Returns the application id
         *
         * @return the application id
         */
        string ApplicationId { get; }

        /**
         * Returns the task id
         *
         * @return the task id
         */
        TaskId TaskId { get; }

        /**
         * Returns the default key serde
         *
         * @return the key serializer
         */
        ISerde KeySerde { get; }

        /**
         * Returns the default value serde
         *
         * @return the value serializer
         */
        ISerde ValueSerde { get; }

        /**
         * Returns the state directory for the partition.
         *
         * @return the state directory
         */
        DirectoryInfo StateDir { get; }

        /**
         * Returns Metrics instance
         *
         * @return IStreamsMetrics
         */
        //IStreamsMetrics metrics { get; }

        /**
         * Registers and possibly restores the specified storage engine.
         *
         * @param store the storage engine
         * @param stateRestoreCallback the restoration callback logic for log-backed state stores upon restart
         *
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         */
        void Register(IStateStore store, IStateRestoreCallback stateRestoreCallback);

        /**
         * Get the state store given the store name.
         *
         * @param name The store name
         * @return The state store instance
         */
        IStateStore GetStateStore(KafkaStreamsContext context, string name);

        /**
         * Schedules a periodic operation for processors. A processor may call this method during
         * {@link IProcessor#init(IProcessorContext) initialization} or
         * {@link IProcessor#process(object, object) processing} to
         * schedule a periodic callback &mdash; called a punctuation &mdash; to {@link Punctuator#punctuate(long)}.
         * The type parameter controls what notion of time is used for punctuation:
         * <ul>
         *   <li>{@link PunctuationType#STREAM_TIME} &mdash; uses "stream time", which is advanced by the processing of messages
         *   in accordance with the timestamp as extracted by the {@link ITimestampExtractor} in use.
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
        ICancellable Schedule(
            TimeSpan interval,
            PunctuationType type,
            IPunctuator callback);

        ICancellable Schedule(
            TimeSpan interval,
            PunctuationType type,
            Action<long> callback);

        /**
         * Forwards a key/value pair to all downstream processors.
         * Used the input record's timestamp as timestamp for the output record.
         *
         * @param key key
         * @param value value
         */
        void Forward<K1, V1>(K1 key, V1 value);

        /**
         * Forwards a key/value pair to the specified downstream processors.
         * Can be used to set the timestamp of the output record.
         *
         * @param key key
         * @param value value
         * @param to the options to use when forwarding
         */
        void Forward<K1, V1>(K1 key, V1 value, To to);

        /**
         * Forwards a key/value pair to one of the downstream processors designated by the downstream processor name
         * @param key key
         * @param value value
         * @param childName name of downstream processor
         * @deprecated please use {@link #forward(object, object, To)} instead
         */
        void Forward<K1, V1>(K1 key, V1 value, string childName);

        /**
         * Requests a commit
         */
        void Commit();

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
        int Partition { get; }

        /**
         * Returns the offset of the current input record; could be -1 if it is not
         * available (for example, if this method is invoked from the punctuate call)
         *
         * @return the offset
         */
        long Offset { get; }

        /**
         * Returns the headers of the current input record; could be null if it is not available
         * @return the headers
         */
        Headers Headers { get; }

        /**
         * Returns the current timestamp.
         *
         * If it is triggered while processing a record streamed from the source processor, timestamp is defined as the timestamp of the current input record; the timestamp is extracted from
         * {@link org.apache.kafka.clients.consumer.ConsumeResult ConsumeResult} by {@link ITimestampExtractor}.
         *
         * If it is triggered while processing a record generated not from the source processor (for example,
         * if this method is invoked from the punctuate call), timestamp is defined as the current
         * task's stream time, which is defined as the smallest among all its input stream partition timestamps.
         *
         * @return the timestamp
         */
        long Timestamp { get; }

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
        Dictionary<string, object> AppConfigs();

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
        Dictionary<string, object> AppConfigsWithPrefix(string prefix);
    }
}

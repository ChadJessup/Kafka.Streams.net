using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Helpers
{
    /**
     * Factory to Create {@link ConsumeResult consumer records} for a single single-partitioned topic with given Key and
     * value {@link Serializer serializers}.
     *
     * @deprecated Since 2.4 use methods of {@link TestInputTopic} instead
     *
     * @param <K> the type of the Key
     * @param <V> the type of the value
     *
     * @see TopologyTestDriver
     */
    public class ConsumerRecordFactory<K, V>
    {
        private readonly string? topicName;
        private readonly ISerializer<K> keySerializer;
        private readonly ISerializer<V> valueSerializer;
        private long timeMs;
        private readonly long advanceMs;

        /**
         * Create a new factory for the given topic.
         * Uses current system time as start timestamp.
         * Auto-advance is disabled.
         *
         * @param keySerializer the Key serializer
         * @param valueSerializer the value serializer
         */
        public ConsumerRecordFactory(
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer)
            : this(
                  null,
                  keySerializer,
                  valueSerializer,
                  SystemClock.AsEpochMilliseconds)
        {
        }

        /**
         * Create a new factory for the given topic.
         * Uses current system time as start timestamp.
         * Auto-advance is disabled.
         *
         * @param defaultTopicName the default topic Name used for All generated {@link ConsumeResult consumer records}
         * @param keySerializer the Key serializer
         * @param valueSerializer the value serializer
         */

        public ConsumerRecordFactory(
            string defaultTopicName,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer)
            : this(
                  defaultTopicName,
                  keySerializer,
                  valueSerializer,
                  SystemClock.AsEpochMilliseconds)
        {
        }

        /**
         * Create a new factory for the given topic.
         * Auto-advance is disabled.
         *
         * @param keySerializer the Key serializer
         * @param valueSerializer the value serializer
         * @param startTimestampMs the initial timestamp for generated records
         */

        public ConsumerRecordFactory(
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            long startTimestampMs)
            : this(null, keySerializer, valueSerializer, startTimestampMs, 0L)
        {
        }

        /**
         * Create a new factory for the given topic.
         * Auto-advance is disabled.
         *
         * @param defaultTopicName the topic Name used for All generated {@link ConsumeResult consumer records}
         * @param keySerializer the Key serializer
         * @param valueSerializer the value serializer
         * @param startTimestampMs the initial timestamp for generated records
         */

        public ConsumerRecordFactory(
            string? defaultTopicName,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            long startTimestampMs)
            : this(
                  defaultTopicName,
                  keySerializer,
                  valueSerializer,
                  startTimestampMs,
                  0L)
        {
        }

        /**
         * Create a new factory for the given topic.
         *
         * @param keySerializer the Key serializer
         * @param valueSerializer the value serializer
         * @param startTimestampMs the initial timestamp for generated records
         * @param autoAdvanceMs the time increment pre generated record
         */

        public ConsumerRecordFactory(
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            long startTimestampMs,
            long autoAdvanceMs)
            : this(
                  null,
                  keySerializer,
                  valueSerializer,
                  startTimestampMs,
                  autoAdvanceMs)
        {
        }

        /**
         * Create a new factory for the given topic.
         *
         * @param defaultTopicName the topic Name used for All generated {@link ConsumeResult consumer records}
         * @param keySerializer the Key serializer
         * @param valueSerializer the value serializer
         * @param startTimestampMs the initial timestamp for generated records
         * @param autoAdvanceMs the time increment pre generated record
         */

        public ConsumerRecordFactory(
            string? defaultTopicName,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            long startTimestampMs,
            long autoAdvanceMs)
        {
            this.topicName = defaultTopicName;
            this.keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
            this.valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
            this.timeMs = startTimestampMs;
            this.advanceMs = autoAdvanceMs;
        }

        public ConsumerRecordFactory(ISerde<K> keySerde, ISerde<V> valueSerde)
            : this(keySerde.Serializer, valueSerde.Serializer)
        {
        }

        public ConsumerRecordFactory(ISerde<K> keySerde, ISerde<V> valueSerde, long startTimestampMs)
            : this(keySerde.Serializer, valueSerde.Serializer, startTimestampMs: startTimestampMs)
        {
        }

        /**
         * Advances the internally tracked time.
         *
         * @param advanceMs the amount of time to advance
         */

        public void AdvanceTimeMs(long advanceMs)
        {
            if (advanceMs < 0)
            {
                throw new ArgumentException("advanceMs must be positive");
            }

            this.timeMs += advanceMs;
        }

        /**
         * Create a {@link ConsumeResult} with the given topic Name, Key, value, headers, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic Name
         * @param Key the record Key
         * @param value the record value
         * @param headers the record headers
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(
            string topicName,
            K key,
            V value,
            Headers headers,
            long timestampMs)
        {
            Objects.requireNonNull(topicName, "topicName cannot be null.");
            Objects.requireNonNull(headers, "headers cannot be null.");
            var serializedKey = this.keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topicName));
            var serializedValue = this.valueSerializer.Serialize(value, new SerializationContext(MessageComponentType.Value, topicName));

            return new ConsumeResult<byte[], byte[]>
            {
                Topic = topicName,
                Partition = -1,
                Offset = -1L,
                Message = new Message<byte[], byte[]>
                {
                    Timestamp = new Timestamp(timestampMs, TimestampType.CreateTime),
                    Key = serializedKey,
                    Value = serializedValue,
                    Headers = headers,
                },
                //(long)ConsumeResult.NULL_CHECKSUM,
            };
        }

        /**
         * Create a {@link ConsumeResult} with the given topic Name and given topic, Key, value, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic Name
         * @param Key the record Key
         * @param value the record value
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(
            string topicName,
            K Key,
            V value,
            long timestampMs)
        {
            return this.Create(topicName, Key, value, new Headers(), timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with default topic Name and given Key, value, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param Key the record Key
         * @param value the record value
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(
            K Key,
            V value,
            long timestampMs)
        {
            return this.Create(Key, value, new Headers(), timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with default topic Name and given Key, value, headers, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param Key the record Key
         * @param value the record value
         * @param headers the record headers
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(
            K Key,
            V value,
            Headers headers,
            long timestampMs)
        {
            if (this.topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #Create(string topicName, K Key, V value, long timestampMs) instead.");
            }

            return this.Create(this.topicName, Key, value, headers, timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with the given topic Name, Key, and value.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic Name
         * @param Key the record Key
         * @param value the record value
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(
            string topicName,
            K Key,
            V value)
        {
            var timestamp = this.timeMs;
            this.timeMs += this.advanceMs;
            return this.Create(topicName, Key, value, new Headers(), timestamp);
        }

        /**
         * Create a {@link ConsumeResult} with the given topic Name, Key, value, and headers.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic Name
         * @param Key the record Key
         * @param value the record value
         * @param headers the record headers
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(
            string topicName,
            K Key,
            V value,
            Headers headers)
        {
            var timestamp = this.timeMs;
            this.timeMs += this.advanceMs;

            return this.Create(topicName, Key, value, headers, timestamp);
        }

        /**
         * Create a {@link ConsumeResult} with default topic Name and given Key and value.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param Key the record Key
         * @param value the record value
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(
            K Key,
            V value)
        {
            return this.Create(Key, value, new Headers());
        }

        /**
         * Create a {@link ConsumeResult} with default topic Name and given Key, value, and headers.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param Key the record Key
         * @param value the record value
         * @param headers the record headers
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(
            K Key,
            V value,
            Headers headers)
        {
            if (this.topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #Create(string topicName, K Key, V value) instead.");
            }

            return this.Create(this.topicName, Key, value, headers);
        }

        /**
         * Create a {@link ConsumeResult} with {@code null}-Key and the given topic Name, value, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic Name
         * @param value the record value
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(
            string topicName,
            V value,
            long timestampMs)
        {
            return this.Create(
                topicName,
                key: default,
                value,
                new Headers(),
                timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with {@code null}-Key and the given topic Name, value, headers, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic Name
         * @param value the record value
         * @param headers the record headers
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(
            string topicName,
            V value,
            Headers headers,
            long timestampMs)
        {
            return this.Create(topicName, default, value, headers, timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with default topic Name and {@code null}-Key as well as given value and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param value the record value
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(V value,
                                                     long timestampMs)
        {
            return this.Create(value, new Headers(), timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with default topic Name and {@code null}-Key as well as given value, headers, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param value the record value
         * @param headers the record headers
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(V value,
                                                     Headers headers,
                                                     long timestampMs)
        {
            if (this.topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #Create(string topicName, V value, long timestampMs) instead.");
            }
            return this.Create(this.topicName, value, headers, timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with {@code null}-Key and the given topic Name, value, and headers.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic Name
         * @param value the record value
         * @param headers the record headers
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(string topicName,
                                                     V value,
                                                     Headers headers)
        {
            return this.Create(topicName, default, value, headers);
        }

        /**
         * Create a {@link ConsumeResult} with {@code null}-Key and the given topic Name and value.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic Name
         * @param value the record value
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(string topicName, V value)
        {
            return this.Create(topicName, default, value, new Headers());
        }

        /**
         * Create a {@link ConsumeResult} with default topic Name and {@code null}-Key was well as given value.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param value the record value
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(V value)
        {
            return this.Create(value, new Headers());
        }

        /**
         * Create a {@link ConsumeResult} with default topic Name and {@code null}-Key was well as given value and headers.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param value the record value
         * @param headers the record headers
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(V value,
                                                     Headers headers)
        {
            if (this.topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #Create(string topicName, V value, long timestampMs) instead.");
            }
            return this.Create(this.topicName, value, headers);
        }

        /**
         * Creates {@link ConsumeResult consumer records} with the given topic Name, keys, and values.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic Name
         * @param keyValues the record keys and values
         * @return the generated {@link ConsumeResult consumer records}
         */
        public List<ConsumeResult<byte[], byte[]>> Create(string topicName,
                                                           List<KeyValuePair<K, V>> keyValues)
        {
            var records = new List<ConsumeResult<byte[], byte[]>>(keyValues.Count);

            foreach (KeyValuePair<K, V> keyValue in keyValues)
            {
                records.Add(this.Create(topicName, keyValue.Key, keyValue.Value));
            }

            return records;
        }

        /**
         * Creates {@link ConsumeResult consumer records} with default topic Name as well as given keys and values.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param keyValues the record keys and values
         * @return the generated {@link ConsumeResult consumer records}
         */
        public List<ConsumeResult<byte[], byte[]>> Create(List<KeyValuePair<K, V>> keyValues)
        {
            if (this.topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #Create(string topicName, List<KeyValuePair<K, V>> keyValues) instead.");
            }

            return this.Create(this.topicName, keyValues);
        }

        /**
         * Creates {@link ConsumeResult consumer records} with the given topic Name, keys, and values.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic Name
         * @param keyValues the record keys and values
         * @param startTimestamp the timestamp for the first generated record
         * @param advanceMs the time difference between two consecutive generated records
         * @return the generated {@link ConsumeResult consumer records}
         */
        public List<ConsumeResult<byte[], byte[]>> Create(string topicName,
                                                           List<KeyValuePair<K, V>> keyValues,
                                                           long startTimestamp,
                                                           long advanceMs)
        {
            if (advanceMs < 0)
            {
                throw new ArgumentException("advanceMs must be positive");
            }

            var records = new List<ConsumeResult<byte[], byte[]>>(keyValues.Count);

            var timestamp = startTimestamp;
            foreach (KeyValuePair<K, V> keyValue in keyValues)
            {
                records.Add(this.Create(topicName, keyValue.Key, keyValue.Value, new Headers(), timestamp));
                timestamp += advanceMs;
            }

            return records;
        }

        /**
         * Creates {@link ConsumeResult consumer records} with default topic Name as well as given keys and values.
         * Does not auto advance internally tracked time.
         *
         * @param keyValues the record keys and values
         * @param startTimestamp the timestamp for the first generated record
         * @param advanceMs the time difference between two consecutive generated records
         * @return the generated {@link ConsumeResult consumer records}
         */
        public List<ConsumeResult<byte[], byte[]>> Create(List<KeyValuePair<K, V>> keyValues,
                                                           long startTimestamp,
                                                           long advanceMs)
        {
            if (this.topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #Create(string topicName, List<KeyValuePair<K, V>> keyValues, long startTimestamp, long advanceMs) instead.");
            }

            return this.Create(this.topicName, keyValues, startTimestamp, advanceMs);
        }

        /**
         * Creates {@link ConsumeResult consumer records} with the given topic Name, keys and values.
         * For each generated record, the time is advanced by 1.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic Name
         * @param keyValues the record keys and values
         * @param startTimestamp the timestamp for the first generated record
         * @return the generated {@link ConsumeResult consumer records}
         */
        public List<ConsumeResult<byte[], byte[]>> Create(string topicName,
                                                           List<KeyValuePair<K, V>> keyValues,
                                                           long startTimestamp)
        {
            return this.Create(topicName, keyValues, startTimestamp, 1);
        }

        /**
         * Creates {@link ConsumeResult consumer records} with the given keys and values.
         * For each generated record, the time is advanced by 1.
         * Does not auto advance internally tracked time.
         *
         * @param keyValues the record keys and values
         * @param startTimestamp the timestamp for the first generated record
         * @return the generated {@link ConsumeResult consumer records}
         */
        public List<ConsumeResult<byte[], byte[]>> Create(List<KeyValuePair<K, V>> keyValues,
                                                           long startTimestamp)
        {
            if (this.topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #Create(string topicName, List<KeyValuePair<K, V>> keyValues, long startTimestamp) instead.");
            }

            return this.Create(this.topicName, keyValues, startTimestamp, 1);
        }
    }
}

using Confluent.Kafka;
using Kafka.Common;
using NodaTime;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Helpers
{
    /**
     * Factory to create {@link ConsumeResult consumer records} for a single single-partitioned topic with given Key and
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
        private readonly string topicName;
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

        public ConsumerRecordFactory(ISerializer<K> keySerializer,
                                     ISerializer<V> valueSerializer)
            : this(null, keySerializer, valueSerializer, SystemClock.Instance.GetCurrentInstant().ToUnixTimeMilliseconds())
        {
        }

        /**
         * Create a new factory for the given topic.
         * Uses current system time as start timestamp.
         * Auto-advance is disabled.
         *
         * @param defaultTopicName the default topic name used for all generated {@link ConsumeResult consumer records}
         * @param keySerializer the Key serializer
         * @param valueSerializer the value serializer
         */

        public ConsumerRecordFactory(string defaultTopicName,
                                     ISerializer<K> keySerializer,
                                     ISerializer<V> valueSerializer)
            : this(defaultTopicName, keySerializer, valueSerializer, SystemClock.Instance.GetCurrentInstant().ToUnixTimeMilliseconds())
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

        public ConsumerRecordFactory(ISerializer<K> keySerializer,
                                     ISerializer<V> valueSerializer,
                                     long startTimestampMs)
            : this(null, keySerializer, valueSerializer, startTimestampMs, 0L)
        {
        }

        /**
         * Create a new factory for the given topic.
         * Auto-advance is disabled.
         *
         * @param defaultTopicName the topic name used for all generated {@link ConsumeResult consumer records}
         * @param keySerializer the Key serializer
         * @param valueSerializer the value serializer
         * @param startTimestampMs the initial timestamp for generated records
         */

        public ConsumerRecordFactory(string defaultTopicName,
                                     ISerializer<K> keySerializer,
                                     ISerializer<V> valueSerializer,
                                     long startTimestampMs)
            : this(defaultTopicName, keySerializer, valueSerializer, startTimestampMs, 0L)
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

        public ConsumerRecordFactory(ISerializer<K> keySerializer,
                                     ISerializer<V> valueSerializer,
                                     long startTimestampMs,
                                     long autoAdvanceMs)
            : this(null, keySerializer, valueSerializer, startTimestampMs, autoAdvanceMs)
        {
        }

        /**
         * Create a new factory for the given topic.
         *
         * @param defaultTopicName the topic name used for all generated {@link ConsumeResult consumer records}
         * @param keySerializer the Key serializer
         * @param valueSerializer the value serializer
         * @param startTimestampMs the initial timestamp for generated records
         * @param autoAdvanceMs the time increment pre generated record
         */

        public ConsumerRecordFactory(string defaultTopicName,
                                     ISerializer<K> keySerializer,
                                     ISerializer<V> valueSerializer,
                                     long startTimestampMs,
                                     long autoAdvanceMs)
        {
            Objects.requireNonNull(keySerializer, "keySerializer cannot be null");
            Objects.requireNonNull(valueSerializer, "valueSerializer cannot be null");
            this.topicName = defaultTopicName;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            timeMs = startTimestampMs;
            advanceMs = autoAdvanceMs;
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

            timeMs += advanceMs;
        }

        /**
         * Create a {@link ConsumeResult} with the given topic name, Key, value, headers, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic name
         * @param Key the record Key
         * @param value the record value
         * @param headers the record headers
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(string topicName,
                                                     K key,
                                                     V value,
                                                     Headers headers,
                                                     long timestampMs)
        {
            Objects.requireNonNull(topicName, "topicName cannot be null.");
            Objects.requireNonNull(headers, "headers cannot be null.");
            var serializedKey = keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topicName));
            var serializedValue = valueSerializer.Serialize(value, new SerializationContext(MessageComponentType.Value, topicName));
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
         * Create a {@link ConsumeResult} with the given topic name and given topic, Key, value, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic name
         * @param Key the record Key
         * @param value the record value
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(string topicName,
                                                     K Key,
                                                     V value,
                                                     long timestampMs)
        {
            return Create(topicName, Key, value, new Headers(), timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with default topic name and given Key, value, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param Key the record Key
         * @param value the record value
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(K Key,
                                                     V value,
                                                     long timestampMs)
        {
            return Create(Key, value, new Headers(), timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with default topic name and given Key, value, headers, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param Key the record Key
         * @param value the record value
         * @param headers the record headers
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(K Key,
                                                     V value,
                                                     Headers headers,
                                                     long timestampMs)
        {
            if (topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #create(string topicName, K Key, V value, long timestampMs) instead.");
            }

            return Create(topicName, Key, value, headers, timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with the given topic name, Key, and value.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic name
         * @param Key the record Key
         * @param value the record value
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(string topicName,
                                                     K Key,
                                                     V value)
        {
            var timestamp = timeMs;
            timeMs += advanceMs;
            return Create(topicName, Key, value, new Headers(), timestamp);
        }

        /**
         * Create a {@link ConsumeResult} with the given topic name, Key, value, and headers.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic name
         * @param Key the record Key
         * @param value the record value
         * @param headers the record headers
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(string topicName,
                                                     K Key,
                                                     V value,
                                                     Headers headers)
        {
            var timestamp = timeMs;
            timeMs += advanceMs;
            return Create(topicName, Key, value, headers, timestamp);
        }

        /**
         * Create a {@link ConsumeResult} with default topic name and given Key and value.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param Key the record Key
         * @param value the record value
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(K Key,
                                                     V value)
        {
            return Create(Key, value, new Headers());
        }

        /**
         * Create a {@link ConsumeResult} with default topic name and given Key, value, and headers.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param Key the record Key
         * @param value the record value
         * @param headers the record headers
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(K Key,
                                                     V value,
                                                     Headers headers)
        {
            if (topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #create(string topicName, K Key, V value) instead.");
            }

            return Create(topicName, Key, value, headers);
        }

        /**
         * Create a {@link ConsumeResult} with {@code null}-Key and the given topic name, value, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic name
         * @param value the record value
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(string topicName,
                                                     V value,
                                                     long timestampMs)
        {
            return Create(topicName, default, value, new Headers(), timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with {@code null}-Key and the given topic name, value, headers, and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic name
         * @param value the record value
         * @param headers the record headers
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */

        public ConsumeResult<byte[], byte[]> Create(string topicName,
                                                     V value,
                                                     Headers headers,
                                                     long timestampMs)
        {
            return Create(topicName, default, value, headers, timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with default topic name and {@code null}-Key as well as given value and timestamp.
         * Does not auto advance internally tracked time.
         *
         * @param value the record value
         * @param timestampMs the record timestamp
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(V value,
                                                     long timestampMs)
        {
            return Create(value, new Headers(), timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with default topic name and {@code null}-Key as well as given value, headers, and timestamp.
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
            if (topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #create(string topicName, V value, long timestampMs) instead.");
            }
            return Create(topicName, value, headers, timestampMs);
        }

        /**
         * Create a {@link ConsumeResult} with {@code null}-Key and the given topic name, value, and headers.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic name
         * @param value the record value
         * @param headers the record headers
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(string topicName,
                                                     V value,
                                                     Headers headers)
        {
            return Create(topicName, default, value, headers);
        }

        /**
         * Create a {@link ConsumeResult} with {@code null}-Key and the given topic name and value.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic name
         * @param value the record value
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(string topicName, V value)
        {
            return Create(topicName, default, value, new Headers());
        }

        /**
         * Create a {@link ConsumeResult} with default topic name and {@code null}-Key was well as given value.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param value the record value
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(V value)
        {
            return Create(value, new Headers());
        }

        /**
         * Create a {@link ConsumeResult} with default topic name and {@code null}-Key was well as given value and headers.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param value the record value
         * @param headers the record headers
         * @return the generated {@link ConsumeResult}
         */
        public ConsumeResult<byte[], byte[]> Create(V value,
                                                     Headers headers)
        {
            if (topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #create(string topicName, V value, long timestampMs) instead.");
            }
            return Create(topicName, value, headers);
        }

        /**
         * Creates {@link ConsumeResult consumer records} with the given topic name, keys, and values.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param topicName the topic name
         * @param keyValues the record keys and values
         * @return the generated {@link ConsumeResult consumer records}
         */
        public List<ConsumeResult<byte[], byte[]>> Create(string topicName,
                                                           List<KeyValuePair<K, V>> keyValues)
        {
            var records = new List<ConsumeResult<byte[], byte[]>>(keyValues.Count);

            foreach (KeyValuePair<K, V> keyValue in keyValues)
            {
                records.Add(Create(topicName, keyValue.Key, keyValue.Value));
            }

            return records;
        }

        /**
         * Creates {@link ConsumeResult consumer records} with default topic name as well as given keys and values.
         * The timestamp will be generated based on the constructor provided start time and time will auto advance.
         *
         * @param keyValues the record keys and values
         * @return the generated {@link ConsumeResult consumer records}
         */
        public List<ConsumeResult<byte[], byte[]>> Create(List<KeyValuePair<K, V>> keyValues)
        {
            if (topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #create(string topicName, List<KeyValuePair<K, V>> keyValues) instead.");
            }

            return Create(topicName, keyValues);
        }

        /**
         * Creates {@link ConsumeResult consumer records} with the given topic name, keys, and values.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic name
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
                records.Add(Create(topicName, keyValue.Key, keyValue.Value, new Headers(), timestamp));
                timestamp += advanceMs;
            }

            return records;
        }

        /**
         * Creates {@link ConsumeResult consumer records} with default topic name as well as given keys and values.
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
            if (topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #create(string topicName, List<KeyValuePair<K, V>> keyValues, long startTimestamp, long advanceMs) instead.");
            }

            return Create(topicName, keyValues, startTimestamp, advanceMs);
        }

        /**
         * Creates {@link ConsumeResult consumer records} with the given topic name, keys and values.
         * For each generated record, the time is advanced by 1.
         * Does not auto advance internally tracked time.
         *
         * @param topicName the topic name
         * @param keyValues the record keys and values
         * @param startTimestamp the timestamp for the first generated record
         * @return the generated {@link ConsumeResult consumer records}
         */
        public List<ConsumeResult<byte[], byte[]>> Create(string topicName,
                                                           List<KeyValuePair<K, V>> keyValues,
                                                           long startTimestamp)
        {
            return Create(topicName, keyValues, startTimestamp, 1);
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
            if (topicName == null)
            {
                throw new Exception("ConsumerRecordFactory was created without defaultTopicName. " +
                    "Use #create(string topicName, List<KeyValuePair<K, V>> keyValues, long startTimestamp) instead.");
            }

            return Create(topicName, keyValues, startTimestamp, 1);
        }
    }
}

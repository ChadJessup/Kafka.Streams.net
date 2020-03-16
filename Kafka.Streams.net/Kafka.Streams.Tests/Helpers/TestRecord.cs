using Confluent.Kafka;
using NodaTime;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Helpers
{
    /**
     * A key/value pair, including timestamp and record headers, to be sent to or received from {@link TopologyTestDriver}.
     * If [a] record does not contain a timestamp,
     * {@link TestInputTopic} will auto advance it's time when the record is piped.
     */
    public class TestRecord<K, V>
    {
        public Headers? Headers { get; }
        public K Key { get; }
        public V Value { get; }
        public Instant? RecordTime { get; }

        /**
         * Creates a record.
         *
         * @param key The key that will be included in the record
         * @param value The value of the record
         * @param headers the record headers that will be included in the record
         * @param recordTime The timestamp of the record.
         */
        public TestRecord(K key, V value, Headers? headers, Instant recordTime)
        {
            this.Key = key;
            this.Value = value;
            this.RecordTime = recordTime;
            this.Headers = headers;
        }

        /**
         * Creates a record.
         * 
         * @param key The key that will be included in the record
         * @param value The value of the record
         * @param headers the record headers that will be included in the record
         * @param timestampMs The timestamp of the record, in milliseconds since the beginning of the epoch.
         */
        public TestRecord(K key, V value, Headers headers, long? timestampMs)
        {
            if (timestampMs != null)
            {
                if (timestampMs < 0)
                {
                    throw new ArgumentException(
                        string.Format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestampMs));
                }

                this.RecordTime = Instant.FromUnixTimeMilliseconds(timestampMs.Value);
            }
            else
            {
                this.RecordTime = null;
            }

            this.Key = key;
            this.Value = value;
            this.Headers = headers;
        }

        /**
         * Creates a record.
         *
         * @param key The key of the record
         * @param value The value of the record
         * @param recordTime The timestamp of the record as Instant.
         */
        public TestRecord(K key, V value, Instant recordTime)
            : this(key, value, null, recordTime)
        {
        }

        /**
         * Creates a record.
         *
         * @param key The key of the record
         * @param value The value of the record
         * @param headers The record headers that will be included in the record
         */
        public TestRecord(K key, V value, Headers headers)
        {
            this.Key = key;
            this.Value = value;
            this.Headers = headers;
            this.RecordTime = null;
        }

        /**
         * Creates a record.
         *
         * @param key The key of the record
         * @param value The value of the record
         */
        public TestRecord(K key, V value)
        {
            this.Key = key;
            this.Value = value;
            this.Headers = new Headers();
            this.RecordTime = null;
        }

        /**
         * Create a record with {@code null} key.
         *
         * @param value The value of the record
         */
        public TestRecord(V value)
            : this(default, value)
        {
        }

        /**
         * Create a {@code TestRecord} from a {@link ConsumeResult}.
         *
         * @param record The v
         */
        public TestRecord(ConsumeResult<K, V> record)
        {
            if (record is null)
            {
                throw new ArgumentNullException(nameof(record));
            }

            this.Key = record.Key;
            this.Value = record.Value;
            this.Headers = record.Headers;
            this.RecordTime = Instant.FromUnixTimeMilliseconds(record.Timestamp.UnixTimestampMs);
        }

        /**
         * Create a {@code TestRecord} from a {@link Message}.
         *
         * @param record The record contents
         */
        public TestRecord(Message<K, V> record)
        {
            if (record is null)
            {
                throw new ArgumentNullException(nameof(record));
            }

            this.Key = record.Key;
            this.Value = record.Value;
            this.Headers = record.Headers;
            this.RecordTime = Instant.FromUnixTimeMilliseconds(record.Timestamp.UnixTimestampMs);
        }

        /**
         * @return The timestamp, which is in milliseconds since epoch.
         */
        public long? Timestamp()
        {
            return this.RecordTime == null
                ? (long?)null
                : this.RecordTime.Value.ToUnixTimeMilliseconds();
        }

        public override string ToString()
        {
            return $"{this.GetType().FullName}[" +
                "key=" + Key +
                "value=" + Value +
                "headers=" + Headers +
                "recordTime=" + RecordTime +
                "]";
        }

        public override bool Equals(object? o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            var that = (TestRecord<K, V>)o;
            return Headers.Equals(that.Headers)
                    && Key.Equals(that.Key)
                    && Value.Equals(that.Value)
                    && RecordTime.Equals(that.RecordTime);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Headers, Key, Value, RecordTime);
        }
    }
}

using Confluent.Kafka;

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
        public DateTime? RecordTime { get; }

        /**
         * Creates a record.
         *
         * @param key The key that will be included in the record
         * @param value The value of the record
         * @param headers the record headers that will be included in the record
         * @param recordTime The timestamp of the record.
         */
        public TestRecord(K key, V value, Headers? headers, DateTime recordTime)
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

                this.RecordTime = Confluent.Kafka.Timestamp.UnixTimestampMsToDateTime(timestampMs.Value);
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
        public TestRecord(K key, V value, DateTime recordTime)
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
            this.RecordTime = record.Timestamp.UtcDateTime;
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
            this.RecordTime = record.Timestamp.UtcDateTime;
        }

        /**
         * @return The timestamp, which is in milliseconds since epoch.
         */
        public long? Timestamp()
        {
            return this.RecordTime == null
                ? (long?)null
                : Confluent.Kafka.Timestamp.DateTimeToUnixTimestampMs(this.RecordTime.Value);
        }

        public override string ToString()
        {
            return $"{this.GetType().FullName}[" +
                "key=" + this.Key +
                "value=" + this.Value +
                "headers=" + this.Headers +
                "recordTime=" + this.RecordTime +
                "]";
        }

        public override bool Equals(object? o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (TestRecord<K, V>)o;
            return this.Headers.Equals(that.Headers)
                    && this.Key.Equals(that.Key)
                    && this.Value.Equals(that.Value)
                    && this.RecordTime.Equals(that.RecordTime);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.Headers, this.Key, this.Value, this.RecordTime);
        }
    }
}

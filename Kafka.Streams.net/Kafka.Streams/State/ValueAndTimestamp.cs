using System;
using Confluent.Kafka;

namespace Kafka.Streams.State
{
    public static class ValueAndTimestamp
    {
        /**
         * Return the wrapped {@code value} of the given {@code valueAndTimestamp} parameter
         * if the parameter is not {@code null}.
         *
         * @param valueAndTimestamp a {@link ValueAndTimestamp} instance; can be {@code null}
         * @param <V> the type of the value
         * @return the wrapped {@code value} of {@code valueAndTimestamp} if not {@code null}; otherwise {@code null}
         */
        public static V GetValueOrNull<V>(IValueAndTimestamp<V>? valueAndTimestamp)
        {
            if (valueAndTimestamp == null)
            {
                return default;
            }

            return valueAndTimestamp.Value;
        }

        /**
         * Create a new {@link ValueAndTimestamp} instance if the provide {@code value} is not {@code null}.
         *
         * @param value      the value
         * @param timestamp  the timestamp
         * @param the type of the value
         * @return a new {@link ValueAndTimestamp} instance if the provide {@code value} is not {@code null};
         *         otherwise {@code null} is returned
         */
        public static IValueAndTimestamp<V>? Make<V>(V value, DateTime timestamp)
        {
            return value == null
                ? null
                : new ValueAndTimestamp<V>(value, timestamp);
        }

        public static IValueAndTimestamp<V>? Make<V>(V value, long timestamp)
        {
            return value == null
                ? null
                : new ValueAndTimestamp<V>(value, Timestamp.UnixTimestampMsToDateTime(timestamp));
        }
    }

    /**
     * Combines a value from a {@link KeyValuePair} with a timestamp.
     *
     * @param <V>
     */

    public interface IValueAndTimestamp
    {
        DateTime Timestamp { get; }
    }

    public interface IValueAndTimestamp<out V> : IValueAndTimestamp
    {
        V Value { get; }
    }

    public class ValueAndTimestamp<V> : IValueAndTimestamp<V>
    {
        public V Value { get; }
        public DateTime Timestamp { get; }
        public ValueAndTimestamp(V value, DateTime timestamp)
        {
            value = value ?? throw new ArgumentNullException(nameof(value));

            this.Value = value;
            this.Timestamp = timestamp;
        }

        public override string ToString()
            => "<" + this.Value + "," + this.Timestamp + ">";

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (IValueAndTimestamp<object>)o;

            return this.Timestamp == that.Timestamp
                && this.Value.Equals(that.Value);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.Value, this.Timestamp);
        }
    }
}

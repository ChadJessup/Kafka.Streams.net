using System;

namespace Kafka.Streams.State
{
    /**
     * Combines a value from a {@link KeyValue} with a timestamp.
     *
     * @param <V>
     */
    public class ValueAndTimestamp<V>
    {
        public V value { get; }
        public long timestamp { get; }

        public ValueAndTimestamp(V value, long timestamp)
        {
            value = value ?? throw new ArgumentNullException(nameof(value));

            this.value = value;
            this.timestamp = timestamp;
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
        public static ValueAndTimestamp<V>? make(V value, long timestamp)
        {
            return value == null
                ? null
                : new ValueAndTimestamp<V>(value, timestamp);
        }

        /**
         * Return the wrapped {@code value} of the given {@code valueAndTimestamp} parameter
         * if the parameter is not {@code null}.
         *
         * @param valueAndTimestamp a {@link ValueAndTimestamp} instance; can be {@code null}
         * @param <V> the type of the value
         * @return the wrapped {@code value} of {@code valueAndTimestamp} if not {@code null}; otherwise {@code null}
         */
        public static V getValueOrNull(ValueAndTimestamp<V> valueAndTimestamp)
        {
            return valueAndTimestamp.value;
        }

        public override string ToString()
            => "<" + value + "," + timestamp + ">";

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            var that = (ValueAndTimestamp<object>)o;

            return timestamp == that.timestamp
                && value.Equals(that.value);
        }

        public override int GetHashCode()
        {
            return (value, timestamp).GetHashCode();
        }
    }
}
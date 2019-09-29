using System;

namespace Kafka.Streams
{
    /**
     * A key-value pair defined for a single Kafka Streams record.
     * If the record comes directly from a Kafka topic then its key/value are defined as the message key/value.
     *
     * @param Key type
     * @param Value type
     */
    public class KeyValue<K, V>
    {
        /** The key of the key-value pair. */
        public K Key { get; }
        /** The value of the key-value pair. */
        public V Value { get; }

        /**
         * Create a new key-value pair.
         *
         * @param key   the key
         * @param value the value
         */
        public KeyValue(K key, V value)
        {
            this.Key = key ?? throw new ArgumentNullException(nameof(key));
            this.Value = value ?? throw new ArgumentNullException(nameof(value));
        }

        /**
         * Create a new key-value pair.
         *
         * @param key   the key
         * @param value the value
         * @param   the type of the key
         * @param   the type of the value
         * @return a new key-value pair
         */
        public static KeyValue<K, V> Pair(K key, V value)
        {
            return new KeyValue<K, V>(key, value);
        }

        public override string ToString()
        {
            return $"KeyValue({this.Key}, {this.Value})";
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }

            if (!(obj is KeyValue<K, V>))
            {
                return false;
            }

            KeyValue<K, V> other = (KeyValue<K, V>)obj;

            return this.Key.Equals(other.Key) && this.Value.Equals(other.Value);
        }

        public override int GetHashCode()
        {
            return (this.Key, this.Value).GetHashCode();
        }
    }
}
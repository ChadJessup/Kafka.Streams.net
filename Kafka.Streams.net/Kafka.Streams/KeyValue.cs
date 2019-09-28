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
        public K key;
        /** The value of the key-value pair. */
        public V value;

        /**
         * Create a new key-value pair.
         *
         * @param key   the key
         * @param value the value
         */
        public KeyValue(K key, V value)
        {
            this.key = key;
            this.value = value;
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
        public static KeyValue<K, V> pair(K key, V value)
        {
            return new KeyValue<K, V>(key, value);
        }

        public override string ToString()
        {
            return "KeyValue(" + key + ", " + value + ")";
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
            return key.Equals(other.key) && value.Equals(other.value);
        }

        public override int GetHashCode()
        {
            return (key, value).GetHashCode();
        }
    }
}
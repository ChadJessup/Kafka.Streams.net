﻿namespace Kafka.Streams
{
    public class KeyValueTimestamp<K, V>
    {
        public KeyValueTimestamp(K key, V value, long timestamp)
        {
            this.Key = key;
            this.Value = value;
            this.Timestamp = timestamp;
        }

        public K Key { get; }
        public V Value { get; }
        public long Timestamp { get; }

        public override string ToString()
        {
            return $"KeyValueTimestamp{{key={this.Key}, " +
                $"value={this.Value}, " +
                $"timestamp={this.Timestamp}";
        }

        public override bool Equals(object o)
        {
            if (this == o) return true;
            if (o == null || this.GetType() != o.GetType()) return false;

            var that = (KeyValueTimestamp<K, V>)o;

            return Timestamp == that.Timestamp &&
                this.Key.Equals(that.Key) &&
                this.Value.Equals(that.Value);
        }

        public override int GetHashCode()
        {
            return (Key, Value, Timestamp).GetHashCode();
        }
    }
}

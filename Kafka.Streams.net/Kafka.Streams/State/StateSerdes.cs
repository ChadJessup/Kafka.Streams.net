using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.State
{
    public static class StateSerdes
    {
        /**
        * Create a new instance of {@link StateSerdes} for the given state Name and key-/value-type classes.
        *
        * @param topic      the topic Name
        * @param keyClass   the class of the key type
        * @param valueClass the class of the value type
        * @param <K>        the key type
        * @param <V>        the value type
        * @return a new instance of {@link StateSerdes}
        */
        public static StateSerdes<K, V> WithBuiltinTypes<K, V>(string topic)
        {
            return new StateSerdes<K, V>(
                topic,
                Serdes.SerdeFrom<K>(),
                Serdes.SerdeFrom<V>());
        }
    }

    /**
    * Factory for creating serializers / deserializers for state stores in Kafka Streams.
    *
    * @param <K> key type of serde
    * @param <V> value type of serde
    */
    public class StateSerdes<K, V> : IStateSerdes<K, V>
    {
        public string Topic { get; }
        private readonly ISerde<K> keySerde;
        private readonly ISerde<V> valueSerde;

        /**
         * Create a context for serialization using the specified serializers and deserializers which
         * <em>must</em> match the key and value types used as parameters for this object; the state changelog topic
         * is provided to bind this serde factory to, so that future calls for serialize / deserialize do not
         * need to provide the topic Name any more.
         *
         * @param topic         the topic Name
         * @param keySerde      the serde for keys; cannot be null
         * @param valueSerde    the serde for values; cannot be null
         * @throws ArgumentException if key or value serde is null
         */
        public StateSerdes(string topic, ISerde<K> keySerde, ISerde<V> valueSerde)
        {
            this.Topic = topic ?? throw new ArgumentNullException(nameof(topic));
            this.keySerde = keySerde ?? throw new ArgumentNullException(nameof(keySerde));
            this.valueSerde = valueSerde ?? throw new ArgumentNullException(nameof(valueSerde));
        }

        /**
         * Return the key deserializer.
         *
         * @return the key deserializer
         */
        public IDeserializer<K> KeyDeserializer()
            => this.keySerde.Deserializer;

        /**
         * Return the key serializer.
         *
         * @return the key serializer
         */
        public ISerializer<K> KeySerializer()
            => this.keySerde.Serializer;

        /**
         * Return the value deserializer.
         *
         * @return the value deserializer
         */
        public IDeserializer<V> ValueDeserializer()
            => this.valueSerde.Deserializer;

        /**
         * Return the value serializer.
         *
         * @return the value serializer
         */
        public ISerializer<V> ValueSerializer()
            => this.valueSerde.Serializer;

        /**
         * Deserialize the key from raw bytes.
         *
         * @param rawKey  the key as raw bytes
         * @return        the key as typed object
         */
        public K KeyFrom(byte[] rawKey)
            => this.keySerde.Deserializer.Deserialize(
                rawKey,
                isNull: rawKey == null,
                new SerializationContext(MessageComponentType.Key, this.Topic));

        /**
         * Deserialize the value from raw bytes.
         *
         * @param RawValue  the value as raw bytes
         * @return          the value as typed object
         */
        public V ValueFrom(byte[] RawValue)
            => this.valueSerde.Deserializer.Deserialize(
                RawValue,
                RawValue == null,
                new SerializationContext(MessageComponentType.Value, this.Topic));

        /**
         * Serialize the given key.
         *
         * @param key  the key to be serialized
         * @return     the serialized key
         */
        public byte[] RawKey(K key)
        {
            try
            {
                return this.keySerde.Serializer.Serialize(key, new SerializationContext(MessageComponentType.Key, this.Topic));
            }
            catch (InvalidCastException e)
            {
                var keyClass = key == null
                    ? "unknown because key is null"
                    : key.GetType().FullName;

                throw new StreamsException(
                        $"A serializer ({this.KeySerializer().GetType().FullName}) is not compatible to the actual key type " +
                        $"(key type: {keyClass}). Change the default Serdes in StreamConfig or " +
                        "provide correct Serdes via method parameters.", e);
            }
        }

        /**
         * Serialize the given value.
         *
         * @param value  the value to be serialized
         * @return       the serialized value
         */
        public byte[] RawValue(V value)
        {
            try
            {
                return this.valueSerde.Serializer.Serialize(value, new SerializationContext(MessageComponentType.Value, this.Topic));
            }
            catch (InvalidCastException e)
            {
                string valueClass;
                Type serializerClass;
                if (this.ValueSerializer() is ValueAndTimestampSerializer<V>)
                {
                    serializerClass = ((ValueAndTimestampSerializer<V>)this.ValueSerializer()).valueSerializer.GetType();
                    valueClass = value == null ? "unknown because value is null" : value.GetType().FullName;
                }
                else
                {
                    serializerClass = this.ValueSerializer().GetType();
                    valueClass = value == null ? "unknown because value is null" : value.GetType().FullName;
                }

                throw new StreamsException(
                        $"A serializer ({serializerClass.FullName}) is not compatible to the actual value type " +
                        $"(value type: {valueClass}). Change the default Serdes in StreamConfig or " +
                        "provide correct Serdes via method parameters.", e);
            }
        }
    }
}

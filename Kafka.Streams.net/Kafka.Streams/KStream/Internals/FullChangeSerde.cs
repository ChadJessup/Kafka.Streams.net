
using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class FullChangeSerde<T>
    {
        private readonly ISerde<T> inner;

        public static FullChangeSerde<T> Wrap(ISerde<T> serde)
        {
            if (serde == null)
            {
                return null;
            }
            else
            {
                return new FullChangeSerde<T>(serde);
            }
        }

        private FullChangeSerde(ISerde<T> inner)
        {
            this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public ISerde<T> InnerSerde()
        {
            return inner;
        }

        public IChange<byte[]> SerializeParts(string topic, IChange<T> data)
        {
            if (data == null)
            {
                return null;
            }

            ISerializer<T> innerSerializer = InnerSerde().Serializer;

            var oldBytes = data.OldValue == null ? null : innerSerializer.Serialize(data.OldValue, new SerializationContext(MessageComponentType.Key, topic));
            var newBytes = data.NewValue == null ? null : innerSerializer.Serialize(data.NewValue, new SerializationContext(MessageComponentType.Key, topic));

            return new Change<byte[]>(newBytes, oldBytes);
        }

        public IChange<T> DeserializeParts(string topic, IChange<byte[]> serialChange)
        {
            if (serialChange == null)
            {
                return null;
            }

            IDeserializer<T> innerDeserializer = InnerSerde().Deserializer;

            var oldValue = innerDeserializer.Deserialize(serialChange.OldValue, isNull: serialChange.OldValue == null, new SerializationContext(MessageComponentType.Key, topic));
            var newValue = innerDeserializer.Deserialize(serialChange.NewValue, isNull: serialChange.NewValue == null, new SerializationContext(MessageComponentType.Key, topic));

            return new Change<T>(newValue, oldValue);
        }

        /**
         * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still keep this logic here
         * so that we can produce the legacy string.Format to test that we can still deserialize it.
         */
        public static byte[] MergeChangeArraysIntoSingleLegacyFormattedArray(IChange<byte[]> serialChange)
        {
            if (serialChange == null)
            {
                return null;
            }

            var oldSize = serialChange.OldValue == null ? -1 : serialChange.OldValue.Length;
            var newSize = serialChange.NewValue == null ? -1 : serialChange.NewValue.Length;

            ByteBuffer buffer = new ByteBuffer().Allocate(sizeof(int) * 2 + Math.Max(0, oldSize) + Math.Max(0, newSize));

            buffer.PutInt(oldSize);

            if (serialChange.OldValue != null)
            {
                buffer.Add(serialChange.OldValue);
            }

            buffer.PutInt(newSize);
            if (serialChange.NewValue != null)
            {
                buffer.Add(serialChange.NewValue);
            }
            return buffer.Array();
        }

        /**
         * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still
         * need to be able to read it (so that we can load the state store from previously-written changelog records).
         */
        public static IChange<byte[]> DecomposeLegacyFormattedArrayIntoChangeArrays(byte[] data)
        {
            if (data == null)
            {
                return null;
            }

            ByteBuffer buffer = new ByteBuffer().Wrap(data);

            var oldSize = buffer.GetInt();
            var oldBytes = oldSize == -1
                ? null
                : new byte[oldSize];

            if (oldBytes != null)
            {
                buffer.Get(oldBytes);
            }

            var newSize = buffer.GetInt();
            var newBytes = newSize == -1 ? null : new byte[newSize];
            if (newBytes != null)
            {
                buffer.Get(newBytes);
            }

            return new Change<byte[]>(newBytes, oldBytes);
        }
    }
}

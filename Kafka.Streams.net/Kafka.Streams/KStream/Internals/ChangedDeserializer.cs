using System;
using Confluent.Kafka;

namespace Kafka.Streams.KStream.Internals
{
    public class ChangedDeserializer<T> : IDeserializer<IChange<T>>
    {
        private static int NEWFLAG_SIZE = 1;

        private IDeserializer<T> inner;

        public ChangedDeserializer(IDeserializer<T> inner)
        {
            this.inner = inner;
        }

        public void SetInner(IDeserializer<T> inner)
        {
            this.inner = inner;
        }

        public IChange<T> Deserialize(string topic, Headers headers, byte[] data)
        {
            byte[] bytes = new byte[data.Length - NEWFLAG_SIZE];

            Array.Copy(data, 0, bytes, 0, bytes.Length);

            if (new ByteBuffer().Wrap(data).GetLong(data.Length - NEWFLAG_SIZE) != 0)
            {
                return new Change<T>(inner.Deserialize(topic, bytes, isKey: true), default);
            }
            else
            {
                return new Change<T>(default, inner.Deserialize(topic, bytes, isKey: true));
            }
        }

        public IChange<T> Deserialize(string topic, byte[] data)
            => Deserialize(topic, headers: null, data);

        public IChange<T> Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            //  => this.Deserialize(inner.Deserialize<T>(context.Topic, data, context.Component.Ha);
            return default;
        }
    }
}

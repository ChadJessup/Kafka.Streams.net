using Confluent.Kafka;
using Kafka.Streams.Errors;

namespace Kafka.Streams.KStream.Internals
{
    public class ChangedSerializer<T> : ISerializer<IChange<T>>, ISerializer<T>
    {
        private const int NEWFLAG_SIZE = 1;

        public ISerializer<T> inner { get; private set; }

        public ChangedSerializer(ISerializer<T> inner)
        {
            this.inner = inner;
        }

        public void SetInner(ISerializer<T> inner)
        {
            this.inner = inner;
        }

        /**
         * @throws StreamsException if both old and new values of data are null, or if
         * both values are not null
         */
        public byte[] Serialize(IChange<T> data, SerializationContext context)
        {
            // only one of the old / new values would be not null
            if (data.NewValue != null)
            {
                if (data.OldValue != null)
                {
                    throw new StreamsException("Both old and new values are not null (" + data.OldValue
                        + " : " + data.NewValue + ") in ChangeSerializer, which is not allowed.");
                }

                return this.inner.Serialize(data.NewValue, context);
            }
            else
            {
                if (data.OldValue == null)
                {
                    throw new StreamsException("Both old and new values are null in ChangeSerializer, which is not allowed.");
                }

                return this.inner.Serialize(data.OldValue, context);
            }
        }

        public byte[] Serialize(T data, SerializationContext context)
        {
            byte[] serializedKey = this.inner.Serialize(data, context);

            ByteBuffer buf = new ByteBuffer().Allocate(serializedKey.Length + NEWFLAG_SIZE);
            buf.Add(serializedKey);
            buf.Add((byte)(data != null ? 1 : 0));

            return buf.Array();
        }
    }
}

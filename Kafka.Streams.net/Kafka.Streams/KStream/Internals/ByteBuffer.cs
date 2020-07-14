
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class ByteBuffer
    {
        public ByteBuffer Add(byte[] serializedKey)
        {
            return this;
        }

        internal byte[] Array()
        {
            throw new NotImplementedException();
        }

        public ByteBuffer Allocate(int v)
        {
            return this;
        }

        internal void Add(byte v)
        {
            throw new NotImplementedException();
        }

        internal void PutInt(int oldSize)
        {
            throw new NotImplementedException();
        }

        internal ByteBuffer Wrap(byte[] data)
        {
            return this;
        }

        internal int GetInt()
        {
            throw new NotImplementedException();
        }

        internal void Get(byte[] oldBytes)
        {
            throw new NotImplementedException();
        }

        internal ByteBuffer PutLong(long timestamp)
        {
            return this;
        }

        internal long GetLong()
        {
            throw new NotImplementedException();
        }

        internal long GetLong(int v)
        {
            throw new NotImplementedException();
        }

        internal int GetInt(int v)
        {
            throw new NotImplementedException();
        }

        internal void Rewind()
        {
            throw new NotImplementedException();
        }
    }
}

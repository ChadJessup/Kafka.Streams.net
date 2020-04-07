using Kafka.Streams.KStream.Internals;

namespace Kafka.Streams.State.Internals
{
    public static class OrderedBytes
    {
        private const int MIN_KEY_LENGTH = 1;
        /**
         * Returns the upper byte range for a key with a given fixed size maximum suffix
         *
         * Assumes the minimum key.Length is one byte
         */
        public static Bytes UpperRange(Bytes key, byte[] maxSuffix)
        {
            var bytes = key.Get();
            ByteBuffer rangeEnd = new ByteBuffer().Allocate(bytes.Length + maxSuffix.Length);

            var i = 0;
            while (i < bytes.Length && (
                i < MIN_KEY_LENGTH // assumes keys are at least one byte long
                || (bytes[i] & 0xFF) >= (maxSuffix[0] & 0xFF)
                ))
            {
                //rangeEnd.Add(bytes[i++]);
            }

            rangeEnd.Add(maxSuffix);
            //rangeEnd.flip();

            //byte[] res = new byte[rangeEnd.remaining()];
            //new ByteBuffer().Wrap(res).Add(rangeEnd);
            return null; // Bytes.Wrap(res);
        }

        public static Bytes LowerRange(Bytes key, byte[] minSuffix)
        {
            var bytes = key.Get();
            ByteBuffer rangeStart = new ByteBuffer().Allocate(bytes.Length + minSuffix.Length);
            // any key in the range would start at least with the given prefix to be
            // in the range, and have at least SUFFIX_SIZE number of trailing zero bytes.

            // unless there is a maximum key.Length, you can keep appending more zero bytes
            // to keyFrom to create a key that will match the range, yet that would precede
            // KeySchema.toBinaryKey(keyFrom, from, 0) in byte order
            return null;
            //Bytes.Wrap(
            //    rangeStart
            //        .Add(bytes)
            //        .Add(minSuffix)
            //        .array()
            //);
        }
    }
}

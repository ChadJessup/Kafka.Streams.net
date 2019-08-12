using Kafka.Common.Utils;

namespace Kafka.Streams.State.Internals
{
    public class RocksDbDualCFRangeIterator : RocksDbDualCFIterator
    {
        // RocksDb's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        // private Comparator<byte[]> comparator = sizeof(Bytes)_LEXICO_COMPARATOR;
        private byte[] upperBoundKey;

        RocksDbDualCFRangeIterator(string storeName,
                                   RocksIterator iterWithTimestamp,
                                   RocksIterator iterNoTimestamp,
                                   Bytes from,
                                   Bytes to)
        {
            base(storeName, iterWithTimestamp, iterNoTimestamp);
            iterWithTimestamp.seek(from());
            iterNoTimestamp.seek(from());
            upperBoundKey = to.get();
            if (upperBoundKey == null)
            {
                throw new ArgumentNullException("RocksDbDualCFRangeIterator: upperBoundKey is null for key " + to);
            }
        }


        public KeyValue<Bytes, byte[]> makeNext()
        {
            KeyValue<Bytes, byte[]> next = base.makeNext();

            if (next == null)
            {
                return allDone();
            }
            else
            {
                if (comparator.compare(next.key(), upperBoundKey) <= 0)
                {
                    return next;
                }
                else
                {
                    return allDone();
                }
            }
        }
    }
}

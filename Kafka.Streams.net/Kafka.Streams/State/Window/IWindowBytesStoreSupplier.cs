using Kafka.Streams.State.Interfaces;
using System;

namespace Kafka.Streams.State.Window
{
    /**
     * A store supplier that can be used to create one or more {@link WindowStore WindowStore&lt;Byte, byte[]&gt;} instances of type &lt;Byte, byte[]&gt;.
     *
     * For any stores implementing the {@link WindowStore WindowStore&lt;Byte, byte[]&gt;} interface, null value bytes are considered as "not exist". This means:
     *
     * 1. Null value bytes in put operations should be treated as delete.
     * 2. Null value bytes should never be returned in range query results.
     */
    public interface IWindowBytesStoreSupplier : IStoreSupplier<IWindowStore<Bytes, byte[]>>
    {
        /**
         * The number of segments the store has. If your store is segmented then this should be the number of segments
         * in the underlying store.
         * It is also used to reduce the amount of data that is scanned when caching is enabled.
         *
         * @return number of segments
         * @deprecated since 2.1. Use {@link WindowBytesStoreSupplier#segmentIntervalMs()} instead.
         */
        [Obsolete]
        int segments();

        /**
         * The size of the segments (in milliseconds) the store has.
         * If your store is segmented then this should be the size of segments in the underlying store.
         * It is also used to reduce the amount of data that is scanned when caching is enabled.
         *
         * @return size of the segments (in milliseconds)
         */
        long segmentIntervalMs();

        /**
         * The size of the windows (in milliseconds) any store created from this supplier is creating.
         *
         * @return window size
         */
        long windowSize();

        /**
         * Whether or not this store is retaining duplicate keys.
         * Usually only true if the store is being used for joins.
         * Note this should return false if caching is enabled.
         *
         * @return true if duplicates should be retained
         */
        bool retainDuplicates();

        /**
         * The time period for which the {@link WindowStore} will retain historic data.
         *
         * @return retentionPeriod
         */
        long retentionPeriod();
    }
}
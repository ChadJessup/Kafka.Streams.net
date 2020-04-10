using Kafka.Streams.State.Interfaces;
using System;

namespace Kafka.Streams.State.Sessions
{
    /**
     * A store supplier that can be used to create one or more {@link ISessionStore ISessionStore&lt;Byte, byte[]&gt;} instances.
     *
     * For any stores implementing the {@link ISessionStore ISessionStore&lt;Byte, byte[]&gt;} interface, {@code null} value
     * bytes are considered as "not exist". This means:
     * <ol>
     *   <li>{@code null} value bytes in Put operations should be treated as delete.</li>
     *   <li>{@code null} value bytes should never be returned in range query results.</li>
     * </ol>
     */
    public interface ISessionBytesStoreSupplier : IStoreSupplier<ISessionStore<Bytes, byte[]>>
    {
        /**
         * The size of a segment, in milliseconds. Used when caching is enabled to segment the cache
         * and reduce the amount of data that needs to be scanned when performing range queries.
         *
         * @return segmentInterval in milliseconds
         */
        long SegmentIntervalMs();

        /**
         * The time period for which the {@link ISessionStore} will retain historic data.
         *
         * @return retentionPeriod
         */
        TimeSpan RetentionPeriod { get; }
    }
}
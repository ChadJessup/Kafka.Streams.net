using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Processors
{
    /**
     * Interface for batching restoration of a {@link IStateStore}
     *
     * It is expected that implementations of this will not call the {@link StateRestoreCallback#restore(byte[],
     * byte[]]} method.
     */
    public interface IBatchingStateRestoreCallback : IStateRestoreCallback
    {
        /**
         * Called to restore a number of records.  This method is called repeatedly until the {@link IStateStore} is fulled
         * restored.
         *
         * @param records the records to restore.
         */
        void restoreAll(List<KeyValue<byte[], byte[]>> records);
    }
}
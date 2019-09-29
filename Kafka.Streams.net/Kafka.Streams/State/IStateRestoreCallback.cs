namespace Kafka.Streams.State
{
    /**
     * Restoration logic for log-backed state stores upon restart,
     * it takes one record at a time from the logs to apply to the restoring state.
     */

    public interface IStateRestoreCallback
    {
        void restore(byte[] key, byte[] value);
    }
}
namespace Kafka.Streams.Processor.Internals
{
    public static class LockAndOwner
    {
        //FileLock @lock;
        string owningThread;
    }
}
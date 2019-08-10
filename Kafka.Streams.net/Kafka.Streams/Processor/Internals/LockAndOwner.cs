namespace Kafka.Streams.IProcessor.Internals
{
    public class LockAndOwner
    {
        public FileLock @lock { get; set; }
        public string owningThread { get; set; }
    }
}
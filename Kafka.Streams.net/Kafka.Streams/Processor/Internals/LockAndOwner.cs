namespace Kafka.Streams.Processor.Internals
{
    public class LockAndOwner
    {
        public FileLock @lock { get; set; }
        public string owningThread { get; set; }
    }
}
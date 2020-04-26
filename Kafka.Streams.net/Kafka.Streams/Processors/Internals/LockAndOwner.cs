namespace Kafka.Streams.Processors.Internals
{
    public class LockAndOwner
    {
        public LockAndOwner(string owningThread, FileLock lockedFile)
        {
            this.OwningThread = owningThread;
            this.Lockedfile = lockedFile;
        }

        public FileLock Lockedfile { get; }
        public string OwningThread { get; }
    }
}

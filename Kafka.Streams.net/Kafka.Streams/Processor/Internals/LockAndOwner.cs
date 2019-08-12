namespace Kafka.Streams.Processor.Internals
{
    public class LockAndOwner
    {
        private string name;

        public LockAndOwner(string name, FileLock @lock)
        {
            this.name = name;
            this.@lock = @lock;
        }

        public FileLock @lock { get; set; }
        public string owningThread { get; set; }
    }
}
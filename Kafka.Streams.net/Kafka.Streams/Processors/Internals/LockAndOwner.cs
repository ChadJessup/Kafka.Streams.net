namespace Kafka.Streams.Processors.Internals
{
    public class LockAndOwner
    {
        private readonly string Name;

        public LockAndOwner(string Name, FileLock @lock)
        {
            this.Name = Name;
            this.@lock = @lock;
        }

        public FileLock @lock { get; set; }
        public string owningThread { get; set; }
    }
}
using System;
using System.IO;

namespace Kafka.Streams.Processors.Internals
{
    public class FileLock
    {
        private readonly FileStream channel;

        public FileLock(FileStream channel)
        {
            this.channel = channel;
        }

        public void Release()
        {
            this.channel.Unlock(0, this.channel.Length);
        }
    }
}

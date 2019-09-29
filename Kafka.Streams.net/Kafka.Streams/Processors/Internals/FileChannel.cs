using System;

namespace Kafka.Streams.Processors.Internals
{
    internal class FileChannel
    {
        internal FileLock tryLock()
        {
            throw new NotImplementedException();
        }

        internal void close()
        {
            throw new NotImplementedException();
        }

        internal static FileChannel open(string fullName, object cREATE, object wRITE)
        {
            throw new NotImplementedException();
        }
    }
}
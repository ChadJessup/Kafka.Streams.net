using System;

namespace Kafka.Streams.Processors.Internals
{
    internal class FileChannel
    {
        internal FileLock TryLock()
        {
            throw new NotImplementedException();
        }

        internal void Close()
        {
            throw new NotImplementedException();
        }

        internal static FileChannel Open(string fullName, object cREATE, object wRITE)
        {
            throw new NotImplementedException();
        }
    }
}
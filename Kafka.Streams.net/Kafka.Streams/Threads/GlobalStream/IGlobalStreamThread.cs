using System;

namespace Kafka.Streams.Threads.GlobalStream
{
    public interface IGlobalStreamThread : IThread<GlobalStreamThreadStates>, IDisposable
    {
        void Run();
    }
}
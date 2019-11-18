using System;

namespace Kafka.Streams.Threads
{
    public interface IThreadObserver<States>
        where States : Enum
    {
        IThread<States> Thread { get; }
        void SetThread(IThread<States> thread);
    }
}

using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public interface IDirtyEntryFlushListener
    {
        void apply(List<DirtyEntry> dirty);
    }
}
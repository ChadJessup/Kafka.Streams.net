
using Kafka.Streams.Processors.Interfaces;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.Processors.Internals
{
    public class RepointableCancellable : ICancellable
    {
        private PunctuationSchedule schedule;

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void SetSchedule(PunctuationSchedule schedule)
        {
            this.schedule = schedule;
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Cancel()
        {
            schedule.MarkCancelled();
        }
    }
}

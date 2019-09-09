using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class PriorityQueue<T> : Queue<T>
    {
        private int v;
        private object p;

        public PriorityQueue()
        {
        }

        public PriorityQueue(int v, object p)
            : this()
        {
            this.v = v;
            this.p = p;
        }

        internal void Add(PunctuationSchedule sched)
        {
            throw new NotImplementedException();
        }

        internal void offer(RecordQueue recordQueue)
        {
            throw new NotImplementedException();
        }

        internal RecordQueue poll()
        {
            throw new NotImplementedException();
        }
    }
}
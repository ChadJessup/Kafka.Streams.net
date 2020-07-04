using System;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Priority_Queue;

namespace Kafka.Streams.Processors.Internals
{
    public class PunctuationQueue
    {
        private readonly SimplePriorityQueue<PunctuationSchedule> pq = new SimplePriorityQueue<PunctuationSchedule>();

        public ICancellable Schedule(PunctuationSchedule sched)
        {
            lock (this.pq)
            {
                this.pq.Enqueue(sched, sched.timestamp.Ticks);
            }

            return sched.cancellable;
        }

        public void Close()
        {
            lock (this.pq)
            {
                this.pq.Clear();
            }
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public bool MayPunctuate<K, V>(
           DateTime timestamp,
           PunctuationType type,
           ProcessorNodePunctuator<K, V> processorNodePunctuator)
           => this.MayPunctuate<K, V>(
               timestamp,
               type,
               new WrappedProcessorNodePunctuator<K, V>(processorNodePunctuator));

        public bool MayPunctuate<K, V>(
            DateTime timestamp,
            PunctuationType type,
            IProcessorNodePunctuator<K, V> processorNodePunctuator)
        {
            lock (this.pq)
            {
                var punctuated = false;
                PunctuationSchedule top = null;//pq.Peek();
                while (top != null && top.timestamp <= timestamp)
                {
                    PunctuationSchedule sched = top;
                    //pq.poll();

                    if (!sched.isCancelled)
                    {
                        processorNodePunctuator.Punctuate((ProcessorNode<K, V>)sched.Node(), timestamp, type, sched.punctuator);
                        // sched can be cancelled from within the punctuator
                        if (!sched.isCancelled)
                        {
                            this.pq.Enqueue(sched.Next(timestamp), sched.timestamp.Ticks);
                        }

                        punctuated = true;
                    }

                    top = null;// pq.Peek();
                }

                return punctuated;
            }
        }

    }
}

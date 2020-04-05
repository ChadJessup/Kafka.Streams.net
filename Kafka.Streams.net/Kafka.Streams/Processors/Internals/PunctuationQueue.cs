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
            lock (pq)
            {
                pq.Enqueue(sched, sched.timestamp);
            }

            return sched.cancellable;
        }

        public void Close()
        {
            lock (pq)
            {
                pq.Clear();
            }
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public bool MayPunctuate<K, V>(long timestamp, PunctuationType type, IProcessorNodePunctuator<K, V> processorNodePunctuator)
        {
            lock (pq)
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
                            pq.Enqueue(sched.Next(timestamp), sched.timestamp);
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
using Kafka.Streams.Processor.Interfaces;
using Priority_Queue;

namespace Kafka.Streams.Processor.Internals
{
    public class PunctuationQueue
    {
        private readonly SimplePriorityQueue<PunctuationSchedule> pq = new SimplePriorityQueue<PunctuationSchedule>();

        public ICancellable schedule(PunctuationSchedule sched)
        {
            lock (pq)
            {
                pq.Enqueue(sched, sched.timestamp);
            }

            return sched.cancellable;
        }

        public void close()
        {
            lock (pq)
            {
                pq.Clear();
            }
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public bool mayPunctuate<K, V>(long timestamp, PunctuationType type, IProcessorNodePunctuator<K, V> processorNodePunctuator)
        {
            lock (pq)
            {
                bool punctuated = false;
                PunctuationSchedule top = null;//pq.Peek();
                while (top != null && top.timestamp <= timestamp)
                {
                    PunctuationSchedule sched = top;
                    //pq.poll();

                    if (!sched.isCancelled)
                    {
                        processorNodePunctuator.punctuate((ProcessorNode<K, V>)sched.node(), timestamp, type, sched.punctuator);
                        // sched can be cancelled from within the punctuator
                        if (!sched.isCancelled)
                        {
                            pq.Enqueue(sched.next(timestamp), sched.timestamp);
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
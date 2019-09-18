using Kafka.Streams.Processor.Interfaces;

namespace Kafka.Streams.Processor.Internals
{
    public class PunctuationQueue
    {
        private readonly PriorityQueue<PunctuationSchedule> pq = new PriorityQueue<PunctuationSchedule>();

        public ICancellable schedule(PunctuationSchedule sched)
        {
            lock (pq)
            {
                pq.Add(sched);
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
                PunctuationSchedule top = pq.Peek();
                while (top != null && top.timestamp <= timestamp)
                {
                    PunctuationSchedule sched = top;
                    pq.poll();

                    if (!sched.isCancelled)
                    {
                        processorNodePunctuator.punctuate((ProcessorNode<K, V>)sched.node(), timestamp, type, sched.punctuator);
                        // sched can be cancelled from within the punctuator
                        if (!sched.isCancelled)
                        {
                            pq.Add(sched.next(timestamp));
                        }

                        punctuated = true;
                    }

                    top = pq.Peek();
                }

                return punctuated;
            }
        }

    }
}
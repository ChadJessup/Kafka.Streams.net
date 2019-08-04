using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Kafka.Common
{
    // TODO: chad - 8/3/19 - shamelessly taken from:
    // https://blog.adamfurmanek.pl/2018/08/18/trivial-scheduledthreadpoolexecutor-in-c/
    // to unblock initial port...
    public class ScheduledThreadPoolExecutor
    {
        public int ThreadCount => threads.Length;
        public EventHandler<Exception> OnException;

        private ManualResetEvent waiter;
        private Thread[] threads;
        private SortedSet<Tuple<DateTime, Action>> queue;

        public ScheduledThreadPoolExecutor(int threadCount)
        {
            waiter = new ManualResetEvent(false);
            queue = new SortedSet<Tuple<DateTime, Action>>();
            OnException += (o, e) => { };
            threads = Enumerable.Range(0, threadCount).Select(i => new Thread(RunLoop)).ToArray();

            foreach (var thread in threads)
            {
                thread.Start();
            }
        }

        private void RunLoop()
        {
            while (true)
            {
                TimeSpan sleepingTime = TimeSpan.MaxValue;
                bool needToSleep = true;
                Action task = () => { };

                try
                {
                    lock (waiter)
                    {
                        if (queue.Any())
                        {
                            if (queue.First().Item1 <= DateTime.Now)
                            {
                                task = queue.First().Item2;
                                queue.Remove(queue.First());
                                needToSleep = false;
                            }
                            else
                            {
                                sleepingTime = queue.First().Item1 - DateTime.Now;
                            }
                        }
                    }

                    if (needToSleep)
                    {
                        waiter.WaitOne((int)sleepingTime.TotalMilliseconds);
                    }
                    else
                    {
                        task();
                    }
                }
                catch (Exception e)
                {
                    OnException(task, e);
                }
            }
        }
    }
}

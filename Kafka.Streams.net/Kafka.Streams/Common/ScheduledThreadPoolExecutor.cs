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
        public int ThreadCount => this.threads.Length;
        public EventHandler<Exception> OnException;

        private readonly ManualResetEvent waiter;
        private readonly Thread[] threads;
        private readonly SortedSet<Tuple<DateTime, Action>> queue;

        public ScheduledThreadPoolExecutor(int threadCount)
        {
            this.waiter = new ManualResetEvent(false);
            this.queue = new SortedSet<Tuple<DateTime, Action>>();
            this.OnException += (o, e) => { };
            this.threads = Enumerable.Range(0, threadCount).Select(i => new Thread(this.RunLoop)).ToArray();

            foreach (var thread in this.threads)
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
                    lock (this.waiter)
                    {
                        if (this.queue.Any())
                        {
                            if (this.queue.First().Item1 <= DateTime.Now)
                            {
                                task = this.queue.First().Item2;
                                this.queue.Remove(this.queue.First());
                                needToSleep = false;
                            }
                            else
                            {
                                sleepingTime = this.queue.First().Item1 - DateTime.Now;
                            }
                        }
                    }

                    if (needToSleep)
                    {
                        this.waiter.WaitOne((int)sleepingTime.TotalMilliseconds);
                    }
                    else
                    {
                        task();
                    }
                }
                catch (Exception e)
                {
                    this.OnException(task, e);
                }
            }
        }
    }
}

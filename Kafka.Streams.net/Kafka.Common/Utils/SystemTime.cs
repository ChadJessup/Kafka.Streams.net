using Kafka.Common.Utils.Interfaces;
using System;
using System.Threading;

namespace Kafka.Common.Utils
{
    /**
     * A time implementation that uses the system clock and sleep call. Use `Time.SYSTEM` instead of creating an instance
     * of this.
     */
    public SystemTime : ITime
    {

        public long milliseconds()
        {
            return DateTime.Now.Millisecond;
        }

        public long nanoseconds()
        {
            return DateTime.Now.Ticks / 100;
        }

        public void sleep(long ms)
        {
            //Utils.sleep(ms);
        }

        public Timer timer(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void waitObject(object obj, Func<bool> condition, long deadlineMs)
        {
            lock (obj)
            {
                while (true)
                {
                    if (condition())
                        return;

                    long currentTimeMs = milliseconds();
                    if (currentTimeMs >= deadlineMs)
                    {
                        throw new TimeoutException("Condition not satisfied before deadline");
                    }

                    Monitor.Wait(obj, TimeSpan.FromMilliseconds(deadlineMs - currentTimeMs));
                }
            }
        }

    }
}
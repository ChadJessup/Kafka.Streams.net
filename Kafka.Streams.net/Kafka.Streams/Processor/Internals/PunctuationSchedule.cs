/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Punctuator;

public class PunctuationSchedule : Stamped<ProcessorNode> {

    private long interval;
    private Punctuator punctuator;
    private bool isCancelled = false;
    // this Cancellable will be re-pointed at the successor schedule in next()
    private RepointableCancellable cancellable;

    PunctuationSchedule(ProcessorNode node,
                        long time,
                        long interval,
                        Punctuator punctuator)
{
        this(node, time, interval, punctuator, new RepointableCancellable());
        cancellable.setSchedule(this);
    }

    private PunctuationSchedule(ProcessorNode node,
                                long time,
                                long interval,
                                Punctuator punctuator,
                                RepointableCancellable cancellable)
{
        super(node, time);
        this.interval = interval;
        this.punctuator = punctuator;
        this.cancellable = cancellable;
    }

    public ProcessorNode node()
{
        return value;
    }

    public Punctuator punctuator()
{
        return punctuator;
    }

    public ICancellable cancellable()
{
        return cancellable;
    }

    void markCancelled()
{
        isCancelled = true;
    }

    bool isCancelled()
{
        return isCancelled;
    }

    public PunctuationSchedule next(long currTimestamp)
{
        long nextPunctuationTime = timestamp + interval;
        if (currTimestamp >= nextPunctuationTime)
{
            // we missed one ore more punctuations
            // avoid scheduling a new punctuations immediately, this can happen:
            // - when using STREAM_TIME punctuation and there was a gap i.e., no data was
            //   received for at least 2*interval
            // - when using WALL_CLOCK_TIME and there was a gap i.e., punctuation was delayed for at least 2*interval (GC pause, overload, ...)
            long intervalsMissed = (currTimestamp - timestamp) / interval;
            nextPunctuationTime = timestamp + (intervalsMissed + 1) * interval;
        }

        PunctuationSchedule nextSchedule = new PunctuationSchedule(value, nextPunctuationTime, interval, punctuator, cancellable);

        cancellable.setSchedule(nextSchedule);

        return nextSchedule;
    }

    
    public bool Equals(object other)
{
        return super.Equals(other);
    }

    
    public int GetHashCode()
{
        return super.GetHashCode();
    }

    private static class RepointableCancellable : Cancellable {
        private PunctuationSchedule schedule;

        synchronized void setSchedule(PunctuationSchedule schedule)
{
            this.schedule = schedule;
        }

        
        synchronized public void cancel()
{
            schedule.markCancelled();
        }
    }
}

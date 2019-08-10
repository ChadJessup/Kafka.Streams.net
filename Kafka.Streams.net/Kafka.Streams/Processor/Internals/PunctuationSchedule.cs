/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
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
using Kafka.Streams.IProcessor.Interfaces;

namespace Kafka.Streams.IProcessor.Internals
{
    public class PunctuationSchedule<K, V> : Stamped<ProcessorNode<K, V>>
    {

        private long interval;
        private Punctuator punctuator;
        private bool isCancelled = false;
        // this Cancellable will be re-pointed at the successor schedule in next()
        private RepointableCancellable cancellable;

        public PunctuationSchedule(ProcessorNode<K, V> node,
                            long time,
                            long interval,
                            Punctuator punctuator)
            : this(node, time, interval, punctuator, new RepointableCancellable())
        {
            cancellable.setSchedule(this);
        }

        public PunctuationSchedule(ProcessorNode<K, V> node,
                                    long time,
                                    long interval,
                                    Punctuator punctuator,
                                    RepointableCancellable cancellable)
            : base(node, time)
        {
            this.interval = interval;
            this.punctuator = punctuator;
            this.cancellable = cancellable;
        }

        public ProcessorNode<K, V> node()
        {
            return value;
        }

        public void markCancelled()
        {
            isCancelled = true;
        }

        bool isCancelled()
        {
            return isCancelled;
        }

        public PunctuationSchedule<K, V> next(long currTimestamp)
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

            var nextSchedule = new PunctuationSchedule<K, V>(value, nextPunctuationTime, interval, punctuator, cancellable);

            cancellable.setSchedule(nextSchedule);

            return nextSchedule;
        }


        public override bool Equals(object other)
        {
            return base.Equals(other);
        }


        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
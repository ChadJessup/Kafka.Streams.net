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
using Kafka.Common.Utils;
using Kafka.Streams.Errors;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * Iterate over multiple KeyValueSegments
     */
    public class SegmentIterator<S> : IKeyValueIterator<Bytes, byte[]>
            where S : ISegment
    {

        private Bytes from;
        private Bytes to;
        protected IEnumerator<S> segments;
        protected HasNextCondition hasNextCondition;

        private S currentSegment;
        IKeyValueIterator<Bytes, byte[]> currentIterator;

        SegmentIterator(IEnumerator<S> segments,
                        HasNextCondition hasNextCondition,
                        Bytes from,
                        Bytes to)
        {
            this.segments = segments;
            this.hasNextCondition = hasNextCondition;
            this.from = from;
            this.to = to;
        }

        public void close()
        {
            if (currentIterator != null)
            {
                currentIterator.close();
                currentIterator = null;
            }
        }

        public override Bytes peekNextKey()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return currentIterator.peekNextKey();
        }

        public override bool hasNext()
        {
            bool hasNext = false;
            while ((currentIterator == null || !(hasNext = hasNextConditionHasNext()) || !currentSegment.isOpen())
                    && segments.hasNext())
            {
                close();
                currentSegment = segments.next();
                try
                {
                    if (from == null || to == null)
                    {
                        currentIterator = currentSegment.all();
                    }
                    else
                    {
                        currentIterator = currentSegment.range(from, to);
                    }
                }
                catch (InvalidStateStoreException e)
                {
                    // segment may have been closed so we ignore it.
                }
            }
            return currentIterator != null && hasNext;
        }

        private bool hasNextConditionHasNext()
        {
            bool hasNext = false;
            try
            {
                hasNext = hasNextCondition.hasNext(currentIterator);
            }
            catch (InvalidStateStoreException e)
            {
                //already closed so ignore
            }
            return hasNext;
        }

        public KeyValue<Bytes, byte[]> next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return currentIterator.next();
        }

        public void Remove()
        {
            throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
        }
    }
}
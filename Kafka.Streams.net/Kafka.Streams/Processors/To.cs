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
using System;

namespace Kafka.Streams.Processor
{




    /**
     * This is used to provide the optional parameters when sending output records to downstream processor
     * using {@link IProcessorContext#forward(object, object, To)}.
     */
    public class To
    {
        protected string childName;
        protected long timestamp;

        private To(string childName,
                   long timestamp)
        {
            this.childName = childName;
            this.timestamp = timestamp;
        }

        protected To(To to)
            : this(to.childName, to.timestamp)
        {
        }

        public virtual void update(To to)
        {
            childName = to.childName;
            timestamp = to.timestamp;
        }

        /**
         * Forward the key/value pair to one of the downstream processors designated by the downstream processor name.
         * @param childName name of downstream processor
         * @return a new {@link To} instance configured with {@code childName}
         */
        public static To child(string childName)
        {
            return new To(childName, -1);
        }

        /**
         * Forward the key/value pair to all downstream processors
         * @return a new {@link To} instance configured for all downstream processor
         */
        public static To all()
        {
            return new To(null, -1);
        }

        /**
         * Set the timestamp of the output record.
         * @param timestamp the output record timestamp
         * @return itself (i.e., {@code this})
         */
        public To withTimestamp(long timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }


        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }
            To to = (To)o;
            return timestamp == to.timestamp &&
                childName.Equals(to.childName);
        }

        /**
         * Equality is implemented in support of tests, *not* for use in Hash collections, since this is mutable.
         */

        public override int GetHashCode()
        {
            throw new InvalidOperationException("To is unsafe for use in Hash collections");
        }

    }
}
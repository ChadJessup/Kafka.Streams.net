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
using Kafka.Common;

namespace Kafka.Streams.KStream.Internals
{
    public class Change<T>
    {
        public T newValue;
        public T oldValue;

        public Change(T newValue, T oldValue)
        {
            this.newValue = newValue;
            this.oldValue = oldValue;
        }

        public override string ToString()
        {
            return "(" + newValue + "<-" + oldValue + ")";
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

            Change<object> change = (Change<object>)o;
            return newValue.Equals(change.newValue)
                && oldValue.Equals(change.oldValue);
        }

        public override int GetHashCode()
        {
            return (newValue, oldValue).GetHashCode();
        }
    }
}

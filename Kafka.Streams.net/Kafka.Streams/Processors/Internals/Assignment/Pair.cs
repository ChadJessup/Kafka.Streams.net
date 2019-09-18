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

namespace Kafka.Streams.Processor.Internals.Assignment
{
    public class Pair
    {

        private readonly TaskId task1;
        private readonly TaskId task2;

        public Pair(TaskId task1, TaskId task2)
        {
            this.task1 = task1;
            this.task2 = task2;
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

            Pair pair = (Pair)o;

            return (task1.Equals(pair.task1) &&
                    task2.Equals(pair.task2));
        }

        public override int GetHashCode()
        {
            return (task1, task2).GetHashCode();
        }
    }
}
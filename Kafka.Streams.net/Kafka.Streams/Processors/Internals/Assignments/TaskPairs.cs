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
using Kafka.Streams.Tasks;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    public class TaskPairs
    {

        private readonly HashSet<Pair> pairs;
        private readonly int maxPairs;

        TaskPairs(int maxPairs)
        {
            //this.maxPairs = maxPairs;
            //this.pairs = new HashSet<Pair>(maxPairs);
        }

        bool hasNewPair(TaskId task1, HashSet<TaskId> taskIds)
        {
            if (pairs.Count == maxPairs)
            {
                return false;
            }

            foreach (TaskId taskId in taskIds)
            {
                if (!pairs.Contains(pair(task1, taskId)))
                {
                    return true;
                }
            }
            return false;
        }

        void addPairs(TaskId taskId, HashSet<TaskId> assigned)
        {
            foreach (TaskId id in assigned)
            {
                pairs.Add(pair(id, taskId));
            }
        }

        Pair pair(TaskId task1, TaskId task2)
        {
            if (task1.CompareTo(task2) < 0)
            {
                return new Pair(task1, task2);
            }
            return new Pair(task2, task1);
        }
    }
}
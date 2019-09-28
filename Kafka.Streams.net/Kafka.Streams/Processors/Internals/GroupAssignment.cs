﻿using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class GroupAssignment
    {
        private readonly Dictionary<string, Assignment> assignments;

        public GroupAssignment(Dictionary<string, Assignment> assignments)
        {
            this.assignments = assignments;
        }

        public Dictionary<string, Assignment> groupAssignment()
        {
            return assignments;
        }
    }
}

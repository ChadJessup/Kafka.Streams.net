﻿using Kafka.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Interfaces
{
    /**
     * A source node of a topology.
     */
    public interface ISource : INode
    {
        /**
         * The topic names this source node is reading from.
         * @return a set of topic names
         */
        HashSet<string> topicSet();

        /**
         * The pattern used to match topic names that is reading from.
         * @return the pattern used to match topic names
         */
        Regex topicPattern { get; }
    }
}

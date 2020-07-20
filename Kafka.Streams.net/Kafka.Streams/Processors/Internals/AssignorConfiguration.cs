using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Streams.Processors.Internals.Assignments;
using Kafka.Streams.Tasks;

namespace Kafka.Streams.Processors.Internals
{
    internal class AssignorConfiguration
    {
        private Dictionary<string, string?> configs;
        internal ISupplier<ITaskAssignor> taskAssignor;

        public AssignorConfiguration(Dictionary<string, string?> configs)
        {
            this.configs = configs;
        }

        internal IAdminClient adminClient()
        {
            throw new NotImplementedException();
        }

        internal InternalTopicManager internalTopicManager()
        {
            throw new NotImplementedException();
        }

        internal CopartitionedTopicsEnforcer copartitionedTopicsEnforcer()
        {
            throw new NotImplementedException();
        }

        internal RebalanceProtocol rebalanceProtocol()
        {
            throw new NotImplementedException();
        }

        internal AssignmentListener assignmentListener()
        {
            throw new NotImplementedException();
        }

        internal string userEndPoint()
        {
            throw new NotImplementedException();
        }

        internal AssignmentConfigs assignmentConfigs()
        {
            throw new NotImplementedException();
        }

        internal int configuredMetadataVersion(int usedSubscriptionMetadataVersion)
        {
            throw new NotImplementedException();
        }

        internal ITaskManager taskManager()
        {
            throw new NotImplementedException();
        }

        internal StreamsMetadataState streamsMetadataState()
        {
            throw new NotImplementedException();
        }

        internal IClock time()
        {
            throw new NotImplementedException();
        }

        internal int assignmentErrorCode()
        {
            throw new NotImplementedException();
        }

        internal long nextScheduledRebalanceMs()
        {
            throw new NotImplementedException();
        }
    }
}

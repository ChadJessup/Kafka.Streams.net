using Kafka.Common;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class StreamsPartitionAssignor : IConsumerPartitionAssignor//, Configurable
    {
        public static int UNKNOWN = -1;
        private static readonly int VERSION_ONE = 1;
        private static readonly int VERSION_TWO = 2;
        private static readonly int VERSION_THREE = 3;
        private static readonly int VERSION_FOUR = 4;
        private static readonly int EARLIEST_PROBEABLE_VERSION = VERSION_THREE;
        protected HashSet<int> supportedVersions = new HashSet<int>();

        private readonly ILogger log;
        private readonly string logPrefix;
        public enum Error
        {
            NONE = 0,
            INCOMPLETE_SOURCE_TOPIC_METADATA = 1,
            VERSION_PROBING = 2,
        }

        private readonly int code;

        public static Error fromCode(int code)
        {
            return code switch
            {
                0 => Error.NONE,
                1 => Error.INCOMPLETE_SOURCE_TOPIC_METADATA,
                2 => Error.VERSION_PROBING,
                _ => throw new System.ArgumentException("Unknown error code: " + code),
            };
        }

        public GroupAssignment Assign(
            Cluster metadata, 
            GroupSubscription groupSubscription)
        {
            throw new System.NotImplementedException();
        }

        public string Name()
        {
            throw new System.NotImplementedException();
        }
    }
}
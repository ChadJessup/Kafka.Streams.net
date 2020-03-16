using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Tasks
{
    public class AssignedStandbyTasks : AssignedTasks<StandbyTask>
    {
        public AssignedStandbyTasks(ILogger<AssignedStandbyTasks> logger)
            : base(logger, "standby task")
        {
        }
    }
}
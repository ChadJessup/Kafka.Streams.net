using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.Tasks
{
    public class AssignedStandbyTasks : AssignedTasks<StandbyTask>
    {
        public AssignedStandbyTasks(LogContext logContext)
            : base(logContext, "standby task")
        {
        }
    }
}
namespace Kafka.Streams.Processor.Internals
{
    public class AssignedStandbyTasks : AssignedTasks<StandbyTask>
    {
        public AssignedStandbyTasks(LogContext logContext)
            : base(logContext, "standby task")
        {
        }
    }
}
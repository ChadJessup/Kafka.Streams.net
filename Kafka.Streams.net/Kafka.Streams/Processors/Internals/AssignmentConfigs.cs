namespace Kafka.Streams.Processors.Internals
{
    public class AssignmentConfigs
    {
        internal int maxWarmupReplicas;
        internal int numStandbyReplicas;
        internal long probingRebalanceIntervalMs;

        public long AcceptableRecoveryLag { get; internal set; }
    }
}

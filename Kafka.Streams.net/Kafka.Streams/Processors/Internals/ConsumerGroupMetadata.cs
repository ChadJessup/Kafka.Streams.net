namespace Kafka.Streams.Processors.Internals
{
    public class ConsumerGroupMetadata2
    {
        private readonly string groupId;
        private readonly int generationId;
        private readonly string memberId;
        private readonly string? groupInstanceId;

        public ConsumerGroupMetadata2(
            string groupId,
            int generationId,
            string memberId,
            string? groupInstanceId)
        {
            this.groupId = groupId;
            this.generationId = generationId;
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
        }
    }
}

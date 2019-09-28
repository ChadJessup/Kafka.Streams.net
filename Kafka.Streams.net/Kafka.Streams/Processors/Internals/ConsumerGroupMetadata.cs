namespace Kafka.Streams.Processor.Internals
{
    public class ConsumerGroupMetadata
    {
        private readonly string groupId;
        private readonly int generationId;
        private readonly string memberId;
        readonly string? groupInstanceId;

        public ConsumerGroupMetadata(
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

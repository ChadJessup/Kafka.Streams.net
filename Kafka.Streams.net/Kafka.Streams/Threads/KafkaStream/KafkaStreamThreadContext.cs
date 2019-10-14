using Kafka.Streams.Configs;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Threads.KafkaStream
{
    public class KafkaStreamThreadContext : ThreadContext<IKafkaStreamThread, IStateMachine<KafkaStreamThreadStates>, KafkaStreamThreadStates>
    {
        public KafkaStreamThreadContext(
            ILogger<KafkaStreamThreadContext> logger,
            IKafkaStreamThread thread,
            IStateMachine<KafkaStreamThreadStates> stateMachine,
            StreamsConfig config)
            : base(logger, thread, stateMachine, config)
        {
        }
    }
}

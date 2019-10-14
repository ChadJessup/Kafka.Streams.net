using Kafka.Streams.Configs;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Threads.GlobalStream
{
    public class GlobalStreamThreadContext2 : ThreadContext<IGlobalStreamThread, IStateMachine<GlobalStreamThreadStates>, GlobalStreamThreadStates>
    {
        public GlobalStreamThreadContext2(
            ILogger<GlobalStreamThreadContext2> logger,
            IGlobalStreamThread thread,
            IStateMachine<GlobalStreamThreadStates> stateMachine,
            StreamsConfig config)
            : base(logger, thread, stateMachine, config)
        {
        }
    }
}

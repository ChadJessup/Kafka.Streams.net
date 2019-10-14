using System;
using Kafka.Streams.Configs;
using Kafka.Streams.Threads.GlobalStream;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Threads.KafkaStreams
{
    public class KafkaStreamsThreadContext : ThreadContext<IKafkaStreamsThread, IStateMachine<KafkaStreamsThreadStates>, KafkaStreamsThreadStates>
    {
        private readonly IServiceProvider services;
        private readonly GlobalStreamThreadContext globalThreadContext;

        public KafkaStreamsThreadContext(
            ILogger<KafkaStreamsThreadContext> logger,
            IServiceProvider serviceProvider,
            IKafkaStreamsThread thread,
            IStateMachine<KafkaStreamsThreadStates> stateMachine,
            GlobalStreamThreadContext globalStreamThreadContext,
            StreamsConfig config)
            : base(logger, thread, stateMachine, config)
        {
            this.services = serviceProvider;
            this.globalThreadContext = globalStreamThreadContext;
        }

        /**
         * Set the {@link KafkaStreamThread.StateListener} to be notified when state changes. Note this API is internal to
         * Kafka Streams and is not intended to be used by an external application.
         */
        public override void SetStateListener(IStateListener listener)
        {
            this.StateListener = listener;
        }

        public IStateListener GetStateListener()
        {
            if (this.StateListener != null)
            {
                return this.StateListener;
            }

            IStateListener streamStateListener = ActivatorUtilities.GetServiceOrCreateInstance<IStateListener>(this.services);

            streamStateListener.SetThreadStates(this.Thread.ThreadStates);

            return streamStateListener;
        }
    }
}

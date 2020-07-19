using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams
{
    /// <summary>
    /// Context object that is built by the <seealso cref="StreamsBuilder"/>.
    /// This is injected into many components, and contains various thingys,
    /// that will be needed All over the place.
    /// </summary>
    public class KafkaStreamsContext : IServiceProvider, IDisposable, IAsyncDisposable
    {
        private readonly CancellationTokenSource cts = new CancellationTokenSource();

        public KafkaStreamsContext(
            StreamsConfig config,
            InternalTopologyBuilder internalTopologyBuilder,
            StateDirectory stateDirectory,
            IServiceCollection serviceCollection,
            IConfiguration configuration,
            IStoresFactory storesFactory,
            ILoggerFactory loggerFactory,
            IClock clock)
        {
            this.Clock = clock ?? throw new ArgumentNullException(nameof(clock));
            this.StreamsConfig = config ?? throw new ArgumentNullException(nameof(config));
            this.LoggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            this.Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            this.StoresFactory = storesFactory ?? throw new ArgumentNullException(nameof(storesFactory));
            this.StateDirectory = stateDirectory ?? throw new ArgumentNullException(nameof(stateDirectory));
            this.ServiceCollection = serviceCollection ?? throw new ArgumentNullException(nameof(serviceCollection));
            this.InternalTopologyBuilder = internalTopologyBuilder ?? throw new ArgumentNullException(nameof(internalTopologyBuilder));

            this.Logger = this.LoggerFactory.CreateLogger<KafkaStreamsContext>();
        }

        public InternalStreamsBuilder InternalStreamsBuilder { get; internal set; }
        public InternalTopologyBuilder InternalTopologyBuilder { get; }
        public ILogger<KafkaStreamsContext> Logger { get; }
        public Topology Topology { get; internal set; }
        public IConfiguration Configuration { get; }
        public ILoggerFactory LoggerFactory { get; }
        public ServiceProvider Services { get; internal set; }
        public IStoresFactory StoresFactory { get; }
        public IServiceCollection ServiceCollection { get; }
        public StreamsConfig StreamsConfig { get; }
        public StateDirectory StateDirectory { get; }
        public IClock Clock { get; }

        public ILogger<TCategory> CreateLogger<TCategory>()
            => this.LoggerFactory.CreateLogger<TCategory>();

        public object? GetService<T>() => this.Services.GetService<T>();
        public object GetService(Type serviceType) => this.Services.GetService(serviceType);
        public T GetRequiredService<T>() => this.Services.GetRequiredService<T>();
        public IServiceScope CreateScope() => this.Services.CreateScope();

        private bool disposedValue = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    this.LoggerFactory?.Dispose();
                    this.Services?.Dispose();
                }

                this.disposedValue = true;
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            if (this.Services != null)
            {
                await this.Services.DisposeAsync();
            }
        }
    }
}

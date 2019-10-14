using System.Collections.Generic;

namespace Kafka.Streams.State
{
    /**
     * Build a {@link IStateStore} wrapped with optional caching and logging.
     * @param  the type of store to build
     */
    public interface IStoreBuilder<out T>
        where T : IStateStore
    {
        /**
         * Returns a Map containing any log configs that will be used when creating the changelog for the {@link IStateStore}.
         * <p>
         * Note: any unrecognized configs will be ignored by the Kafka brokers.
         *
         * @return Map containing any log configs to be used when creating the changelog for the {@link IStateStore}
         * If {@code loggingEnabled} returns false, this function will always return an empty map
         */
        Dictionary<string, string> logConfig { get; }

        /**
         * @return {@code true} if the {@link IStateStore} should have logging enabled
         */
        bool loggingEnabled { get; }

        /**
         * Return the name of this state store builder.
         * This must be a valid Kafka topic name; valid characters are ASCII alphanumerics, '.', '_' and '-'.
         *
         * @return the name of this state store builder
         */
        string name { get; }

        /**
 * Enable caching on the store.
 * @return  this
 */
        IStoreBuilder<T> WithCachingEnabled();

        /**
         * Disable caching on the store.
         * @return  this
         */
        IStoreBuilder<T> WithCachingDisabled();

        /**
         * Maintain a changelog for any changes made to the store.
         * Use the provided config to set the config of the changelog topic.
         * @param config  config applied to the changelog topic
         * @return this
         */
        IStoreBuilder<T> WithLoggingEnabled(Dictionary<string, string> config);

        /**
         * Disable the changelog for store built by this {@link StoreBuilder}.
         * This will turn off fault-tolerance for your store.
         * By default the changelog is enabled.
         * @return this
         */
        IStoreBuilder<T> WithLoggingDisabled();

        /**
         * Build the store as defined by the builder.
         *
         * @return the built {@link IStateStore}
         */
        T Build();
    }
}
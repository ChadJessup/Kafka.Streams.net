using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream
{
    public interface ISuppressed<K> : INamedOperation<ISuppressed<K>>
    {
        /**
         * Configure the suppression to emit only the " results" from the window.
         *
         * By default all Streams operators emit results whenever new results are available.
         * This includes windowed operations.
         *
         * This configuration will instead emit just one result per key for each window, guaranteeing
         * to deliver only the  result. This option is suitable for use cases in which the business logic
         * requires a hard guarantee that only the  result is propagated. For example, sending alerts.
         *
         * To accomplish this, the operator will buffer events from the window until the window close (that is,
         * until the end-time passes, and.Additionally until the grace period expires). Since windowed operators
         * are required to reject late events for a window whose grace period is expired, there is an.Additional
         * guarantee that the  results emitted from this suppression will match any queriable state upstream.
         *
         * @param bufferConfig A configuration specifying how much space to use for buffering intermediate results.
         *                     This is required to be a "strict" config, since it would violate the " results"
         *                     property to emit early and then issue an update later.
         * @return a " results" mode suppression configuration
         */
        //static ISuppressed<Windowed<K>> untilWindowCloses(IStrictBufferConfig bufferConfig)
        //{
        //    return new FinalResultsSuppressionBuilder<K>(null, bufferConfig);
        //}

        /**
         * Configure the suppression to wait {@code timeToWaitForMoreEvents} amount of time after receiving a record
         * before emitting it further downstream. If another record for the same key arrives in the mean time, it replaces
         * the first record in the buffer but does <em>not</em> re-start the timer.
         *
         * @param timeToWaitForMoreEvents The amount of time to wait, per record, for new events.
         * @param bufferConfig A configuration specifying how much space to use for buffering intermediate results.
         * @param The key type for the KTable to apply this suppression to.
         * @return a suppression configuration
         */
        //static ISuppressed<K> untilTimeLimit(TimeSpan timeToWaitForMoreEvents, IBufferConfig bufferConfig)
        //{
        //    return new SuppressedInternal<K>(null, timeToWaitForMoreEvents, bufferConfig, null, false);
        //}

        /**
         * Use the specified name for the suppression node in the topology.
         * <p>
         * This can be used to insert a suppression without changing the rest of the topology names
         * (and therefore not requiring an application reset).
         * <p>
         * Note however, that once a suppression has buffered some records, removing it from the topology would cause
         * the loss of those records.
         * <p>
         * A suppression can be "disabled" with the configuration {@code untilTimeLimit(Duration.ZERO, ...}.
         *
         * @param name The name to be used for the suppression node and changelog topic
         * @return The same configuration with the.Addition of the given {@code name}.
         */
        ISuppressed<K> withName(string name);
    }
}
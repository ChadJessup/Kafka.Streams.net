using Kafka.Streams.State.Window;

namespace Kafka.Streams.State.TimeStamped
{
    /**
     * Interface for storing the aggregated values of fixed-size time windows.
     * <p>
     * Note, that the stores's physical key type is {@link Windowed Windowed&lt;K&gt;}.
     * In contrast to a {@link WindowStore} that stores plain windowedKeys-value pairs,
     * a {@code TimestampedWindowStore} stores windowedKeys-(value/timestamp) pairs.
     * <p>
     * While the window start- and end-timestamp are fixed per window, the value-side timestamp is used
     * to store the last update timestamp of the corresponding window.
     *
     * @param Type of keys
     * @param Type of values
     */
    public interface ITimestampedWindowStore<K, V> : ITimestampedWindowStore, IWindowStore<K, ValueAndTimestamp<V>>
    {
    }
}
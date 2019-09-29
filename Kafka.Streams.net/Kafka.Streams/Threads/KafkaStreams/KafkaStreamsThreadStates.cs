namespace Kafka.Streams.Threads.KafkaStreams
{
    // container states
    /**
     * Kafka Streams states are the possible state that a Kafka Streams instance can be in.
     * An instance must only be in one state at a time.
     * The expected state transition with the following defined states is:
     *
     * <pre>
     *                 +--------------+
     *         +&lt;----- | Created (0)  |
     *         |       +-----+--------+
     *         |             |
     *         |             v
     *         |       +----+--+------+
     *         |       | Re-          |
     *         +&lt;----- | Balancing (1)| --------&gt;+
     *         |       +-----+-+------+          |
     *         |             | ^                 |
     *         |             v |                 |
     *         |       +--------------+          v
     *         |       | Running (2)  | --------&gt;+
     *         |       +------+-------+          |
     *         |              |                  |
     *         |              v                  |
     *         |       +------+-------+     +----+-------+
     *         +-----&gt; | Pending      |&lt;--- | Error (5)  |
     *                 | Shutdown (3) |     +------------+
     *                 +------+-------+
     *                        |
     *                        v
     *                 +------+-------+
     *                 | Not          |
     *                 | Running (4)  |
     *                 +--------------+
     *
     *
     * </pre>
     * Note the following:
     * - RUNNING state will transit to REBALANCING if any of its threads is in PARTITION_REVOKED state
     * - REBALANCING state will transit to RUNNING if all of its threads are in RUNNING state
     * - Any state except NOT_RUNNING can go to PENDING_SHUTDOWN (whenever close is called)
     * - Of special importance: If the global stream thread dies, or all stream threads die (or both) then
     *   the instance will be in the ERROR state. The user will need to close it.
     */
    public enum KafkaStreamsThreadStates
    {
        UNKNOWN = 0,
        CREATED, //(1, 3),
        REBALANCING, //(2, 3, 5),
        RUNNING, //(1, 3, 5),
        PENDING_SHUTDOWN, //(4),
        NOT_RUNNING,
        ERROR, //(3);
        DEAD,
    }
}

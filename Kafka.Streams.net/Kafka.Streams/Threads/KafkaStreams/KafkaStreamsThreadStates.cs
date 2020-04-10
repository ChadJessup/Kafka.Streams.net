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
     *         <-----  | Created (1)  |
     *         |       +-----+--------+
     *         |             |
     *         |             v
     *         |       +----+--+------+
     *         |       | Re-          |
     *         <------ | Balancing (2)| --------->
     *         |       +-----+-+------+          |
     *         |             | ^                 |
     *         |             v |                 |
     *         |       +--------------+          v
     *         |       | Running (3)  | -------- >
     *         |       +------+-------+          |
     *         |              |                  |
     *         |              v                  |
     *         |       +------+-------+     +----+-------+
     *         +-----> | Pending      |<--- | Error (6)  |
     *                 | Shutdown (4) |     +------------+
     *                 +------+-------+
     *                        |
     *                        v
     *                 +------+-------+
     *                 | Not          |
     *                 | Running (5)  |
     *                 +--------------+
     *
     *
     * </pre>
     * Note the following:
     * - RUNNING state will transit to REBALANCING if any of its threads is in PARTITION_REVOKED state
     * - REBALANCING state will transit to RUNNING if All of its threads are in RUNNING state
     * - Any state except NOT_RUNNING can go to PENDING_SHUTDOWN (whenever Close is called)
     * - Of special importance: If the global stream thread dies, or All stream threads die (or both) then
     *   the instance will be in the ERROR state. The user will need to Close it.
     */
    public enum KafkaStreamsThreadStates
    {
        UNKNOWN = 0,
        CREATED = 1, //(2, 4),
        REBALANCING = 2, //(3, 4, 6),
        RUNNING = 3, //(2, 4, 6),
        PENDING_SHUTDOWN = 4, //(5),
        NOT_RUNNING = 5,
        ERROR = 6, //(4);
        DEAD = 7,
    }
}

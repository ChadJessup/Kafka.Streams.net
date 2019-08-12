//using Kafka.Streams.Processor.Interfaces;

//namespace Kafka.Streams.Processor.Internals
//{
//    public class StreamThread //: Thread
//    {
//        /**
//         * Stream thread states are the possible states that a stream thread can be in.
//         * A thread must only be in one state at a time
//         * The expected state transitions with the following defined states is:
//         *
//         * <pre>
//         *                +-------------+
//         *          +<--- | Created (0) |
//         *          |     +-----+-------+
//         *          |           |
//         *          |           v
//         *          |     +-----+-------+
//         *          +<--- | Starting (1)|
//         *          |     +-----+-------+
//         *          |           |
//         *          |           |
//         *          |           v
//         *          |     +-----+-------+
//         *          +<--- | Partitions  |
//         *          |     | Revoked (2) | <----+
//         *          |     +-----+-------+      |
//         *          |           |              |
//         *          |           v              |
//         *          |     +-----+-------+      |
//         *          |     | Partitions  |      |
//         *          +<--- | Assigned (3)| ---=>+
//         *          |     +-----+-------+      |
//         *          |           |              |
//         *          |           v              |
//         *          |     +-----+-------+      |
//         *          |     | Running (4) | ---=>+
//         *          |     +-----+-------+
//         *          |           |
//         *          |           v
//         *          |     +-----+-------+
//         *          +--=> | Pending     |
//         *                | Shutdown (5)|
//         *                +-----+-------+
//         *                      |
//         *                      v
//         *                +-----+-------+
//         *                | Dead (6)    |
//         *                +-------------+
//         * </pre>
//         *
//         * Note the following:
//         * <ul>
//         *     <li>Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.</li>
//         *     <li>
//         *         State PENDING_SHUTDOWN may want to transit to some other states other than DEAD,
//         *         in the corner case when the shutdown is triggered while the thread is still in the rebalance loop.
//         *         In this case we will forbid the transition but will not treat as an error.
//         *     </li>
//         *     <li>
//         *         State PARTITIONS_REVOKED may want transit to itself indefinitely, in the corner case when
//         *         the coordinator repeatedly fails in-between revoking partitions and assigning new partitions.
//         *         Also during streams instance start up PARTITIONS_REVOKED may want to transit to itself as well.
//         *         In this case we will forbid the transition but will not treat as an error.
//         *     </li>
//         * </ul>
//         */
//        public class StreamThreadState : IThreadStateTransitionValidator
//        {

//            //            CREATED(1, 5), STARTING(2, 5), PARTITIONS_REVOKED(3, 5), PARTITIONS_ASSIGNED(2, 4, 5), RUNNING(2, 5), PENDING_SHUTDOWN(6), DEAD;
//            public static StreamThreadState DEAD { get; internal set; }
//        }
//    }
//}
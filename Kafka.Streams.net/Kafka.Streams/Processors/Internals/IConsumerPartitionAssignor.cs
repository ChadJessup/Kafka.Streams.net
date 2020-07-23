using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.KStream.Internals;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * This interface is used to define custom partition assignment for use in
     * {@link org.apache.kafka.clients.consumer.KafkaConsumer}. Members of the consumer group subscribe
     * to the topics they are interested in and forward their subscriptions to a Kafka broker serving
     * as the group coordinator. The coordinator selects one member to perform the group assignment and
     * propagates the subscriptions of all members to it. Then {@link #assign(Cluster, GroupSubscription)} is called
     * to perform the assignment and the results are forwarded back to each respective members
     *
     * In some cases, it is useful to forward additional metadata to the assignor in order to make
     * assignment decisions. For this, you can override {@link #subscriptionUserData(Set)} and provide custom
     * userData in the returned Subscription. For example, to have a rack-aware assignor, an implementation
     * can use this user data to forward the rackId belonging to each member.
     */
    public interface IConsumerPartitionAssignor
    {
        /**
         * Return serialized data that will be included in the {@link Subscription} sent to the leader
         * and can be leveraged in {@link #assign(Cluster, GroupSubscription)} ((e.g. local host/rack information)
         *
         * @param topics Topics subscribed to through {@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(java.util.Collection)}
         *               and variants
         * @return nullable subscription user data
         */
        public ByteBuffer SubscriptionUserData(HashSet<string> topics)
        {
            return null;
        }

        /**
         * Perform the group assignment given the member subscriptions and current cluster metadata.
         * @param metadata Current topic/broker metadata known by consumer
         * @param groupSubscription Subscriptions from All members including metadata provided through {@link #subscriptionUserData(Set)}
         * @return A map from the members to their respective assignments. This should have one entry
         *         for each member in the input subscription map.
         */
        GroupAssignment Assign(Cluster metadata, GroupSubscription groupSubscription);

        /**
         * Callback which is invoked when a group member receives its assignment from the leader.
         * @param assignment The local member's assignment as provided by the leader in {@link #assign(Cluster, GroupSubscription)}
         * @param metadata Additional metadata on the consumer (optional)
         */
        void OnAssignment(Assignment assignment, IConsumerGroupMetadata metadata);

        /**
         * Indicate which rebalance protocol this assignor works with;
         * By default it should always work with {@link RebalanceProtocol#EAGER}.
         */
        public List<RebalanceProtocol> SupportedProtocols()
        {
            return new List<RebalanceProtocol> { RebalanceProtocol.EAGER };
        }

        /**
         * Return the version of the assignor which indicates how the user metadata encodings
         * and the assignment algorithm gets evolved.
         */
        public short Version()
        {
            return (short)0;
        }

        /**
         * Unique Name for this assignor (e.g. "range" or "roundrobin" or "sticky"). Note, this is not required
         * to be the same as the class Name specified in {@link ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
         * @return non-null unique Name
         */
        string Name();
    }
}

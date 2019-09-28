using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class GroupSubscription
    {
        private readonly Dictionary<string, Subscription> subscriptions;

        public GroupSubscription(Dictionary<string, Subscription> subscriptions)
        {
            this.subscriptions = subscriptions;
        }

        public Dictionary<string, Subscription> groupSubscription()
        {
            return subscriptions;
        }
    }
}

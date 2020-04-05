using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class GroupSubscription
    {
        private readonly Dictionary<string, Subscription> subscriptions;

        public GroupSubscription(Dictionary<string, Subscription> subscriptions)
        {
            this.subscriptions = subscriptions;
        }
    }
}

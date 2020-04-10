
using System.Collections.Generic;
using System.Linq;
/**
* Used to capture subscribed topic via Patterns discovered during the
* partition assignment process.
*/
public class SubscriptionUpdates
{
    private readonly HashSet<string> updatedTopicSubscriptions = new HashSet<string>();

    public void UpdateTopics(List<string> topicNames)
    {
        this.updatedTopicSubscriptions.Clear();
        this.updatedTopicSubscriptions.UnionWith(topicNames);
    }

    public List<string> GetUpdates()
    {
        return this.updatedTopicSubscriptions.ToList();
    }

    public bool HasUpdates()
    {
        return this.updatedTopicSubscriptions.Any();
    }


    public override string ToString()
    {
        return string.Format("SubscriptionUpdates{updatedTopicSubscriptions=%s}", this.updatedTopicSubscriptions);
    }
}

using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Kafka.Streams.Processors.Internals
{
    public class InternalTopicManager
    {
        private readonly ILogger log;
        private readonly Dictionary<string, string> defaultTopicConfigs = new Dictionary<string, string>();
        private readonly IAdminClient adminClient;

        private readonly int retries;

        public InternalTopicManager(IAdminClient adminClient, StreamsConfig streamsConfig)
        {
            this.adminClient = adminClient;

            //new LogContext($"stream-thread [{Thread.CurrentThread.Name}] ");

            //replicationFactor = streamsConfig.GetInt(StreamsConfig.REPLICATION_FACTOR_CONFIG).shortValue();
            //windowChangeLogAdditionalRetention = streamsConfig.GetLong(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG);
            //InternalAdminClientConfig dummyAdmin = new InternalAdminClientConfig(streamsConfig.getAdminConfigs("dummy"));
            //retries = dummyAdmin.GetInt(AdminClientConfig.RETRIES_CONFIG);
            //retryBackOffMs = dummyAdmin.GetLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);

            //log.LogDebug("Configs:" + Utils.NL,
            //    "\t{} = {}" + Utils.NL,
            //    "\t{} = {}" + Utils.NL,
            //    "\t{} = {}",
            //    AdminClientConfig.RETRIES_CONFIG, retries,
            //    StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor,
            //    StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, windowChangeLogAdditionalRetention);

            //foreach (KeyValuePair<string, object> entry in streamsConfig.originalsWithPrefix(StreamsConfig.TOPIC_PREFIX))
            //{
            //    if (entry.Value != null)
            //    {
            //        defaultTopicConfigs.Add(entry.Key, entry.Value.ToString());
            //    }
            //}
        }

        /**
         * Prepares a set of given internal topics.
         *
         * If a topic does not exist creates a new topic.
         * If a topic with the correct number of partitions exists ignores it.
         * If a topic exists already but has different number of partitions we fail and throw exception requesting user to reset the app before restarting again.
         */
        public HashSet<string> MakeReady(Dictionary<string, InternalTopicConfig> topics)
        {
            // we will do the validation / topic-creation in a loop, until we have confirmed All topics
            // have existed with the expected number of partitions, or some create topic returns fatal errors.

            var remainingRetries = this.retries;
            var topicsNotReady = new HashSet<string>(topics.Keys);
            HashSet<string> newTopics = new HashSet<string>();

            while (topicsNotReady.Any() && remainingRetries >= 0)
            {
                topicsNotReady = this.ValidateTopics(topicsNotReady, topics);

                //if (topicsNotReady.Count > 0)
                //{

                //    foreach (string topicName in topicsNotReady)
                //    {
                //        InternalTopicConfig internalTopicConfig = Utils.notNull(topics[topicName]);
                //        Dictionary<string, string> topicConfig = internalTopicConfig.getProperties(defaultTopicConfigs, windowChangeLogAdditionalRetention);

                //        log.LogDebug("Going to create topic {} with {} partitions and config {}.",
                //            internalTopicConfig.Name,
                //            internalTopicConfig.numberOfPartitions(),
                //            topicConfig);

                //        newTopics.Add(
                //            new NewTopic(
                //                internalTopicConfig.Name,
                //                internalTopicConfig.numberOfPartitions(),
                //                replicationFactor)
                //                .configs(topicConfig));
                //    }

                //    CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);

                //    foreach (KeyValuePair<string, KafkaFuture<Void>> createTopicResult in createTopicsResult.Values)
                //    {
                //        string topicName = createTopicResult.Key;
                //        try
                //        {

                //            createTopicResult.Value.Get();
                //            topicsNotReady.Remove(topicName);
                //        }
                //        catch (Exception fatalException)
                //        {
                //            // this should not happen; if it ever happens it indicate a bug
                //            Thread.CurrentThread.Interrupt();
                //            log.LogError(INTERRUPTED_ERROR_MESSAGE, fatalException);
                //            throw new InvalidOperationException(INTERRUPTED_ERROR_MESSAGE, fatalException);
                //        }
                //        catch (Exception executionException)
                //        {
                //            Exception cause = executionException.getCause();
                //            if (cause is TopicExistsException)
                //            {
                //                // This topic didn't exist earlier or its leader not known before; just retain it for next round of validation.
                //                log.LogInformation("Could not create topic {}. Topic is probably marked for deletion (number of partitions is unknown).\n" +
                //                    "Will retry to create this topic in {} ms (to let broker finish async delete operation first).\n" +
                //                    "Error message was: {}", topicName, retryBackOffMs, cause.ToString());
                //            }
                //            else
                //            {

                //                log.LogError("Unexpected error during topic creation for {}.\n" +
                //                    "Error message was: {}", topicName, cause.ToString());
                //                throw new StreamsException(string.Format("Could not create topic %s.", topicName), cause);
                //            }
                //        }
                //    }
                //}


                if (topicsNotReady.Any())
                {
                    this.log.LogInformation("Topics {} can not be made ready with {} retries left", topicsNotReady, this.retries);

                    //Utils.sleep(retryBackOffMs);

                    remainingRetries--;
                }
            }

            if (topicsNotReady.Any())
            {
                var timeoutAndRetryError = string.Format("Could not create topics after %d retries. " +
                    "This can happen if the Kafka cluster is temporary not available. " +
                    "You can increase admin client config `retries` to be resilient against this error.", this.retries);
                this.log.LogError(timeoutAndRetryError);
                throw new StreamsException(timeoutAndRetryError);
            }

            return newTopics;
        }

        /**
         * Try to get the number of partitions for the given topics; return the number of partitions for topics that already exists.
         *
         * Topics that were not able to get its description will simply not be returned
         */
        // visible for testing
        protected Dictionary<string, int> GetNumPartitions(HashSet<string> topics)
        {
            this.log.LogDebug("Trying to check if topics {} have been created with expected number of partitions.", topics);

            //DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
            //Dictionary<string, KafkaFuture<TopicDescription>> futures = describeTopicsResult.Values;

            //Dictionary<string, int> existedTopicPartition = new Dictionary<string, int>();
            //foreach (KeyValuePair<string, KafkaFuture<TopicDescription>> topicFuture in futures)
            //{
            //    string topicName = topicFuture.Key;
            //    try
            //    {
            //        TopicDescription topicDescription = topicFuture.Value.Get();
            //        existedTopicPartition.Add(
            //            topicFuture.Key,
            //            topicDescription.partitions().size());
            //    }
            //    catch (InterruptedException fatalException)
            //    {
            //        // this should not happen; if it ever happens it indicate a bug
            //        Thread.CurrentThread.interrupt();
            //        log.LogError(INTERRUPTED_ERROR_MESSAGE, fatalException);
            //        throw new InvalidOperationException(INTERRUPTED_ERROR_MESSAGE, fatalException);
            //    }
            //    catch (ExecutionException couldNotDescribeTopicException)
            //    {
            //        Exception cause = couldNotDescribeTopicException.getCause();
            //        if (cause is UnknownTopicOrPartitionException ||
            //            cause is LeaderNotAvailableException)
            //        {
            //            // This topic didn't exist or leader is not known yet, proceed to try to create it
            //            log.LogDebug("Topic {} is unknown or not found, hence not existed yet.", topicName);
            //        }
            //        else
            //        {

            //            log.LogError("Unexpected error during topic description for {}.\n" +
            //                "Error message was: {}", topicName, cause.ToString());
            //            throw new StreamsException(string.Format("Could not create topic %s.", topicName), cause);
            //        }
            //    }
            //}

            //return existedTopicPartition;
            return null;
        }

        /**
         * Check the existing topics to have correct number of partitions; and return the remaining topics that needs to be created
         */
        private HashSet<string> ValidateTopics(
            HashSet<string> topicsToValidate,
            Dictionary<string, InternalTopicConfig> topicsMap)
        {

            Dictionary<string, int> existedTopicPartition = this.GetNumPartitions(topicsToValidate);

            var topicsToCreate = new HashSet<string>();
            foreach (KeyValuePair<string, InternalTopicConfig> entry in topicsMap)
            {
                //string topicName = entry.Key;
                //int numberOfPartitions = entry.Value.numberOfPartitions();
                //if (existedTopicPartition.ContainsKey(topicName))
                //{
                //    if (!existedTopicPartition[topicName].Equals(numberOfPartitions))
                //    {
                //        string errorMsg = string.Format("Existing internal topic %s has invalid partitions: " +
                //                "expected: %d; actual: %d. " +
                //                "Use 'kafka.tools.StreamsResetter' tool to clean up invalid topics before processing.",
                //            topicName, numberOfPartitions, existedTopicPartition[topicName]);
                //        log.LogError(errorMsg);
                //        throw new StreamsException(errorMsg);
                //    }
                //}
                //else
                //{

                //    topicsToCreate.Add(topicName);
                //}
            }

            return topicsToCreate;
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
using Kafka.Common.KafkaFuture;
using Kafka.Common.errors.LeaderNotAvailableException;
using Kafka.Common.errors.TopicExistsException;
using Kafka.Common.errors.UnknownTopicOrPartitionException;
using Kafka.Common.Utils.LogContext;
using Kafka.Common.Utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class InternalTopicManager {
    private static string INTERRUPTED_ERROR_MESSAGE = "Thread got interrupted. This indicates a bug. " +
        "Please report at https://issues.apache.org/jira/projects/KAFKA or dev-mailing list (https://kafka.apache.org/contact).";

    private static class InternalAdminClientConfig : AdminClientConfig {
        private InternalAdminClientConfig(Dictionary<?, ?> props)
{
            super(props, false);
        }
    }

    private Logger log;
    private long windowChangeLogAdditionalRetention;
    private Dictionary<string, string> defaultTopicConfigs = new HashMap<>();

    private short replicationFactor;
    private Admin adminClient;

    private int retries;
    private long retryBackOffMs;

    public InternalTopicManager(Admin adminClient,
                                StreamsConfig streamsConfig)
{
        this.adminClient = adminClient;

        LogContext logContext = new LogContext(string.Format("stream-thread [%s] ", Thread.currentThread().getName())];
        log = logContext.logger(GetType());

        replicationFactor = streamsConfig.getInt(StreamsConfig.REPLICATION_FACTOR_CONFIG).shortValue();
        windowChangeLogAdditionalRetention = streamsConfig.getLong(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG);
        InternalAdminClientConfig dummyAdmin = new InternalAdminClientConfig(streamsConfig.getAdminConfigs("dummy"));
        retries = dummyAdmin.getInt(AdminClientConfig.RETRIES_CONFIG);
        retryBackOffMs = dummyAdmin.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);

        log.LogDebug("Configs:" + Utils.NL,
            "\t{} = {}" + Utils.NL,
            "\t{} = {}" + Utils.NL,
            "\t{} = {}",
            AdminClientConfig.RETRIES_CONFIG, retries,
            StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor,
            StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, windowChangeLogAdditionalRetention);

        foreach (Map.Entry<string, object> entry in streamsConfig.originalsWithPrefix(StreamsConfig.TOPIC_PREFIX).entrySet())
{
            if (entry.getValue() != null)
{
                defaultTopicConfigs.Add(entry.getKey(), entry.getValue().ToString());
            }
        }
    }

    /**
     * Prepares a set of given internal topics.
     *
     * If a topic does not exist creates a new topic.
     * If a topic with the correct number of partitions exists ignores it.
     * If a topic exists already but has different number of partitions we fail and throw exception requesting user to reset the app before restarting again.
     */
    public void makeReady(Dictionary<string, InternalTopicConfig> topics)
{
        // we will do the validation / topic-creation in a loop, until we have confirmed all topics
        // have existed with the expected number of partitions, or some create topic returns fatal errors.

        int remainingRetries = retries;
        Set<string> topicsNotReady = new HashSet<>(topics.keySet());

        while (!topicsNotReady.isEmpty() && remainingRetries >= 0)
{
            topicsNotReady = validateTopics(topicsNotReady, topics);

            if (topicsNotReady.size() > 0)
{
                Set<NewTopic> newTopics = new HashSet<>();

                foreach (string topicName in topicsNotReady)
{
                    InternalTopicConfig internalTopicConfig = Utils.notNull(topics[topicName)];
                    Dictionary<string, string> topicConfig = internalTopicConfig.getProperties(defaultTopicConfigs, windowChangeLogAdditionalRetention);

                    log.LogDebug("Going to create topic {} with {} partitions and config {}.",
                        internalTopicConfig.name(),
                        internalTopicConfig.numberOfPartitions(),
                        topicConfig);

                    newTopics.add(
                        new NewTopic(
                            internalTopicConfig.name(),
                            internalTopicConfig.numberOfPartitions(),
                            replicationFactor)
                            .configs(topicConfig));
                }

                CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);

                foreach (Map.Entry<string, KafkaFuture<Void>> createTopicResult in createTopicsResult.values().entrySet())
{
                    string topicName = createTopicResult.getKey();
                    try {
                        createTopicResult.getValue()[];
                        topicsNotReady.Remove(topicName);
                    } catch (InterruptedException fatalException)
{
                        // this should not happen; if it ever happens it indicate a bug
                        Thread.currentThread().interrupt();
                        log.LogError(INTERRUPTED_ERROR_MESSAGE, fatalException);
                        throw new InvalidOperationException(INTERRUPTED_ERROR_MESSAGE, fatalException);
                    } catch (ExecutionException executionException)
{
                        Throwable cause = executionException.getCause();
                        if (cause is TopicExistsException)
{
                            // This topic didn't exist earlier or its leader not known before; just retain it for next round of validation.
                            log.info("Could not create topic {}. Topic is probably marked for deletion (number of partitions is unknown).\n" +
                                "Will retry to create this topic in {} ms (to let broker finish async delete operation first).\n" +
                                "Error message was: {}", topicName, retryBackOffMs, cause.ToString());
                        } else {
                            log.LogError("Unexpected error during topic creation for {}.\n" +
                                "Error message was: {}", topicName, cause.ToString());
                            throw new StreamsException(string.Format("Could not create topic %s.", topicName), cause);
                        }
                    }
                }
            }


            if (!topicsNotReady.isEmpty())
{
                log.info("Topics {} can not be made ready with {} retries left", topicsNotReady, retries);

                Utils.sleep(retryBackOffMs);

                remainingRetries--;
            }
        }

        if (!topicsNotReady.isEmpty())
{
            string timeoutAndRetryError = string.Format("Could not create topics after %d retries. " +
                "This can happen if the Kafka cluster is temporary not available. " +
                "You can increase admin client config `retries` to be resilient against this error.", retries);
            log.LogError(timeoutAndRetryError);
            throw new StreamsException(timeoutAndRetryError);
        }
    }

    /**
     * Try to get the number of partitions for the given topics; return the number of partitions for topics that already exists.
     *
     * Topics that were not able to get its description will simply not be returned
     */
    // visible for testing
    protected Dictionary<string, Integer> getNumPartitions(Set<string> topics)
{
        log.LogDebug("Trying to check if topics {} have been created with expected number of partitions.", topics);

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
        Dictionary<string, KafkaFuture<TopicDescription>> futures = describeTopicsResult.values();

        Dictionary<string, Integer> existedTopicPartition = new HashMap<>();
        foreach (Map.Entry<string, KafkaFuture<TopicDescription>> topicFuture in futures.entrySet())
{
            string topicName = topicFuture.getKey();
            try {
                TopicDescription topicDescription = topicFuture.getValue()[];
                existedTopicPartition.Add(
                    topicFuture.getKey(),
                    topicDescription.partitions().size());
            } catch (InterruptedException fatalException)
{
                // this should not happen; if it ever happens it indicate a bug
                Thread.currentThread().interrupt();
                log.LogError(INTERRUPTED_ERROR_MESSAGE, fatalException);
                throw new InvalidOperationException(INTERRUPTED_ERROR_MESSAGE, fatalException);
            } catch (ExecutionException couldNotDescribeTopicException)
{
                Throwable cause = couldNotDescribeTopicException.getCause();
                if (cause is UnknownTopicOrPartitionException ||
                    cause is LeaderNotAvailableException)
{
                    // This topic didn't exist or leader is not known yet, proceed to try to create it
                    log.LogDebug("Topic {} is unknown or not found, hence not existed yet.", topicName);
                } else {
                    log.LogError("Unexpected error during topic description for {}.\n" +
                        "Error message was: {}", topicName, cause.ToString());
                    throw new StreamsException(string.Format("Could not create topic %s.", topicName), cause);
                }
            }
        }

        return existedTopicPartition;
    }

    /**
     * Check the existing topics to have correct number of partitions; and return the remaining topics that needs to be created
     */
    private Set<string> validateTopics(Set<string> topicsToValidate,
                                       Dictionary<string, InternalTopicConfig> topicsMap)
{

        Dictionary<string, Integer> existedTopicPartition = getNumPartitions(topicsToValidate);

        Set<string> topicsToCreate = new HashSet<>();
        foreach (Map.Entry<string, InternalTopicConfig> entry in topicsMap.entrySet())
{
            string topicName = entry.getKey();
            int numberOfPartitions = entry.getValue().numberOfPartitions();
            if (existedTopicPartition.ContainsKey(topicName))
{
                if (!existedTopicPartition[topicName).Equals(numberOfPartitions))
{
                    string errorMsg = string.Format("Existing internal topic %s has invalid partitions: " +
                            "expected: %d; actual: %d. " +
                            "Use 'kafka.tools.StreamsResetter' tool to clean up invalid topics before processing.",
                        topicName, numberOfPartitions, existedTopicPartition[topicName)];
                    log.LogError(errorMsg);
                    throw new StreamsException(errorMsg);
                }
            } else {
                topicsToCreate.add(topicName);
            }
        }

        return topicsToCreate;
    }
}

using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.State;
using System;

namespace Kafka.Streams
{
    public class DelegatingStateRestoreListener : IStateRestoreListener
    {
        private void throwOnFatalException(
            Exception fatalUserException,
            TopicPartition topicPartition,
            string storeName)
        {
            throw new StreamsException(
                    string.Format("Fatal user code error in store restore listener for store %s, partition %s.",
                            storeName,
                            topicPartition),
                    fatalUserException);
        }

        public void onRestoreStart(
            TopicPartition topicPartition,
            string storeName,
            long startingOffset,
            long endingOffset)
        {
            //if (globalStateRestoreListener != null)
            //{
            //    try
            //    {
            //        globalStateRestoreListener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
            //    }
            //    catch (Exception fatalUserException)
            //    {
            //        throwOnFatalException(fatalUserException, topicPartition, storeName);
            //    }
            //}
        }

        public void onBatchRestored(
            TopicPartition topicPartition,
            string storeName,
            long batchEndOffset,
            long numRestored)
        {
            //if (globalStateRestoreListener != null)
            //{
            //    try
            //    {
            //        globalStateRestoreListener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
            //    }
            //    catch (Exception fatalUserException)
            //    {
            //        throwOnFatalException(fatalUserException, topicPartition, storeName);
            //    }
            //}
        }

        public void onRestoreEnd(TopicPartition topicPartition, string storeName, long totalRestored)
        {
            //if (globalStateRestoreListener != null)
            //{
            //    try
            //    {
            //        globalStateRestoreListener.onRestoreEnd(topicPartition, storeName, totalRestored);
            //    }
            //    catch (Exception fatalUserException)
            //    {
            //        throwOnFatalException(fatalUserException, topicPartition, storeName);
            //    }
            //}
        }
    }
}

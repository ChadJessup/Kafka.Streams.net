using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Interfaces
{
    /**
     * The interface for wrapping a serializer and deserializer for the given data type.
     *
     * @param Type to be serialized from and deserialized into.
     *
     * A that : this interface is expected to have a constructor with no parameter.
     */
    public interface ISerde<T> : IDisposable
    {

        /**
         * Configure this, which will configure the underlying serializer and deserializer.
         *
         * @param configs configs in key/value pairs
         * @param isKey whether is for key or value
         */
        void Configure(Dictionary<string, object> configs, bool isKey);

        /**
         * Close this serde, which will close the underlying serializer and deserializer.
         * <p>
         * This method has to be idempotent because it might be called multiple times.
         */
        void Close();

        ISerializer<T> Serializer();

        IDeserializer<T> Deserializer();
    }
}
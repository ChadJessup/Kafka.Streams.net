using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State.KeyValues
{
    /**
     * A store supplier that can be used to create one or more {@link KeyValueStore KeyValueStore&lt;Byte, byte[]&gt;} instances of type &lt;Byte, byte[]&gt;.
     *
     * For any stores implementing the {@link KeyValueStore KeyValueStore&lt;Byte, byte[]&gt;} interface, null value bytes are considered as "not exist". This means:
     *
     * 1. Null value bytes in put operations should be treated as delete.
     * 2. If the key does not exist, get operations should return null value bytes.
     */
    public interface IKeyValueBytesStoreSupplier : IStoreSupplier<IKeyValueStore<Bytes, byte[]>>
    {
    }
}
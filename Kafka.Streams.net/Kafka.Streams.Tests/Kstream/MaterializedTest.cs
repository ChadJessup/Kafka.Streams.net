using Kafka.Streams.Errors;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.Windowed;
using System;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class MaterializedTest
    {
        [Fact]
        public void ShouldAllowValidTopicNamesAsStoreName()
        {
            Materialized.As<string, string>("valid-name");
            Materialized.As<string, string>("valid.name");
            Materialized.As<string, string>("valid_name");
        }

        [Fact]
        public void ShouldNotAllowInvalidTopicNames()
        {
            Assert.Throws<TopologyException>(() => Materialized.As<string, string>("not:valid"));
        }

#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.

        [Fact]
        public void ShouldThrowNullPointerIfWindowBytesStoreSupplierIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => Materialized.As<string, string, IWindowStore<Bytes, byte[]>>(null));
        }

        [Fact]
        public void ShouldThrowNullPointerIfKeyValueBytesStoreSupplierIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>(null));
        }

        [Fact]
        public void ShouldThrowNullPointerIfSessionBytesStoreSupplierIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => Materialized.As<string, string, ISessionStore<Bytes, byte[]>>(null));
        }

#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
    }
}

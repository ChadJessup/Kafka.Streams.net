//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Internals;
//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */















//    public class BufferValueTest
//    {
//        [Fact]
//        public void ShouldDeduplicateNullValues()
//        {
//            BufferValue bufferValue = new BufferValue(null, null, null, null);
//            Assert.Same(bufferValue.priorValue(), bufferValue.oldValue());
//        }

//        [Fact]
//        public void ShouldDeduplicateIndenticalValues()
//        {
//            byte[] bytes = { (byte)0 };
//            BufferValue bufferValue = new BufferValue(bytes, bytes, null, null);
//            Assert.Same(bufferValue.priorValue(), bufferValue.oldValue());
//        }

//        [Fact]
//        public void ShouldDeduplicateEqualValues()
//        {
//            BufferValue bufferValue = new BufferValue(new byte[] { (byte)0 }, new byte[] { (byte)0 }, null, null);
//            Assert.Same(bufferValue.priorValue(), bufferValue.oldValue());
//        }

//        [Fact]
//        public void ShouldStoreDifferentValues()
//        {
//            byte[] priorValue = { (byte)0 };
//            byte[] oldValue = { (byte)1 };
//            BufferValue bufferValue = new BufferValue(priorValue, oldValue, null, null);
//            Assert.Same(priorValue, bufferValue.priorValue());
//            Assert.Same(oldValue, bufferValue.oldValue());
//            Assert.NotEqual(bufferValue.priorValue(), bufferValue.oldValue());
//        }

//        [Fact]
//        public void ShouldStoreDifferentValuesWithPriorNull()
//        {
//            byte[] priorValue = null;
//            byte[] oldValue = { (byte)1 };
//            BufferValue bufferValue = new BufferValue(priorValue, oldValue, null, null);
//            Assert.Null(bufferValue.priorValue());
//            Assert.Same(oldValue, bufferValue.oldValue());
//            Assert.NotEqual(bufferValue.priorValue(), bufferValue.oldValue());
//        }

//        [Fact]
//        public void ShouldStoreDifferentValuesWithOldNull()
//        {
//            byte[] priorValue = { (byte)0 };
//            byte[] oldValue = null;
//            BufferValue bufferValue = new BufferValue(priorValue, oldValue, null, null);
//            Assert.Same(priorValue, bufferValue.priorValue());
//            Assert.Null(bufferValue.oldValue());
//            Assert.NotEqual(bufferValue.priorValue(), bufferValue.oldValue());
//        }

//        [Fact]
//        public void ShouldAccountForDeduplicationInSizeEstimate()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            Assert.Equal(25L, new BufferValue(null, null, null, context).ResidentMemorySizeEstimate());
//            Assert.Equal(26L, new BufferValue(new byte[] { (byte)0 }, null, null, context).ResidentMemorySizeEstimate());
//            Assert.Equal(26L, new BufferValue(null, new byte[] { (byte)0 }, null, context).ResidentMemorySizeEstimate());
//            Assert.Equal(26L, new BufferValue(new byte[] { (byte)0 }, new byte[] { (byte)0 }, null, context).ResidentMemorySizeEstimate());
//            Assert.Equal(27L, new BufferValue(new byte[] { (byte)0 }, new byte[] { (byte)1 }, null, context).ResidentMemorySizeEstimate());

//            // new value should get counted, but doesn't get deduplicated
//            Assert.Equal(28L, new BufferValue(new byte[] { (byte)0 }, new byte[] { (byte)1 }, new byte[] { (byte)0 }, context).ResidentMemorySizeEstimate());
//        }

//        [Fact]
//        public void ShouldSerializeNulls()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            byte[] serializedContext = context.Serialize();
//            byte[] bytes = new BufferValue(null, null, null, context).Serialize(0).array();
//            byte[] withoutContext = Array.copyOfRange(bytes, serializedContext.Length, bytes.Length);

//            Assert.Equal(withoutContext, new ByteBuffer().Allocate(sizeof(int) * 3).putInt(-1).putInt(-1).putInt(-1).array());
//        }

//        [Fact]
//        public void ShouldSerializePrior()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            byte[] serializedContext = context.Serialize();
//            byte[] priorValue = { (byte)5 };
//            byte[] bytes = new BufferValue(priorValue, null, null, context).Serialize(0).array();
//            byte[] withoutContext = Array.copyOfRange(bytes, serializedContext.Length, bytes.Length);

//            Assert.Equal(withoutContext, new ByteBuffer().Allocate(sizeof(int) * 3 + 1).putInt(1).Put(priorValue).putInt(-1).putInt(-1).array());
//        }

//        [Fact]
//        public void ShouldSerializeOld()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            byte[] serializedContext = context.Serialize();
//            byte[] oldValue = { (byte)5 };
//            byte[] bytes = new BufferValue(null, oldValue, null, context).Serialize(0).array();
//            byte[] withoutContext = new byte[5];

//            Array.Copy(bytes, withoutContext, bytes.Length);

//            Assert.Equal(withoutContext, new ByteBuffer().Allocate(sizeof(int) * 3 + 1).putInt(-1).putInt(1).Put(oldValue).putInt(-1).array());
//        }

//        [Fact]
//        public void ShouldSerializeNew()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            byte[] serializedContext = context.Serialize();
//            byte[] newValue = { (byte)5 };
//            byte[] bytes = new BufferValue(null, null, newValue, context).Serialize(0).array();
//            byte[] withoutContext = Array.copyOfRange(bytes, serializedContext.Length, bytes.Length);

//            Assert.Equal(withoutContext, new ByteBuffer().Allocate(sizeof(int) * 3 + 1).putInt(-1).putInt(-1).putInt(1).Put(newValue).array());
//        }

//        [Fact]
//        public void ShouldCompactDuplicates()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            byte[] serializedContext = context.Serialize();
//            byte[] duplicate = { (byte)5 };
//            byte[] bytes = new BufferValue(duplicate, duplicate, null, context).Serialize(0).array();
//            byte[] withoutContext = Array.copyOfRange(bytes, serializedContext.Length, bytes.Length);

//            Assert.Equal(withoutContext, new ByteBuffer().Allocate(sizeof(int) * 3 + 1).putInt(1).Put(duplicate).putInt(-2).putInt(-1).array());
//        }

//        [Fact]
//        public void ShouldDeserializePrior()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            byte[] serializedContext = context.Serialize();
//            byte[] priorValue = { (byte)5 };
//            ByteBuffer serialValue =
//                new ByteBuffer()
//                    .Allocate(serializedContext.Length + sizeof(int) * 3 + priorValue.Length)
//                    .Add(serializedContext).putInt(1).Put(priorValue).putInt(-1).putInt(-1);
//            serialValue.Position(0);

//            BufferValue deserialize = BufferValue.Deserialize(serialValue);
//            Assert.Equal(deserialize, new BufferValue(priorValue, null, null, context));
//        }

//        [Fact]
//        public void ShouldDeserializeOld()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            byte[] serializedContext = context.Serialize();
//            byte[] oldValue = { (byte)5 };
//            ByteBuffer serialValue =
//                new ByteBuffer()
//                    .Allocate(serializedContext.Length + sizeof(int) * 3 + oldValue.Length)
//                    .Add(serializedContext).putInt(-1).putInt(1).Put(oldValue).putInt(-1);
//            serialValue.position(0);

//            Assert.Equal(BufferValue.Deserialize(serialValue), new BufferValue(null, oldValue, null, context));
//        }

//        [Fact]
//        public void ShouldDeserializeNew()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            byte[] serializedContext = context.Serialize();
//            byte[] newValue = { (byte)5 };
//            ByteBuffer serialValue =
//                new ByteBuffer()
//                    .Allocate(serializedContext.Length + sizeof(int) * 3 + newValue.Length)
//                    .Add(serializedContext).putInt(-1).putInt(-1).putInt(1).Put(newValue);
//            serialValue.position(0);

//            Assert.Equal(BufferValue.Deserialize(serialValue), new BufferValue(null, null, newValue, context));
//        }

//        [Fact]
//        public void ShouldDeserializeCompactedDuplicates()
//        {
//            ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
//            byte[] serializedContext = context.Serialize();
//            byte[] duplicate = { (byte)5 };
//            ByteBuffer serialValue =
//                new ByteBuffer()
//                    .Allocate(serializedContext.Length + sizeof(int) * 3 + duplicate.Length)
//                    .Add(serializedContext).putInt(1).Put(duplicate).putInt(-2).putInt(-1);
//            serialValue.position(0);

//            BufferValue bufferValue = BufferValue.Deserialize(serialValue);
//            Assert.Equal(bufferValue, new BufferValue(duplicate, duplicate, null, context));
//            Assert.Same(bufferValue.priorValue(), bufferValue.oldValue());
//        }
//    }
//}

using System;
using System.Collections.Generic;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Class used to represent a {@link IProcessorSupplier} and the name
     * used to register it with the {@link org.apache.kafka.streams.processor.Internals.InternalTopologyBuilder}
     *
     * Used by the Join nodes as there are several parameters, this abstraction helps
     * keep the number of arguments more reasonable.
     */
    public class ProcessorParameters<K, V> : IConvertible
    {
        public IProcessorSupplier<K, V> ProcessorSupplier { get; }
        public string processorName { get; }

        public ProcessorParameters(
            IProcessorSupplier<K, V> processorSupplier,
            string processorName)
        {
            this.ProcessorSupplier = processorSupplier;
            this.processorName = processorName;
        }

        public override string ToString()
            => $"ProcessorParameters{{processor={ProcessorSupplier.GetType()}, " +
            $"processor name='{processorName}'}}";

        public TypeCode GetTypeCode()
        {
            throw new NotImplementedException();
        }

        public bool ToBoolean(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public byte ToByte(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public char ToChar(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public DateTime ToDateTime(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public decimal ToDecimal(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public double ToDouble(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public short ToInt16(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public int ToInt32(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public long ToInt64(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public sbyte ToSByte(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public float ToSingle(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public string ToString(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public object ToType(Type conversionType, IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public ushort ToUInt16(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public uint ToUInt32(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }

        public ulong ToUInt64(IFormatProvider provider)
        {
            throw new NotImplementedException();
        }
    }
}

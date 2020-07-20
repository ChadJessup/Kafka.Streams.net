using System;
using System.Collections.Generic;
using Kafka.Streams.Tasks;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    internal class SubscriptionInfoData
    {
        private ByteBufferAccessor accessor;
        private short version1;

        public static object SCHEMAS { get; internal set; }

        public SubscriptionInfoData()
        {
        }

        public SubscriptionInfoData(ByteBufferAccessor accessor, short version1)
        {
            this.accessor = accessor;
            this.version1 = version1;
        }

        internal void setProcessId(Guid processId)
        {
            throw new NotImplementedException();
        }

        internal void setVersion(int version)
        {
            throw new NotImplementedException();
        }

        internal void setUserEndPoint(object p)
        {
            throw new NotImplementedException();
        }

        internal int version()
        {
            throw new NotImplementedException();
        }

        internal void setLatestSupportedVersion(int latestSupportedVersion)
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<TaskOffsetSum> taskOffsetSums()
        {
            throw new NotImplementedException();
        }

        internal string userEndPoint()
        {
            throw new NotImplementedException();
        }

        internal int latestSupportedVersion()
        {
            throw new NotImplementedException();
        }

        internal void setStandbyTasks(List<TaskId> lists)
        {
            throw new NotImplementedException();
        }

        internal void setPrevTasks(List<TaskId> lists)
        {
            throw new NotImplementedException();
        }

        internal Guid processId()
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<TaskId> prevTasks()
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<TaskId> standbyTasks()
        {
            throw new NotImplementedException();
        }

        internal void setTaskOffsetSums(List<TaskOffsetSum> lists)
        {
            throw new NotImplementedException();
        }

        internal void Write(ByteBufferAccessor accessor, ObjectSerializationCache cache, short v)
        {
            throw new NotImplementedException();
        }

        internal int Size(ObjectSerializationCache cache, short v)
        {
            throw new NotImplementedException();
        }
    }
}

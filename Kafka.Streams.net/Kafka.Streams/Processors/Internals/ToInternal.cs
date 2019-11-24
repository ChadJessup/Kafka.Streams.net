namespace Kafka.Streams.Processors.Internals
{
    public class ToInternal : To
    {

        public ToInternal()
            : base(To.All())
        {
        }

        public override void Update(To to)
        {
            base.Update(to);
        }

        public bool hasTimestamp()
        {
            return Timestamp != -1;
        }

        public string child()
        {
            return childName;
        }
    }
}
namespace Kafka.Streams.State.Internals
{
    internal class SimpleTimeZone
    {
        private readonly int v1;
        private readonly string v2;

        public SimpleTimeZone(int v1, string v2)
        {
            this.v1 = v1;
            this.v2 = v2;
        }
    }
}
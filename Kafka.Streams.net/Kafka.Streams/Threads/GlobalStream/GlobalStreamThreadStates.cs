namespace Kafka.Streams.Threads.GlobalStream
{
    public enum GlobalStreamThreadStates
    {
        Unknown = 0,
        CREATED, //(1, 2)
        RUNNING, //(2)
        PENDING_SHUTDOWN, //(3)
        DEAD
    }
}

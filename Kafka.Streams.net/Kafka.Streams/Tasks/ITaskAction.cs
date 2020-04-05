namespace Kafka.Streams.Tasks
{
    public interface ITaskAction<T>
        where T : ITask
    {
        string name { get; }

        void Apply(T task);
    }
}
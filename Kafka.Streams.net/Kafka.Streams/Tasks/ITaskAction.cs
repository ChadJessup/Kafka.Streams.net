namespace Kafka.Streams.Tasks
{
    public interface ITaskAction<T>
        where T : ITask
    {
        string Name { get; }

        void Apply(T task);
    }
}






















namespace Kafka.Streams.State
{

    public abstract class QueryableStoreTypeMatcher<T> : IQueryableStoreType<T>
    {

        private HashSet<Class> matchTo;

        QueryableStoreTypeMatcher(HashSet<Class> matchTo)
        {
            this.matchTo = matchTo;
        }



        public bool accepts(IStateStore stateStore)
        {
            foreach (Class matchToClass in matchTo)
            {
                if (!matchToClass.isAssignableFrom(stateStore.GetType()))
                {
                    return false;
                }
            }
            return true;
        }
    }
}

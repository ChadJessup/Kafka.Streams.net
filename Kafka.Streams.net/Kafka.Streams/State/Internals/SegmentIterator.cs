
//    public class SegmentIterator<S> : IKeyValueIterator<Bytes, byte[]>
//            where S : ISegment
//    {

//        private Bytes from;
//        private Bytes to;
//        protected IEnumerator<S> segments;
//        protected HasNextCondition hasNextCondition;

//        private S currentSegment;
//        IKeyValueIterator<Bytes, byte[]> currentIterator;

//        SegmentIterator(IEnumerator<S> segments,
//                        HasNextCondition hasNextCondition,
//                        Bytes from,
//                        Bytes to)
//        {
//            this.segments = segments;
//            this.hasNextCondition = hasNextCondition;
//            this.from = from;
//            this.to = to;
//        }

//        public void Close()
//        {
//            if (currentIterator != null)
//            {
//                currentIterator.Close();
//                currentIterator = null;
//            }
//        }

//        public override Bytes PeekNextKey()
//        {
//            if (!HasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return currentIterator.PeekNextKey();
//        }

//        public override bool HasNext()
//        {
//            bool hasNext = false;
//            while ((currentIterator == null || !(hasNext = hasNextConditionHasNext()) || !currentSegment.IsOpen())
//                    && segments.MoveNext())
//            {
//                Close();
//                currentSegment = segments.MoveNext();
//                try
//                {
//                    if (from == null || to == null)
//                    {
//                        currentIterator = currentSegment.All();
//                    }
//                    else
//                    {
//                        currentIterator = currentSegment.Range(from, to);
//                    }
//                }
//                catch (InvalidStateStoreException e)
//                {
//                    // segment may have been closed so we ignore it.
//                }
//            }
//            return currentIterator != null && hasNext;
//        }

//        private bool hasNextConditionHasNext()
//        {
//            bool hasNext = false;
//            try
//            {
//                hasNext = hasNextCondition.hasNext(currentIterator);
//            }
//            catch (InvalidStateStoreException e)
//            {
//                //already closed so ignore
//            }
//            return hasNext;
//        }

//        public KeyValuePair<Bytes, byte[]> next()
//        {
//            if (!HasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return currentIterator.MoveNext();
//        }

//        public void Remove()
//        {
//            throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
//        }
//    }
//}
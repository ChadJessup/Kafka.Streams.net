
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

//        public void close()
//        {
//            if (currentIterator != null)
//            {
//                currentIterator.close();
//                currentIterator = null;
//            }
//        }

//        public override Bytes peekNextKey()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return currentIterator.peekNextKey();
//        }

//        public override bool hasNext()
//        {
//            bool hasNext = false;
//            while ((currentIterator == null || !(hasNext = hasNextConditionHasNext()) || !currentSegment.isOpen())
//                    && segments.hasNext())
//            {
//                close();
//                currentSegment = segments.next();
//                try
//                {
//                    if (from == null || to == null)
//                    {
//                        currentIterator = currentSegment.all();
//                    }
//                    else
//                    {
//                        currentIterator = currentSegment.range(from, to);
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
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return currentIterator.next();
//        }

//        public void Remove()
//        {
//            throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
//        }
//    }
//}
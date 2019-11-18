
//    public class TaskMigratedException : StreamsException
//    {


//        private static long serialVersionUID = 1L;

//        private ITask task;
//        private ProducerFencedException fatal;

//        // this is for unit test only
//        public TaskMigratedException()
//            : base("A task has been migrated unexpectedly", null)
//        {

//            this.task = null;
//        }

//        public TaskMigratedException(ITask task,
//                                      TopicPartition topicPartition,
//                                      long endOffset,
//                                      long pos)
//            : base(string.Format("Log end offset of %s should not change while restoring: old end offset %d, current offset %d",
//                                topicPartition,
//                                endOffset,
//                                pos),
//                null)
//        {

//            this.task = task;
//        }

//        public TaskMigratedException(ITask task)
//            : base(string.Format("Task %s is unexpectedly closed during processing", task.id), null)
//        {

//            this.task = task;
//        }

//        public TaskMigratedException(ITask task,
//                                      Exception throwable)
//            : base(string.Format("Client request for task %s has been fenced due to a rebalance", task.id), throwable)
//        {

//            this.task = task;
//        }

//        public TaskMigratedException(ITask task, ProducerFencedException fatal) : this(task)
//        {
//            this.fatal = fatal;
//        }

//        public ITask migratedTask()
//        {
//            return task;
//        }

//    }
//}
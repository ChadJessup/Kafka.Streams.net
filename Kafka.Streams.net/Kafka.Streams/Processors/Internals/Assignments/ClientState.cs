
//using System.Collections.Generic;

//namespace Kafka.Streams.Processors.Internals.Assignments
//{
//    public class ClientState
//    {

//        private HashSet<TaskId> activeTasks;
//        private HashSet<TaskId> standbyTasks;
//        private HashSet<TaskId> assignedTasks;
//        private HashSet<TaskId> prevActiveTasks;
//        private HashSet<TaskId> prevStandbyTasks;
//        private HashSet<TaskId> prevAssignedTasks;

//        private int capacity;


//        public ClientState()
//        {
//            this(0);
//        }

//        ClientState(int capacity)
//        {
//            this(new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), capacity);
//        }

//        private ClientState(HashSet<TaskId> activeTasks,
//                            HashSet<TaskId> standbyTasks,
//                            HashSet<TaskId> assignedTasks,
//                            HashSet<TaskId> prevActiveTasks,
//                            HashSet<TaskId> prevStandbyTasks,
//                            HashSet<TaskId> prevAssignedTasks,
//                            int capacity)
//        {
//            this.activeTasks = activeTasks;
//            this.standbyTasks = standbyTasks;
//            this.assignedTasks = assignedTasks;
//            this.prevActiveTasks = prevActiveTasks;
//            this.prevStandbyTasks = prevStandbyTasks;
//            this.prevAssignedTasks = prevAssignedTasks;
//            this.capacity = capacity;
//        }

//        public ClientState copy()
//        {
//            return new ClientState(
//                new HashSet<>(activeTasks),
//                new HashSet<>(standbyTasks),
//                new HashSet<>(assignedTasks),
//                new HashSet<>(prevActiveTasks),
//                new HashSet<>(prevStandbyTasks),
//                new HashSet<>(prevAssignedTasks),
//                capacity);
//        }

//        public void assign(TaskId taskId, bool active)
//        {
//            if (active)
//            {
//                activeTasks.Add(taskId);
//            }
//            else
//            {

//                standbyTasks.Add(taskId);
//            }

//            assignedTasks.Add(taskId);
//        }

//        public HashSet<TaskId> activeTasks()
//        {
//            return activeTasks;
//        }

//        public HashSet<TaskId> standbyTasks()
//        {
//            return standbyTasks;
//        }

//        public HashSet<TaskId> prevActiveTasks()
//        {
//            return prevActiveTasks;
//        }

//        public HashSet<TaskId> prevStandbyTasks()
//        {
//            return prevStandbyTasks;
//        }


//        public int assignedTaskCount()
//        {
//            return assignedTasks.size();
//        }

//        public void incrementCapacity()
//        {
//            capacity++;
//        }


//        public int activeTaskCount()
//        {
//            return activeTasks.size();
//        }

//        public void addPreviousActiveTasks(HashSet<TaskId> prevTasks)
//        {
//            prevActiveTasks.AddAll(prevTasks);
//            prevAssignedTasks.AddAll(prevTasks);
//        }

//        public void addPreviousStandbyTasks(HashSet<TaskId> standbyTasks)
//        {
//            prevStandbyTasks.AddAll(standbyTasks);
//            prevAssignedTasks.AddAll(standbyTasks);
//        }


//        public string ToString()
//        {
//            return "[activeTasks: (" + activeTasks +
//                    ") standbyTasks: (" + standbyTasks +
//                    ") assignedTasks: (" + assignedTasks +
//                    ") prevActiveTasks: (" + prevActiveTasks +
//                    ") prevStandbyTasks: (" + prevStandbyTasks +
//                    ") prevAssignedTasks: (" + prevAssignedTasks +
//                    ") capacity: " + capacity +
//                    "]";
//        }

//        bool reachedCapacity()
//        {
//            return assignedTasks.size() >= capacity;
//        }

//        bool hasMoreAvailableCapacityThan(ClientState other)
//        {
//            if (this.capacity <= 0)
//            {
//                throw new InvalidOperationException("Capacity of this ClientState must be greater than 0.");
//            }

//            if (other.capacity <= 0)
//            {
//                throw new InvalidOperationException("Capacity of other ClientState must be greater than 0");
//            }

//            double otherLoad = (double)other.assignedTaskCount() / other.capacity;
//            double thisLoad = (double)assignedTaskCount() / capacity;

//            if (thisLoad < otherLoad)
//            {
//                return true;
//            }
//            else if (thisLoad > otherLoad)
//            {
//                return false;
//            }
//            else
//            {

//                return capacity > other.capacity;
//            }
//        }

//        HashSet<TaskId> previousStandbyTasks()
//        {
//            HashSet<TaskId> standby = new HashSet<>(prevAssignedTasks);
//            standby.removeAll(prevActiveTasks);
//            return standby;
//        }

//        HashSet<TaskId> previousActiveTasks()
//        {
//            return prevActiveTasks;
//        }

//        bool hasAssignedTask(TaskId taskId)
//        {
//            return assignedTasks.Contains(taskId);
//        }

//        // Visible for testing
//        HashSet<TaskId> assignedTasks()
//        {
//            return assignedTasks;
//        }

//        HashSet<TaskId> previousAssignedTasks()
//        {
//            return prevAssignedTasks;
//        }

//        int capacity()
//        {
//            return capacity;
//        }

//        bool hasUnfulfilledQuota(int tasksPerThread)
//        {
//            return activeTasks.size() < capacity * tasksPerThread;
//        }
//    }
//}
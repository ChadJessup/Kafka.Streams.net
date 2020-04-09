
//using Kafka.Streams.Processors.Internals.assignment;
//using Microsoft.Extensions.Logging;
//using System.Collections.Generic;

//namespace Kafka.Streams.Processors.Internals.Assignments
//{
//    public class StickyTaskAssignor<ID> : TaskAssignor<ID, TaskId>
//    {
//        private static ILogger log = new LoggerFactory().CreateLogger<StickyTaskAssignor>();
//        private Dictionary<ID, ClientState> clients;
//        private HashSet<TaskId> taskIds;
//        private Dictionary<TaskId, ID> previousActiveTaskAssignment = new Dictionary<>();
//        private Dictionary<TaskId, HashSet<ID>> previousStandbyTaskAssignment = new Dictionary<>();
//        private TaskPairs taskPairs;

//        public StickyTaskAssignor(Dictionary<ID, ClientState> clients, HashSet<TaskId> taskIds)
//        {
//            this.clients = clients;
//            this.taskIds = taskIds;
//            taskPairs = new TaskPairs(taskIds.size() * (taskIds.size() - 1) / 2);
//            mapPreviousTaskAssignment(clients);
//        }


//        public void assign(int numStandbyReplicas)
//        {
//            assignActive();
//            assignStandby(numStandbyReplicas);
//        }

//        private void assignStandby(int numStandbyReplicas)
//        {
//            foreach (TaskId taskId in taskIds)
//            {
//                for (int i = 0; i < numStandbyReplicas; i++)
//                {
//                    HashSet<ID> ids = findClientsWithoutAssignedTask(taskId);
//                    if (ids.isEmpty())
//                    {
//                        log.LogWarning("Unable to assign {} of {} standby tasks for task [{}]. " +
//                                         "There is not enough available capacity. You should " +
//                                         "increase the number of threads and/or application instances " +
//                                         "to maintain the requested number of standby replicas.",
//                                 numStandbyReplicas - i,
//                                 numStandbyReplicas, taskId);
//                        break;
//                    }
//                    allocateTaskWithClientCandidates(taskId, ids, false);
//                }
//            }
//        }

//        private void assignActive()
//        {
//            int totalCapacity = sumCapacity(clients.Values);
//            int tasksPerThread = taskIds.size() / totalCapacity;
//            HashSet<TaskId> assigned = new HashSet<>();

//            // first try and re-assign existing active tasks to clients that previously had
//            // the same active task
//            foreach (KeyValuePair<TaskId, ID> entry in previousActiveTaskAssignment)
//            {
//                TaskId taskId = entry.Key;
//                if (taskIds.Contains(taskId))
//                {
//                    ClientState client = clients[entry.Value];
//                    if (client.hasUnfulfilledQuota(tasksPerThread))
//                    {
//                        assignTaskToClient(assigned, taskId, client);
//                    }
//                }
//            }

//            HashSet<TaskId> unassigned = new HashSet<TaskId>(taskIds);
//            unassigned.removeAll(assigned);

//            // try and assign any remaining unassigned tasks to clients that previously
//            // have seen the task.
//            for (IEnumerator<TaskId> iterator = unassigned.iterator(); iterator.HasNext();)
//            {
//                TaskId taskId = iterator.MoveNext();
//                HashSet<ID> clientIds = previousStandbyTaskAssignment[taskId];
//                if (clientIds != null)
//                {
//                    foreach (ID clientId in clientIds)
//                    {
//                        ClientState client = clients[clientId];
//                        if (client.hasUnfulfilledQuota(tasksPerThread))
//                        {
//                            assignTaskToClient(assigned, taskId, client);
//                            iterator.Remove();
//                            break;
//                        }
//                    }
//                }
//            }

//            // assign any remaining unassigned tasks
//            List<TaskId> sortedTasks = new List<TaskId>(unassigned);
//            sortedTasks.Sort();

//            foreach (TaskId taskId in sortedTasks)
//            {
//                allocateTaskWithClientCandidates(taskId, clients.Keys, true);
//            }
//        }

//        private void allocateTaskWithClientCandidates(TaskId taskId, HashSet<ID> clientsWithin, bool active)
//        {
//            ClientState client = findClient(taskId, clientsWithin);
//            taskPairs.AddPairs(taskId, client.assignedTasks());
//            client.assign(taskId, active);
//        }

//        private void assignTaskToClient(HashSet<TaskId> assigned, TaskId taskId, ClientState client)
//        {
//            taskPairs.AddPairs(taskId, client.assignedTasks());
//            client.assign(taskId, true);
//            assigned.Add(taskId);
//        }

//        private HashSet<ID> findClientsWithoutAssignedTask(TaskId taskId)
//        {
//            HashSet<ID> clientIds = new HashSet<>();
//            foreach (KeyValuePair<ID, ClientState> client in clients)
//            {
//                if (!client.Value.hasAssignedTask(taskId))
//                {
//                    clientIds.Add(client.Key);
//                }
//            }
//            return clientIds;
//        }


//        private ClientState findClient(TaskId taskId, HashSet<ID> clientsWithin)
//        {

//            // optimize the case where there is only 1 id to search within.
//            if (clientsWithin.size() == 1)
//            {
//                return clients[clientsWithin.iterator().MoveNext()];
//            }

//            ClientState previous = findClientsWithPreviousAssignedTask(taskId, clientsWithin);
//            if (previous == null)
//            {
//                return leastLoaded(taskId, clientsWithin);
//            }

//            if (shouldBalanceLoad(previous))
//            {
//                ClientState standby = findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
//                if (standby == null
//                        || shouldBalanceLoad(standby))
//                {
//                    return leastLoaded(taskId, clientsWithin);
//                }
//                return standby;
//            }

//            return previous;
//        }

//        private bool shouldBalanceLoad(ClientState client)
//        {
//            return client.reachedCapacity() && hasClientsWithMoreAvailableCapacity(client);
//        }

//        private bool hasClientsWithMoreAvailableCapacity(ClientState client)
//        {
//            foreach (ClientState clientState in clients.Values)
//            {
//                if (clientState.hasMoreAvailableCapacityThan(client))
//                {
//                    return true;
//                }
//            }
//            return false;
//        }

//        private ClientState findClientsWithPreviousAssignedTask(TaskId taskId,
//                                                                        HashSet<ID> clientsWithin)
//        {
//            ID previous = previousActiveTaskAssignment[taskId];
//            if (previous != null && clientsWithin.Contains(previous))
//            {
//                return clients[previous];
//            }
//            return findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
//        }

//        private ClientState findLeastLoadedClientWithPreviousStandByTask(TaskId taskId, HashSet<ID> clientsWithin)
//        {
//            HashSet<ID> ids = previousStandbyTaskAssignment[taskId];
//            if (ids == null)
//            {
//                return null;
//            }
//            HashSet<ID> constrainTo = new HashSet<>(ids);
//            constrainTo.retainAll(clientsWithin);
//            return leastLoaded(taskId, constrainTo);
//        }

//        private ClientState leastLoaded(TaskId taskId, HashSet<ID> clientIds)
//        {
//            ClientState leastLoaded = findLeastLoaded(taskId, clientIds, true);
//            if (leastLoaded == null)
//            {
//                return findLeastLoaded(taskId, clientIds, false);
//            }
//            return leastLoaded;
//        }

//        private ClientState findLeastLoaded(TaskId taskId,
//                                            HashSet<ID> clientIds,
//                                            bool checkTaskPairs)
//        {
//            ClientState leastLoaded = null;
//            foreach (ID id in clientIds)
//            {
//                ClientState client = clients[id];
//                if (client.assignedTaskCount() == 0)
//                {
//                    return client;
//                }

//                if (leastLoaded == null || client.hasMoreAvailableCapacityThan(leastLoaded))
//                {
//                    if (!checkTaskPairs)
//                    {
//                        leastLoaded = client;
//                    }
//                    else if (taskPairs.hasNewPair(taskId, client.assignedTasks()))
//                    {
//                        leastLoaded = client;
//                    }
//                }

//            }
//            return leastLoaded;

//        }

//        private void mapPreviousTaskAssignment(Dictionary<ID, ClientState> clients)
//        {
//            foreach (KeyValuePair<ID, ClientState> clientState in clients)
//            {
//                foreach (TaskId activeTask in clientState.Value.previousActiveTasks())
//                {
//                    previousActiveTaskAssignment.Add(activeTask, clientState.Key);
//                }

//                foreach (TaskId prevAssignedTask in clientState.Value.previousStandbyTasks())
//                {
//                    if (!previousStandbyTaskAssignment.ContainsKey(prevAssignedTask))
//                    {
//                        previousStandbyTaskAssignment.Add(prevAssignedTask, new HashSet<>());
//                    }
//                    previousStandbyTaskAssignment[prevAssignedTask].Add(clientState.Key);
//                }
//            }

//        }

//        private int sumCapacity(List<ClientState> values)
//        {
//            int capacity = 0;
//            foreach (ClientState client in values)
//            {
//                capacity += client.capacity();
//            }
//            return capacity;
//        }
//    }
//}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class StickyTaskAssignor<ID> : TaskAssignor<ID, TaskId> {

    private static Logger log = LoggerFactory.getLogger(StickyTaskAssignor.class);
    private Dictionary<ID, ClientState> clients;
    private Set<TaskId> taskIds;
    private Dictionary<TaskId, ID> previousActiveTaskAssignment = new HashMap<>();
    private Dictionary<TaskId, Set<ID>> previousStandbyTaskAssignment = new HashMap<>();
    private TaskPairs taskPairs;

    public StickyTaskAssignor(Dictionary<ID, ClientState> clients, Set<TaskId> taskIds)
{
        this.clients = clients;
        this.taskIds = taskIds;
        taskPairs = new TaskPairs(taskIds.size() * (taskIds.size() - 1) / 2);
        mapPreviousTaskAssignment(clients);
    }

    
    public void assign(int numStandbyReplicas)
{
        assignActive();
        assignStandby(numStandbyReplicas);
    }

    private void assignStandby(int numStandbyReplicas)
{
        foreach (TaskId taskId in taskIds)
{
            for (int i = 0; i < numStandbyReplicas; i++)
{
                Set<ID> ids = findClientsWithoutAssignedTask(taskId);
                if (ids.isEmpty())
{
                    log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                                     "There is not enough available capacity. You should " +
                                     "increase the number of threads and/or application instances " +
                                     "to maintain the requested number of standby replicas.",
                             numStandbyReplicas - i,
                             numStandbyReplicas, taskId);
                    break;
                }
                allocateTaskWithClientCandidates(taskId, ids, false);
            }
        }
    }

    private void assignActive()
{
        int totalCapacity = sumCapacity(clients.values());
        int tasksPerThread = taskIds.size() / totalCapacity;
        Set<TaskId> assigned = new HashSet<>();

        // first try and re-assign existing active tasks to clients that previously had
        // the same active task
        foreach (Map.Entry<TaskId, ID> entry in previousActiveTaskAssignment.entrySet())
{
            TaskId taskId = entry.getKey();
            if (taskIds.contains(taskId))
{
                ClientState client = clients[entry.getValue()];
                if (client.hasUnfulfilledQuota(tasksPerThread))
{
                    assignTaskToClient(assigned, taskId, client);
                }
            }
        }

        Set<TaskId> unassigned = new HashSet<>(taskIds);
        unassigned.removeAll(assigned);

        // try and assign any remaining unassigned tasks to clients that previously
        // have seen the task.
        for (Iterator<TaskId> iterator = unassigned.iterator(); iterator.hasNext(); )
{
            TaskId taskId = iterator.next();
            Set<ID> clientIds = previousStandbyTaskAssignment[taskId];
            if (clientIds != null)
{
                foreach (ID clientId in clientIds)
{
                    ClientState client = clients[clientId];
                    if (client.hasUnfulfilledQuota(tasksPerThread))
{
                        assignTaskToClient(assigned, taskId, client);
                        iterator.Remove();
                        break;
                    }
                }
            }
        }

        // assign any remaining unassigned tasks
        List<TaskId> sortedTasks = new List<>(unassigned);
        Collections.sort(sortedTasks);
        foreach (TaskId taskId in sortedTasks)
{
            allocateTaskWithClientCandidates(taskId, clients.keySet(), true);
        }
    }

    private void allocateTaskWithClientCandidates(TaskId taskId, Set<ID> clientsWithin, bool active)
{
        ClientState client = findClient(taskId, clientsWithin);
        taskPairs.addPairs(taskId, client.assignedTasks());
        client.assign(taskId, active);
    }

    private void assignTaskToClient(Set<TaskId> assigned, TaskId taskId, ClientState client)
{
        taskPairs.addPairs(taskId, client.assignedTasks());
        client.assign(taskId, true);
        assigned.add(taskId);
    }

    private Set<ID> findClientsWithoutAssignedTask(TaskId taskId)
{
        Set<ID> clientIds = new HashSet<>();
        foreach (Map.Entry<ID, ClientState> client in clients.entrySet())
{
            if (!client.getValue().hasAssignedTask(taskId))
{
                clientIds.add(client.getKey());
            }
        }
        return clientIds;
    }


    private ClientState findClient(TaskId taskId, Set<ID> clientsWithin)
{

        // optimize the case where there is only 1 id to search within.
        if (clientsWithin.size() == 1)
{
            return clients[clientsWithin.iterator().next()];
        }

        ClientState previous = findClientsWithPreviousAssignedTask(taskId, clientsWithin);
        if (previous == null)
{
            return leastLoaded(taskId, clientsWithin);
        }

        if (shouldBalanceLoad(previous))
{
            ClientState standby = findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
            if (standby == null
                    || shouldBalanceLoad(standby))
{
                return leastLoaded(taskId, clientsWithin);
            }
            return standby;
        }

        return previous;
    }

    private bool shouldBalanceLoad(ClientState client)
{
        return client.reachedCapacity() && hasClientsWithMoreAvailableCapacity(client);
    }

    private bool hasClientsWithMoreAvailableCapacity(ClientState client)
{
        foreach (ClientState clientState in clients.values())
{
            if (clientState.hasMoreAvailableCapacityThan(client))
{
                return true;
            }
        }
        return false;
    }

    private ClientState findClientsWithPreviousAssignedTask(TaskId taskId,
                                                                    Set<ID> clientsWithin)
{
        ID previous = previousActiveTaskAssignment[taskId];
        if (previous != null && clientsWithin.contains(previous))
{
            return clients[previous];
        }
        return findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
    }

    private ClientState findLeastLoadedClientWithPreviousStandByTask(TaskId taskId, Set<ID> clientsWithin)
{
        Set<ID> ids = previousStandbyTaskAssignment[taskId];
        if (ids == null)
{
            return null;
        }
        HashSet<ID> constrainTo = new HashSet<>(ids);
        constrainTo.retainAll(clientsWithin);
        return leastLoaded(taskId, constrainTo);
    }

    private ClientState leastLoaded(TaskId taskId, Set<ID> clientIds)
{
        ClientState leastLoaded = findLeastLoaded(taskId, clientIds, true);
        if (leastLoaded == null)
{
            return findLeastLoaded(taskId, clientIds, false);
        }
        return leastLoaded;
    }

    private ClientState findLeastLoaded(TaskId taskId,
                                        Set<ID> clientIds,
                                        bool checkTaskPairs)
{
        ClientState leastLoaded = null;
        foreach (ID id in clientIds)
{
            ClientState client = clients[id];
            if (client.assignedTaskCount() == 0)
{
                return client;
            }

            if (leastLoaded == null || client.hasMoreAvailableCapacityThan(leastLoaded))
{
                if (!checkTaskPairs)
{
                    leastLoaded = client;
                } else if (taskPairs.hasNewPair(taskId, client.assignedTasks()))
{
                    leastLoaded = client;
                }
            }

        }
        return leastLoaded;

    }

    private void mapPreviousTaskAssignment(Dictionary<ID, ClientState> clients)
{
        foreach (Map.Entry<ID, ClientState> clientState in clients.entrySet())
{
            foreach (TaskId activeTask in clientState.getValue().previousActiveTasks())
{
                previousActiveTaskAssignment.Add(activeTask, clientState.getKey());
            }

            foreach (TaskId prevAssignedTask in clientState.getValue().previousStandbyTasks())
{
                if (!previousStandbyTaskAssignment.ContainsKey(prevAssignedTask))
{
                    previousStandbyTaskAssignment.Add(prevAssignedTask, new HashSet<>());
                }
                previousStandbyTaskAssignment[prevAssignedTask).add(clientState.getKey()];
            }
        }

    }

    private int sumCapacity(Collection<ClientState> values)
{
        int capacity = 0;
        foreach (ClientState client in values)
{
            capacity += client.capacity();
        }
        return capacity;
    }

    private static class TaskPairs {
        private Set<Pair> pairs;
        private int maxPairs;

        TaskPairs(int maxPairs)
{
            this.maxPairs = maxPairs;
            this.pairs = new HashSet<>(maxPairs);
        }

        bool hasNewPair(TaskId task1,
                           Set<TaskId> taskIds)
{
            if (pairs.size() == maxPairs)
{
                return false;
            }
            foreach (TaskId taskId in taskIds)
{
                if (!pairs.contains(pair(task1, taskId)))
{
                    return true;
                }
            }
            return false;
        }

        void addPairs(TaskId taskId, Set<TaskId> assigned)
{
            foreach (TaskId id in assigned)
{
                pairs.add(pair(id, taskId));
            }
        }

        Pair pair(TaskId task1, TaskId task2)
{
            if (task1.compareTo(task2) < 0)
{
                return new Pair(task1, task2);
            }
            return new Pair(task2, task1);
        }

        private static class Pair {
            private TaskId task1;
            private TaskId task2;

            Pair(TaskId task1, TaskId task2)
{
                this.task1 = task1;
                this.task2 = task2;
            }

            
            public bool Equals(object o)
{
                if (this == o)
{
                    return true;
                }
                if (o == null || GetType() != o.GetType())
{
                    return false;
                }
                Pair pair = (Pair) o;
                return Objects.Equals(task1, pair.task1) &&
                        Objects.Equals(task2, pair.task2);
            }

            
            public int GetHashCode()
{
                return Objects.hash(task1, task2);
            }
        }


    }

}

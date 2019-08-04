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

import java.util.HashSet;
import java.util.Set;

public class ClientState {
    private Set<TaskId> activeTasks;
    private Set<TaskId> standbyTasks;
    private Set<TaskId> assignedTasks;
    private Set<TaskId> prevActiveTasks;
    private Set<TaskId> prevStandbyTasks;
    private Set<TaskId> prevAssignedTasks;

    private int capacity;


    public ClientState()
{
        this(0);
    }

    ClientState(int capacity)
{
        this(new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), capacity);
    }

    private ClientState(Set<TaskId> activeTasks,
                        Set<TaskId> standbyTasks,
                        Set<TaskId> assignedTasks,
                        Set<TaskId> prevActiveTasks,
                        Set<TaskId> prevStandbyTasks,
                        Set<TaskId> prevAssignedTasks,
                        int capacity)
{
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
        this.assignedTasks = assignedTasks;
        this.prevActiveTasks = prevActiveTasks;
        this.prevStandbyTasks = prevStandbyTasks;
        this.prevAssignedTasks = prevAssignedTasks;
        this.capacity = capacity;
    }

    public ClientState copy()
{
        return new ClientState(
            new HashSet<>(activeTasks),
            new HashSet<>(standbyTasks),
            new HashSet<>(assignedTasks),
            new HashSet<>(prevActiveTasks),
            new HashSet<>(prevStandbyTasks),
            new HashSet<>(prevAssignedTasks),
            capacity);
    }

    public void assign(TaskId taskId, bool active)
{
        if (active)
{
            activeTasks.add(taskId);
        } else {
            standbyTasks.add(taskId);
        }

        assignedTasks.add(taskId);
    }

    public Set<TaskId> activeTasks()
{
        return activeTasks;
    }

    public Set<TaskId> standbyTasks()
{
        return standbyTasks;
    }

    public Set<TaskId> prevActiveTasks()
{
        return prevActiveTasks;
    }

    public Set<TaskId> prevStandbyTasks()
{
        return prevStandbyTasks;
    }

    @SuppressWarnings("WeakerAccess")
    public int assignedTaskCount()
{
        return assignedTasks.size();
    }

    public void incrementCapacity()
{
        capacity++;
    }

    @SuppressWarnings("WeakerAccess")
    public int activeTaskCount()
{
        return activeTasks.size();
    }

    public void addPreviousActiveTasks(Set<TaskId> prevTasks)
{
        prevActiveTasks.addAll(prevTasks);
        prevAssignedTasks.addAll(prevTasks);
    }

    public void addPreviousStandbyTasks(Set<TaskId> standbyTasks)
{
        prevStandbyTasks.addAll(standbyTasks);
        prevAssignedTasks.addAll(standbyTasks);
    }

    
    public string ToString()
{
        return "[activeTasks: (" + activeTasks +
                ") standbyTasks: (" + standbyTasks +
                ") assignedTasks: (" + assignedTasks +
                ") prevActiveTasks: (" + prevActiveTasks +
                ") prevStandbyTasks: (" + prevStandbyTasks +
                ") prevAssignedTasks: (" + prevAssignedTasks +
                ") capacity: " + capacity +
                "]";
    }

    bool reachedCapacity()
{
        return assignedTasks.size() >= capacity;
    }

    bool hasMoreAvailableCapacityThan(ClientState other)
{
        if (this.capacity <= 0)
{
            throw new InvalidOperationException("Capacity of this ClientState must be greater than 0.");
        }

        if (other.capacity <= 0)
{
            throw new InvalidOperationException("Capacity of other ClientState must be greater than 0");
        }

        double otherLoad = (double) other.assignedTaskCount() / other.capacity;
        double thisLoad = (double) assignedTaskCount() / capacity;

        if (thisLoad < otherLoad)
{
            return true;
        } else if (thisLoad > otherLoad)
{
            return false;
        } else {
            return capacity > other.capacity;
        }
    }

    Set<TaskId> previousStandbyTasks()
{
        Set<TaskId> standby = new HashSet<>(prevAssignedTasks);
        standby.removeAll(prevActiveTasks);
        return standby;
    }

    Set<TaskId> previousActiveTasks()
{
        return prevActiveTasks;
    }

    bool hasAssignedTask(TaskId taskId)
{
        return assignedTasks.contains(taskId);
    }

    // Visible for testing
    Set<TaskId> assignedTasks()
{
        return assignedTasks;
    }

    Set<TaskId> previousAssignedTasks()
{
        return prevAssignedTasks;
    }

    int capacity()
{
        return capacity;
    }

    bool hasUnfulfilledQuota(int tasksPerThread)
{
        return activeTasks.size() < capacity * tasksPerThread;
    }
}
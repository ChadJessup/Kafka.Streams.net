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
package org.apache.kafka.streams.processor.internals;

using Kafka.Common.Utils.Time;
using Kafka.Common.Utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * Manages the directories where the state of Tasks owned by a {@link StreamThread} are
 * stored. Handles creation/locking/unlocking/cleaning of the Task Directories. This class is not
 * thread-safe.
 */
public class StateDirectory {

    private static Pattern PATH_NAME = Pattern.compile("\\d+_\\d+");

    static string LOCK_FILE_NAME = ".lock";
    private static Logger log = LoggerFactory.getLogger(StateDirectory.class);

    private File stateDir;
    private bool createStateDirectory;
    private HashMap<TaskId, FileChannel> channels = new HashMap<>();
    private HashMap<TaskId, LockAndOwner> locks = new HashMap<>();
    private Time time;

    private FileChannel globalStateChannel;
    private FileLock globalStateLock;

    private static class LockAndOwner {
        FileLock lock;
        string owningThread;

        LockAndOwner(string owningThread, FileLock lock) {
            this.owningThread = owningThread;
            this.lock = lock;
        }
    }

    /**
     * Ensures that the state base directory as well as the application's sub-directory are created.
     *
     * @throws ProcessorStateException if the base state directory or application state directory does not exist
     *                                 and could not be created when createStateDirectory is enabled.
     */
    public StateDirectory(StreamsConfig config,
                          Time time,
                          bool createStateDirectory) {
        this.time = time;
        this.createStateDirectory = createStateDirectory;
        string stateDirName = config.getString(StreamsConfig.STATE_DIR_CONFIG);
        File baseDir = new File(stateDirName);
        if (this.createStateDirectory && !baseDir.exists() && !baseDir.mkdirs()) {
            throw new ProcessorStateException(
                string.format("base state directory [%s] doesn't exist and couldn't be created", stateDirName));
        }
        stateDir = new File(baseDir, config.getString(StreamsConfig.APPLICATION_ID_CONFIG));
        if (this.createStateDirectory && !stateDir.exists() && !stateDir.mkdir()) {
            throw new ProcessorStateException(
                string.format("state directory [%s] doesn't exist and couldn't be created", stateDir.getPath()));
        }
    }

    /**
     * Get or create the directory for the provided {@link TaskId}.
     * @return directory for the {@link TaskId}
     * @throws ProcessorStateException if the task directory does not exists and could not be created
     */
    public File directoryForTask(TaskId taskId) {
        File taskDir = new File(stateDir, taskId.toString());
        if (createStateDirectory && !taskDir.exists() && !taskDir.mkdir()) {
            throw new ProcessorStateException(
                string.format("task directory [%s] doesn't exist and couldn't be created", taskDir.getPath()));
        }
        return taskDir;
    }

    /**
     * Get or create the directory for the global stores.
     * @return directory for the global stores
     * @throws ProcessorStateException if the global store directory does not exists and could not be created
     */
    File globalStateDir() {
        File dir = new File(stateDir, "global");
        if (createStateDirectory && !dir.exists() && !dir.mkdir()) {
            throw new ProcessorStateException(
                string.format("global state directory [%s] doesn't exist and couldn't be created", dir.getPath()));
        }
        return dir;
    }

    private string logPrefix() {
        return string.format("stream-thread [%s]", Thread.currentThread().getName());
    }

    /**
     * Get the lock for the {@link TaskId}s directory if it is available
     * @param taskId
     * @return true if successful
     * @throws IOException
     */
    synchronized bool lock(TaskId taskId) throws IOException {
        if (!createStateDirectory) {
            return true;
        }

        File lockFile;
        // we already have the lock so bail out here
        LockAndOwner lockAndOwner = locks.get(taskId);
        if (lockAndOwner != null && lockAndOwner.owningThread.Equals(Thread.currentThread().getName())) {
            log.trace("{} Found cached state dir lock for task {}", logPrefix(), taskId);
            return true;
        } else if (lockAndOwner != null) {
            // another thread owns the lock
            return false;
        }

        try {
            lockFile = new File(directoryForTask(taskId), LOCK_FILE_NAME);
        } catch (ProcessorStateException e) {
            // directoryForTask could be throwing an exception if another thread
            // has concurrently deleted the directory
            return false;
        }

        FileChannel channel;

        try {
            channel = getOrCreateFileChannel(taskId, lockFile.toPath());
        } catch (NoSuchFileException e) {
            // FileChannel.open(..) could throw NoSuchFileException when there is another thread
            // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
            // file, in this case we will return immediately indicating locking failed.
            return false;
        }

        FileLock lock = tryLock(channel);
        if (lock != null) {
            locks.put(taskId, new LockAndOwner(Thread.currentThread().getName(), lock));

            log.debug("{} Acquired state dir lock for task {}", logPrefix(), taskId);
        }
        return lock != null;
    }

    synchronized bool lockGlobalState() throws IOException {
        if (!createStateDirectory) {
            return true;
        }

        if (globalStateLock != null) {
            log.trace("{} Found cached state dir lock for the global task", logPrefix());
            return true;
        }

        File lockFile = new File(globalStateDir(), LOCK_FILE_NAME);
        FileChannel channel;
        try {
            channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (NoSuchFileException e) {
            // FileChannel.open(..) could throw NoSuchFileException when there is another thread
            // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
            // file, in this case we will return immediately indicating locking failed.
            return false;
        }
        FileLock fileLock = tryLock(channel);
        if (fileLock == null) {
            channel.close();
            return false;
        }
        globalStateChannel = channel;
        globalStateLock = fileLock;

        log.debug("{} Acquired global state dir lock", logPrefix());

        return true;
    }

    synchronized void unlockGlobalState() throws IOException {
        if (globalStateLock == null) {
            return;
        }
        globalStateLock.release();
        globalStateChannel.close();
        globalStateLock = null;
        globalStateChannel = null;

        log.debug("{} Released global state dir lock", logPrefix());
    }

    /**
     * Unlock the state directory for the given {@link TaskId}.
     */
    synchronized void unlock(TaskId taskId) throws IOException {
        LockAndOwner lockAndOwner = locks.get(taskId);
        if (lockAndOwner != null && lockAndOwner.owningThread.Equals(Thread.currentThread().getName())) {
            locks.remove(taskId);
            lockAndOwner.lock.release();
            log.debug("{} Released state dir lock for task {}", logPrefix(), taskId);

            FileChannel fileChannel = channels.remove(taskId);
            if (fileChannel != null) {
                fileChannel.close();
            }
        }
    }

    public synchronized void clean() {
        try {
            cleanRemovedTasks(0, true);
        } catch (Exception e) {
            // this is already logged within cleanRemovedTasks
            throw new StreamsException(e);
        }
        try {
            if (stateDir.exists()) {
                Utils.delete(globalStateDir().getAbsoluteFile());
            }
        } catch (IOException e) {
            log.error("{} Failed to delete global state directory due to an unexpected exception", logPrefix(), e);
            throw new StreamsException(e);
        }
    }

    /**
     * Remove the directories for any {@link TaskId}s that are no-longer
     * owned by this {@link StreamThread} and aren't locked by either
     * another process or another {@link StreamThread}
     * @param cleanupDelayMs only remove directories if they haven't been modified for at least
     *                       this amount of time (milliseconds)
     */
    public synchronized void cleanRemovedTasks(long cleanupDelayMs) {
        try {
            cleanRemovedTasks(cleanupDelayMs, false);
        } catch (Exception cannotHappen) {
            throw new InvalidOperationException("Should have swallowed exception.", cannotHappen);
        }
    }

    private synchronized void cleanRemovedTasks(long cleanupDelayMs,
                                                bool manualUserCall) throws Exception {
        File[] taskDirs = listTaskDirectories();
        if (taskDirs == null || taskDirs.length == 0) {
            return; // nothing to do
        }

        for (File taskDir : taskDirs) {
            string dirName = taskDir.getName();
            TaskId id = TaskId.parse(dirName);
            if (!locks.containsKey(id)) {
                try {
                    if (lock(id)) {
                        long now = time.milliseconds();
                        long lastModifiedMs = taskDir.lastModified();
                        if (now > lastModifiedMs + cleanupDelayMs || manualUserCall) {
                            if (!manualUserCall) {
                                log.info(
                                    "{} Deleting obsolete state directory {} for task {} as {}ms has elapsed (cleanup delay is {}ms).",
                                    logPrefix(),
                                    dirName,
                                    id,
                                    now - lastModifiedMs,
                                    cleanupDelayMs);
                            } else {
                                log.info(
                                        "{} Deleting state directory {} for task {} as user calling cleanup.",
                                        logPrefix(),
                                        dirName,
                                        id);
                            }
                            Utils.delete(taskDir);
                        }
                    }
                } catch (OverlappingFileLockException e) {
                    // locked by another thread
                    if (manualUserCall) {
                        log.error("{} Failed to get the state directory lock.", logPrefix(), e);
                        throw e;
                    }
                } catch (IOException e) {
                    log.error("{} Failed to delete the state directory.", logPrefix(), e);
                    if (manualUserCall) {
                        throw e;
                    }
                } finally {
                    try {
                        unlock(id);
                    } catch (IOException e) {
                        log.error("{} Failed to release the state directory lock.", logPrefix());
                        if (manualUserCall) {
                            throw e;
                        }
                    }
                }
            }
        }
    }

    /**
     * List all of the task directories
     * @return The list of all the existing local directories for stream tasks
     */
    File[] listTaskDirectories() {
        return !stateDir.exists() ? new File[0] :
                stateDir.listFiles(pathname -> pathname.isDirectory() && PATH_NAME.matcher(pathname.getName()).matches());
    }

    private FileChannel getOrCreateFileChannel(TaskId taskId,
                                               Path lockPath) throws IOException {
        if (!channels.containsKey(taskId)) {
            channels.put(taskId, FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
        }
        return channels.get(taskId);
    }

    private FileLock tryLock(FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (OverlappingFileLockException e) {
            return null;
        }
    }

}

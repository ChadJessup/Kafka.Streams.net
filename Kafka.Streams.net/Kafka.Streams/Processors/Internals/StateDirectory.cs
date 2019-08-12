using Kafka.Common;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Kafka.Streams.Processor.Internals
{
    /**
     * Manages the directories where the state of Tasks owned by a {@link StreamThread} are
     * stored. Handles creation/locking/unlocking/cleaning of the Task Directories. This is not
     * thread-safe.
     */
    public class StateDirectory
    {
        private static Regex PATH_NAME = Regex.compile("\\d+_\\d+");

        static string LOCK_FILE_NAME = ".lock";
        private static ILogger log = new LoggerFactory().CreateLogger<StateDirectory>();

        private DirectoryInfo stateDir;
        private bool createStateDirectory;
        private Dictionary<TaskId, FileChannel> channels = new Dictionary<TaskId, FileChannel>();
        private Dictionary<TaskId, LockAndOwner> locks = new Dictionary<TaskId, LockAndOwner>();
        private ITime time;

        private FileChannel globalStateChannel;
        private FileLock globalStateLock;

        /**
         * Ensures that the state base directory as well as the application's sub-directory are created.
         *
         * @throws ProcessorStateException if the base state directory or application state directory does not exist
         *                                 and could not be created when createStateDirectory is enabled.
         */
        public StateDirectory(
            StreamsConfig config,
            ITime time,
            bool createStateDirectory)
        {
            this.time = time;
            this.createStateDirectory = createStateDirectory;
            string stateDirName = config.getString(StreamsConfig.STATE_DIR_CONFIG);
            DirectoryInfo baseDir = new DirectoryInfo(stateDirName);

            Directory.CreateDirectory(baseDir.FullName);

            if (this.createStateDirectory && !baseDir.Exists)
            {
                throw new ProcessorStateException(
                    string.Format("base state directory [%s] doesn't exist and couldn't be created", stateDirName));
            }

            stateDir = new DirectoryInfo(Path.Combine(baseDir.FullName, config.getString(StreamsConfig.APPLICATION_ID_CONFIG)));

            Directory.CreateDirectory(stateDir.FullName);

            if (this.createStateDirectory && !stateDir.Exists)
            {
                throw new ProcessorStateException(
                    string.Format("state directory [%s] doesn't exist and couldn't be created", stateDir.FullName));
            }
        }

        /**
         * Get or create the directory for the provided {@link TaskId}.
         * @return directory for the {@link TaskId}
         * @throws ProcessorStateException if the task directory does not exists and could not be created
         */
        public DirectoryInfo directoryForTask(TaskId taskId)
        {
            var taskDir = new DirectoryInfo(Path.Combine(stateDir.FullName, taskId.ToString()));
            Directory.CreateDirectory(taskDir.FullName);

            if (createStateDirectory && !taskDir.Exists)
            {
                throw new ProcessorStateException(
                    string.Format("task directory [%s] doesn't exist and couldn't be created", taskDir.getPath()));
            }

            return taskDir;
        }

        /**
         * Get or create the directory for the global stores.
         * @return directory for the global stores
         * @throws ProcessorStateException if the global store directory does not exists and could not be created
         */
        public FileInfo globalStateDir()
        {
            FileInfo dir = new FileInfo(Path.Combine(stateDir.FullName, "global"));

            Directory.CreateDirectory(dir.FullName);
            if (createStateDirectory && !dir.Exists)
            {
                throw new ProcessorStateException(
                    string.Format("global state directory [%s] doesn't exist and couldn't be created", dir.FullName));
            }

            return dir;
        }

        private string logPrefix()
        {
            return string.Format("stream-thread [%s]", Thread.CurrentThread.getName());
        }

        /**
         * Get the lock for the {@link TaskId}s directory if it is available
         * @param taskId
         * @return true if successful
         * @throws IOException
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        bool lockTask(TaskId taskId)
        {
            if (!createStateDirectory)
            {
                return true;
            }

            FileInfo lockFile;
            // we already have the lock so bail out here
            LockAndOwner lockAndOwner = locks[taskId];
            if (lockAndOwner != null && lockAndOwner.owningThread.Equals(Thread.CurrentThread.Name))
            {
                log.LogTrace("{} Found cached state dir lock for task {}", logPrefix(), taskId);
                return true;
            }
            else if (lockAndOwner != null)
            {
                // another thread owns the lock
                return false;
            }

            try
            {
                lockFile = new FileInfo(Path.Combine(directoryForTask(taskId).FullName, LOCK_FILE_NAME));
            }
            catch (ProcessorStateException e)
            {
                // directoryForTask could be throwing an exception if another thread
                // has concurrently deleted the directory
                return false;
            }

            FileChannel channel;

            try
            {

                channel = getOrCreateFileChannel(taskId, lockFile.toPath());
            }
            catch (FileNotFoundException e)
            {
                // FileChannel.open(..) could throw FileNotFoundException when there is another thread
                // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
                // file, in this case we will return immediately indicating locking failed.
                return false;
            }

            FileLock @lock = tryLock(channel);
            if (@lock != null)
            {
                locks.Add(taskId, new LockAndOwner(Thread.CurrentThread.Name, @lock));

                log.LogDebug("{} Acquired state dir lock for task {}", logPrefix(), taskId);
            }
            return @lock != null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool lockGlobalState()
        {
            if (!createStateDirectory)
            {
                return true;
            }

            if (globalStateLock != null)
            {
                log.LogTrace("{} Found cached state dir lock for the global task", logPrefix());
                return true;
            }

            FileInfo lockFile = new FileInfo(Path.Combine(globalStateDir(), LOCK_FILE_NAME));
            FileChannel channel;
            try
            {

                channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            }
            catch (FileNotFoundException e)
            {
                // FileChannel.open(..) could throw FileNotFoundException when there is another thread
                // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
                // file, in this case we will return immediately indicating locking failed.
                return false;
            }
            FileLock fileLock = tryLock(channel);
            if (fileLock == null)
            {
                channel.close();
                return false;
            }
            globalStateChannel = channel;
            globalStateLock = fileLock;

            log.LogDebug("{} Acquired global state dir lock", logPrefix());

            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void unlockGlobalState()
        {
            if (globalStateLock == null)
            {
                return;
            }
            globalStateLock.release();
            globalStateChannel.close();
            globalStateLock = null;
            globalStateChannel = null;

            log.LogDebug("{} Released global state dir lock", logPrefix());
        }

        /**
         * Unlock the state directory for the given {@link TaskId}.
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void unlock(TaskId taskId)
        {
            LockAndOwner lockAndOwner = locks[taskId];
            if (lockAndOwner != null && lockAndOwner.owningThread.Equals(Thread.CurrentThread.Name))
            {
                locks.Remove(taskId);
                lockAndOwner.@lock.release();
                log.LogDebug("{} Released state dir lock for task {}", logPrefix(), taskId);

                FileChannel fileChannel = channels.Remove(taskId);
                if (fileChannel != null)
                {
                    fileChannel.close();
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void clean()
        {
            try
            {

                cleanRemovedTasks(0, true);
            }
            catch (Exception e)
            {
                // this is already logged within cleanRemovedTasks
                throw new StreamsException(e);
            }
            try
            {

                if (stateDir.Exists)
                {
                    Utils.delete(globalStateDir().FullName);
                }
            }
            catch (IOException e)
            {
                log.LogError("{} Failed to delete global state directory due to an unexpected exception", logPrefix(), e);
                throw new StreamsException(e);
            }
        }

        /**
         * Remove the directories for any {@link TaskId}s that are no-longer
         * owned by this {@link StreamThread} and aren't locked by either
         * another process or another {@link StreamThread}
         * @param cleanupDelayMs only Remove directories if they haven't been modified for at least
         *                       this amount of time (milliseconds)
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void cleanRemovedTasks(long cleanupDelayMs)
        {
            try
            {

                cleanRemovedTasks(cleanupDelayMs, false);
            }
            catch (Exception cannotHappen)
            {
                throw new InvalidOperationException("Should have swallowed exception.", cannotHappen);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void cleanRemovedTasks(long cleanupDelayMs,
                                                    bool manualUserCall)
        {
            FileInfo[] taskDirs = listTaskDirectories();
            if (taskDirs == null || taskDirs.Length == 0)
            {
                return; // nothing to do
            }

            foreach (FileInfo taskDir in taskDirs)
            {
                string dirName = taskDir.FullName;
                TaskId id = TaskId.parse(dirName);
                if (!locks.ContainsKey(id))
                {
                    try
                    {
                        lock (id)
                        {
                            long now = time.milliseconds();
                            long lastModifiedMs = taskDir.LastWriteTimeUtc;
                            if (now > lastModifiedMs + cleanupDelayMs || manualUserCall)
                            {
                                if (!manualUserCall)
                                {
                                    log.LogInformation(
                                        "{} Deleting obsolete state directory {} for task {} as {}ms has elapsed (cleanup delay is {}ms).",
                                        logPrefix(),
                                        dirName,
                                        id,
                                        now - lastModifiedMs,
                                        cleanupDelayMs);
                                }
                                else
                                {

                                    log.LogInformation(
                                            "{} Deleting state directory {} for task {} as user calling cleanup.",
                                            logPrefix(),
                                            dirName,
                                            id);
                                }
                                Utils.delete(taskDir);
                            }
                        }
                    }
                    catch (OverlappingFileLockException e)
                    {
                        // locked by another thread
                        if (manualUserCall)
                        {
                            log.LogError("{} Failed to get the state directory lock.", logPrefix(), e);
                            throw e;
                        }
                    }
                    catch (IOException e)
                    {
                        log.LogError("{} Failed to delete the state directory.", logPrefix(), e);
                        if (manualUserCall)
                        {
                            throw e;
                        }
                    }
                    finally
                    {

                        try
                        {

                            unlock(id);
                        }
                        catch (IOException e)
                        {
                            log.LogError("{} Failed to release the state directory lock.", logPrefix());
                            if (manualUserCall)
                            {
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
        public DirectoryInfo[] listTaskDirectories()
        {
            return !stateDir.Exists
                ? new DirectoryInfo[0]
                : stateDir.GetDirectories().Select(pathname => pathname.Attributes == FileAttributes.Directory && PATH_NAME.matcher(pathname.FullName).matches()).ToArray();
        }

        private FileChannel getOrCreateFileChannel(TaskId taskId,
                                                   Path lockPath)
        {
            if (!channels.ContainsKey(taskId))
            {
                channels.Add(taskId, FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
            }
            return channels[taskId];
        }

        private FileLock tryLock(FileChannel channel)
        {
            try
            {

                return channel.tryLock();
            }
            catch (OverlappingFileLockException e)
            {
                return null;
            }
        }
    }
}
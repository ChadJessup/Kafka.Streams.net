using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tasks;
using Microsoft.Extensions.Logging;
using NodaTime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;

namespace Kafka.Streams.State
{
    /**
     * Manages the directories where the state of Tasks owned by a {@link KafkaStreamThread} are
     * stored. Handles creation/locking/unlocking/cleaning of the Task Directories. This is not
     * thread-safe.
     */
    public class StateDirectory
    {
        private static readonly Regex PATH_NAME = new Regex("\\d+_\\d+", RegexOptions.Compiled);

        static readonly string LOCK_FILE_NAME = ".lock";

        private readonly ILogger logger;
        private readonly DirectoryInfo stateDir;
        private readonly bool createStateDirectory;
        private readonly Dictionary<TaskId, FileChannel> channels = new Dictionary<TaskId, FileChannel>();
        private readonly Dictionary<TaskId, LockAndOwner> locks = new Dictionary<TaskId, LockAndOwner>();
        private readonly IClock clock;

        private FileChannel globalStateChannel;
        private FileLock globalStateLock;

        /**
         * Ensures that the state base directory as well as the application's sub-directory are created.
         *
         * @throws ProcessorStateException if the base state directory or application state directory does not exist
         *                                 and could not be created when createStateDirectory is enabled.
         */
        public StateDirectory(
            ILogger<StateDirectory> logger,
            StreamsConfig config,
            IClock clock)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            config = config ?? throw new ArgumentNullException(nameof(config));

            this.clock = clock;
            this.createStateDirectory = true;
            string stateDirName = config.StateStoreDirectory;
            DirectoryInfo baseDir = new DirectoryInfo(stateDirName);

            if (this.createStateDirectory)
            {
                try
                {
                    Directory.CreateDirectory(baseDir.FullName);
                }
                catch (Exception e)
                {
                    throw new ProcessorStateException($"base state directory [{stateDirName}] doesn't exist and couldn't be created", e);
                }
            }

            stateDir = new DirectoryInfo(Path.Combine(baseDir.FullName, config.ApplicationId));

            if (this.createStateDirectory && !stateDir.Exists)
            {
                try
                {
                    Directory.CreateDirectory(stateDir.FullName);
                }
                catch (Exception e)
                {
                    throw new ProcessorStateException($"state directory [{stateDir.FullName}] doesn't exist and couldn't be created", e);
                }
            }
        }

        /**
         * Get or create the directory for the provided {@link TaskId}.
         * @return directory for the {@link TaskId}
         * @throws ProcessorStateException if the task directory does not exists and could not be created
         */
        public DirectoryInfo directoryForTask(TaskId taskId)
        {
            taskId = taskId ?? throw new ArgumentNullException(nameof(taskId));

            var taskDir = new DirectoryInfo(Path.Combine(stateDir.FullName, taskId.ToString()));

            if (this.createStateDirectory && !taskDir.Exists)
            {
                try
                {
                    Directory.CreateDirectory(taskDir.FullName);
                }
                catch (Exception e)
                {
                    throw new ProcessorStateException($"task directory [{taskDir.FullName}] doesn't exist and couldn't be created", e);
                }
            }

            return taskDir;
        }

        /**
         * Get or create the directory for the global stores.
         * @return directory for the global stores
         * @throws ProcessorStateException if the global store directory does not exists and could not be created
         */
        public DirectoryInfo globalStateDir()
        {
            DirectoryInfo dir = new DirectoryInfo(Path.Combine(stateDir.FullName, "global"));

            Directory.CreateDirectory(dir.FullName);
            if (createStateDirectory && !dir.Exists)
            {
                throw new ProcessorStateException(
                    string.Format("global state directory [%s] doesn't exist and couldn't be created", dir.FullName));
            }

            return dir;
        }

        private string logPrefix()
            => $"stream-thread [{Thread.CurrentThread.Name}]";

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
                logger.LogTrace("{} Found cached state dir lock for task {}", logPrefix(), taskId);
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
            catch (ProcessorStateException)
            {
                // directoryForTask could be throwing an exception if another thread
                // has concurrently deleted the directory
                return false;
            }

            FileChannel channel;

            try
            {

                channel = getOrCreateFileChannel(taskId, lockFile.FullName);
            }
            catch (FileNotFoundException)
            {
                // FileChannel.open(..) could throw FileNotFoundException when there is another thread
                // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
                // file, in this case we will return immediately indicating locking failed.
                return false;
            }

            FileLock @lock = TryLock(channel);
            if (@lock != null)
            {
                locks.Add(taskId, new LockAndOwner(Thread.CurrentThread.Name, @lock));

                logger.LogDebug("{} Acquired state dir lock for task {}", logPrefix(), taskId);
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
                logger.LogTrace($"{logPrefix()} Found cached state dir lock for the global task");

                return true;
            }

            FileInfo lockFile = new FileInfo(Path.Combine(globalStateDir().FullName, LOCK_FILE_NAME));
            FileChannel channel;

            try
            {
                channel = null;// FileChannel.open(lockFile.FullName, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            }
            catch (FileNotFoundException)
            {
                // FileChannel.open(..) could throw FileNotFoundException when there is another thread
                // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
                // file, in this case we will return immediately indicating locking failed.
                return false;
            }

            FileLock fileLock = TryLock(channel);
            if (fileLock == null)
            {
                channel.close();
                return false;
            }

            globalStateChannel = channel;
            globalStateLock = fileLock;

            logger.LogDebug("{} Acquired global state dir lock", logPrefix());

            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void UnlockGlobalState()
        {
            if (globalStateLock == null)
            {
                return;
            }

            globalStateLock.release();
            globalStateChannel.close();
            globalStateLock = null;
            globalStateChannel = null;

            logger.LogDebug($"{logPrefix()} Released global state dir lock");
        }

        /**
         * Unlock the state directory for the given {@link TaskId}.
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Unlock(TaskId taskId)
        {
            LockAndOwner lockAndOwner = locks[taskId];
            if (lockAndOwner != null && lockAndOwner.owningThread.Equals(Thread.CurrentThread.Name))
            {
                locks.Remove(taskId);
                //lockAndOwner.@lock.release();
                logger.LogDebug("{} Released state dir lock for task {}", logPrefix(), taskId);

                if (channels.Remove(taskId, out FileChannel fileChannel))
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
                    globalStateDir().Delete();
                }
            }
            catch (IOException e)
            {
                logger.LogError("{} Failed to delete global state directory due to an unexpected exception", logPrefix(), e);

                throw new StreamsException(e);
            }
        }

        /**
         * Remove the directories for any {@link TaskId}s that are no-longer
         * owned by this {@link KafkaStreamThread} and aren't locked by either
         * another process or another {@link KafkaStreamThread}
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
        private void cleanRemovedTasks(
            long cleanupDelayMs,
            bool manualUserCall)
        {
            DirectoryInfo[] taskDirs = listTaskDirectories();
            if (taskDirs == null || taskDirs.Length == 0)
            {
                return; // nothing to do
            }

            foreach (var taskDir in taskDirs)
            {
                string dirName = taskDir.FullName;
                TaskId id = TaskId.parse(dirName);
                if (!locks.ContainsKey(id))
                {
                    try
                    {
                        lock (id)
                        {
                            DateTime now = DateTime.Now;
                            DateTime lastModified = taskDir.LastWriteTimeUtc;
                            if (now > (lastModified.AddMilliseconds(cleanupDelayMs)) || manualUserCall)
                            {
                                if (!manualUserCall)
                                {
                                    logger.LogInformation(
                                        "{} Deleting obsolete state directory {} for task {} as {}ms has elapsed (cleanup delay is {}ms).",
                                        logPrefix(),
                                        dirName,
                                        id,
                                        now - lastModified,
                                        cleanupDelayMs);
                                }
                                else
                                {
                                    logger.LogInformation(
                                            "{} Deleting state directory {} for task {} as user calling cleanup.",
                                            logPrefix(),
                                            dirName,
                                            id);
                                }

                                taskDir.Delete();
                            }
                        }
                    }
                    catch (OverlappingFileLockException e)
                    {
                        // locked by another thread
                        if (manualUserCall)
                        {
                            logger.LogError("{} Failed to get the state directory lock.", logPrefix(), e);
                            
                            throw;
                        }
                    }
                    catch (IOException e)
                    {
                        logger.LogError("{} Failed to delete the state directory.", logPrefix(), e);
                        if (manualUserCall)
                        {
                            throw;
                        }
                    }
                    finally
                    {
                        try
                        {
                            Unlock(id);
                        }
                        catch (IOException e)
                        {
                            logger.LogError(e, $"{logPrefix()} Failed to release the state directory lock.");

                            if (manualUserCall)
                            {
                                throw;
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
                : stateDir.GetDirectories().Where(pathname => pathname.Attributes == FileAttributes.Directory && PATH_NAME.IsMatch(pathname.FullName)).ToArray();
        }

        private FileChannel getOrCreateFileChannel(
            TaskId taskId,
            string lockPath)
        {
            if (!channels.ContainsKey(taskId))
            {
                //                channels.Add(taskId, FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
            }

            return channels[taskId];
        }

        private FileLock TryLock(FileChannel channel)
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
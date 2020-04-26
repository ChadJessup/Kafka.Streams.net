using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tasks;
using Microsoft.Extensions.Logging;

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
        public const string LOCK_FILE_NAME = ".lock";

        private readonly ILogger logger;
        private readonly DirectoryInfo stateDir;
        private readonly bool createStateDirectory;
        private readonly Dictionary<TaskId, FileStream> channels = new Dictionary<TaskId, FileStream>();
        private readonly Dictionary<TaskId, LockAndOwner> locks = new Dictionary<TaskId, LockAndOwner>();
        private readonly IClock clock;

        private FileStream globalStateChannel;
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
            this.logger = logger;// ?? throw new ArgumentNullException(nameof(logger));
            config = config ?? throw new ArgumentNullException(nameof(config));

            this.clock = clock;
            this.createStateDirectory = true;
            var stateDirName = config.StateStoreDirectory;
            var baseDir = stateDirName;

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

            this.stateDir = new DirectoryInfo(Path.Combine(baseDir.FullName, config.ApplicationId));

            if (this.createStateDirectory && !this.stateDir.Exists)
            {
                try
                {
                    Directory.CreateDirectory(this.stateDir.FullName);
                }
                catch (Exception e)
                {
                    throw new ProcessorStateException($"state directory [{this.stateDir.FullName}] doesn't exist and couldn't be created", e);
                }
            }
        }

        /**
         * Get or create the directory for the provided {@link TaskId}.
         * @return directory for the {@link TaskId}
         * @throws ProcessorStateException if the task directory does not exists and could not be created
         */
        public DirectoryInfo DirectoryForTask(TaskId taskId)
        {
            taskId = taskId ?? throw new ArgumentNullException(nameof(taskId));

            var taskDir = new DirectoryInfo(Path.Combine(this.stateDir.FullName, taskId.ToString()));

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
        public DirectoryInfo GlobalStateDir()
        {
            var dir = new DirectoryInfo(Path.Combine(this.stateDir.FullName, "global"));

            Directory.CreateDirectory(dir.FullName);
            if (this.createStateDirectory && !dir.Exists)
            {
                throw new ProcessorStateException(
                    $"global state directory [{dir.FullName}] doesn't exist and couldn't be created");
            }

            return dir;
        }

        private string LogPrefix()
            => $"stream-thread [{Thread.CurrentThread.Name}]";

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool LockGlobalState()
        {
            if (!this.createStateDirectory)
            {
                return true;
            }

            if (this.globalStateLock != null)
            {
                this.logger.LogTrace($"{this.LogPrefix()} Found cached state dir lock for the global task");

                return true;
            }

            var lockFile = new FileInfo(Path.Combine(this.GlobalStateDir().FullName, LOCK_FILE_NAME));
            FileStream channel;

            try
            {
                channel = null;// FileStream.open(lockFile.FullName, FileMode.Create, FileAccess.Write);
            }
            catch (FileNotFoundException)
            {
                // FileStream.open(..) could throw FileNotFoundException when there is another thread
                // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
                // file, in this case we will return immediately indicating locking failed.
                return false;
            }

            FileLock? fileLock = this.TryLock(channel);
            if (fileLock == null)
            {
                channel.Close();
                return false;
            }

            this.globalStateChannel = channel;
            this.globalStateLock = fileLock;

            this.logger.LogDebug($"{this.LogPrefix()} Acquired global state dir lock");

            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void UnlockGlobalState()
        {
            if (this.globalStateLock == null)
            {
                return;
            }

            this.globalStateLock.Release();
            this.globalStateChannel.Close();
            this.globalStateLock = null;
            this.globalStateChannel = null;

            this.logger.LogDebug($"{this.LogPrefix()} Released global state dir lock");
        }

        /**
         * Unlock the state directory for the given {@link TaskId}.
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Unlock(TaskId taskId)
        {
            LockAndOwner lockAndOwner = this.locks[taskId];
            if (lockAndOwner != null && lockAndOwner.OwningThread.Equals(Thread.CurrentThread.Name))
            {
                this.locks.Remove(taskId);
                //lockAndOwner.@lock.release();
                this.logger.LogDebug("{} Released state dir lock for task {}", this.LogPrefix(), taskId);

                if (this.channels.Remove(taskId, out FileStream FileStream))
                {
                    FileStream.Close();
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Clean()
        {
            try
            {
                this.CleanRemovedTasks(0, true);
            }
            catch (Exception e)
            {
                // this is already logged within cleanRemovedTasks
                throw new StreamsException(e);
            }

            try
            {
                if (this.stateDir.Exists)
                {
                    this.GlobalStateDir().Delete();
                }
            }
            catch (IOException e)
            {
                this.logger.LogError("{} Failed to delete global state directory due to an unexpected exception", this.LogPrefix(), e);

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
        public void CleanRemovedTasks(long cleanupDelayMs)
        {
            try
            {
                this.CleanRemovedTasks(cleanupDelayMs, false);
            }
            catch (Exception cannotHappen)
            {
                throw new InvalidOperationException("Should have swallowed exception.", cannotHappen);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void CleanRemovedTasks(
            long cleanupDelayMs,
            bool manualUserCall)
        {
            DirectoryInfo[] taskDirs = this.ListTaskDirectories();
            if (taskDirs == null || taskDirs.Length == 0)
            {
                return; // nothing to do
            }

            foreach (var taskDir in taskDirs)
            {
                var dirName = taskDir.FullName;
                var id = TaskId.Parse(dirName);
                if (!this.locks.ContainsKey(id))
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
                                    this.logger.LogInformation(
                                        "{} Deleting obsolete state directory {} for task {} as {}ms has elapsed (cleanup delay is {}ms).",
                                        this.LogPrefix(),
                                        dirName,
                                        id,
                                        now - lastModified,
                                        cleanupDelayMs);
                                }
                                else
                                {
                                    this.logger.LogInformation(
                                            "{} Deleting state directory {} for task {} as user calling cleanup.",
                                            this.LogPrefix(),
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
                            this.logger.LogError("{} Failed to get the state directory lock.", this.LogPrefix(), e);

                            throw;
                        }
                    }
                    catch (IOException e)
                    {
                        this.logger.LogError("{} Failed to delete the state directory.", this.LogPrefix(), e);
                        if (manualUserCall)
                        {
                            throw;
                        }
                    }
                    finally
                    {
                        try
                        {
                            this.Unlock(id);
                        }
                        catch (IOException e)
                        {
                            this.logger.LogError(e, $"{this.LogPrefix()} Failed to release the state directory lock.");

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
         * List All of the task directories
         * @return The list of All the existing local directories for stream tasks
         */
        public DirectoryInfo[] ListTaskDirectories()
        {
            return !this.stateDir.Exists
                ? Array.Empty<DirectoryInfo>()
                : this.stateDir.GetDirectories().Where(pathname => pathname.Attributes == FileAttributes.Directory && PATH_NAME.IsMatch(pathname.FullName)).ToArray();
        }

        private FileStream GetOrCreateFileStream(
            TaskId taskId,
            string lockPath)
        {
            if (!this.channels.ContainsKey(taskId))
            {
                //                channels.Add(taskId, FileStream.open(lockPath, FileMode.Create, FileAccess.Write));
            }

            return this.channels[taskId];
        }

        /**
 * Get the lock for the {@link TaskId}s directory if it is available
 * @param taskId
 * @return true if successful
 * @throws IOException
 */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool Lock(TaskId taskId)
        {
            if (!this.createStateDirectory)
            {
                return true;
            }

            FileInfo lockFile;
            // we already have the lock so bail out here
            LockAndOwner lockAndOwner = this.locks[taskId];
            if (lockAndOwner != null && lockAndOwner.OwningThread.Equals(Thread.CurrentThread.Name))
            {
                this.logger.LogTrace("{} Found cached state dir lock for task {}", this.LogPrefix(), taskId);
                return true;
            }
            else if (lockAndOwner != null)
            {
                // another thread owns the lock
                return false;
            }

            try
            {
                lockFile = new FileInfo(Path.Combine(this.DirectoryForTask(taskId).FullName, LOCK_FILE_NAME));
            }
            catch (ProcessorStateException e)
            {
                // directoryForTask could be throwing an exception if another thread
                // has concurrently deleted the directory
                return false;
            }

            FileStream channel;

            try
            {
                channel = this.GetOrCreateFileStream(taskId, lockFile.FullName);
            }
            catch (FileNotFoundException)
            {
                // FileStream.open(..) could throw NoSuchFileException when there is another thread
                // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
                // file, in this case we will return immediately indicating locking failed.
                return false;
            }

            FileLock? lockedFile = this.TryLock(channel);
            if (lockedFile != null)
            {
                this.locks.Add(taskId, new LockAndOwner(Thread.CurrentThread.Name, lockedFile));

                this.logger.LogDebug($"{this.LogPrefix()} Acquired state dir lock for task {taskId}");
            }

            return lockFile != null;
        }

        private FileLock? TryLock(FileStream channel)
        {
            try
            {
                return new FileLock(channel);
            }
            catch (IOException)
            {
                return null;
            }
        }
    }
}

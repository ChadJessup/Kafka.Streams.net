using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tasks;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.State
{
    /**
     * Manages the directories where the state of Tasks owned by a {@link StreamThread} are
     * stored. Handles creation/locking/unlocking/cleaning of the Task Directories. This class is not
     * thread-safe.
     */
    public class StateDirectory
    {
        private static readonly Regex PathName = new Regex("\\d+_\\d+", RegexOptions.Compiled);
        public static readonly string LockFileName = ".lock";
        private readonly ILogger<StateDirectory> logger;

        private readonly string appId;
        private readonly DirectoryInfo stateDir;
        private readonly KafkaStreamsContext context;
        private bool hasPersistentStores;
        private readonly Dictionary<TaskId, FileStream> channels = new Dictionary<TaskId, FileStream>();
        private readonly Dictionary<TaskId, LockAndOwner> locks = new Dictionary<TaskId, LockAndOwner>();

        private FileStream? globalStateChannel;
        private FileLock? globalStateLock;

        private class LockAndOwner
        {
            public FileLock Lock { get; }
            public string OwningThread { get; }

            public LockAndOwner(string owningThread, FileLock Lock)
            {
                this.OwningThread = owningThread;
                this.Lock = Lock;
            }
        }

        /**
         * Ensures that the state base directory as well as the application's sub-directory are created.
         *
         * @param config              streams application configuration to read the root state directory path
         * @param time                system timer used to execute periodic cleanup procedure
         * @param hasPersistentStores only when the application's topology does have stores persisted on local file
         *                            system, we would go ahead and auto-create the corresponding application / task / store
         *                            directories whenever necessary; otherwise no directories would be created.
         *
         * @throws ProcessorStateException if the base state directory or application state directory does not exist
         *                                 and could not be created when hasPersistentStores is enabled.
         */
        public StateDirectory(KafkaStreamsContext context)
        {
            if (context is null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            this.context = context;
            this.logger = this.context.CreateLogger<StateDirectory>();

            this.appId = context.StreamsConfig.ApplicationId;
            var stateDirName = context.StreamsConfig.StateStoreDirectory;
            this.hasPersistentStores = context.StreamsConfig.StateStoreIsPersistent;

            DirectoryInfo baseDir = new DirectoryInfo(stateDirName.FullName);

            if (this.hasPersistentStores)
            {
                baseDir.Create();

                if (!baseDir.Exists)
                {
                    throw new ProcessorStateException(
                        $"base state directory [{stateDirName}] doesn't exist and couldn't be created");
                }
            }

            this.stateDir = new DirectoryInfo(Path.Combine(baseDir.FullName, this.appId));

            if (this.hasPersistentStores)
            {
                this.stateDir.Create();

                if (!this.stateDir.Exists)
                {
                    throw new ProcessorStateException(
                        $"state directory [{this.stateDir.FullName}] doesn't exist and couldn't be created");
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
            if (taskId is null)
            {
                throw new ArgumentNullException(nameof(taskId));
            }

            this.stateDir.Refresh();

            DirectoryInfo taskDir = new DirectoryInfo(Path.Combine(this.stateDir.FullName, taskId.ToString()));

            if (!this.stateDir.Exists)
            {
                throw new ProcessorStateException(
                    $"task directory [{taskDir.FullName}] doesn't exist and couldn't be created");
            }

            if (this.hasPersistentStores)
            {
                taskDir.Create();

                if (!taskDir.Exists)
                {
                    throw new ProcessorStateException(
                        $"task directory [{taskDir.FullName}] doesn't exist and couldn't be created");
                }
            }

            return taskDir;
        }

        /**
         * @return The File handle for the checkpoint in the given task's directory
         */
        public FileInfo CheckpointFileFor(TaskId taskId)
        {
            return new FileInfo(Path.Combine(this.DirectoryForTask(taskId).FullName, StateManagerUtil.CHECKPOINT_FILE_NAME));
        }

        /**
     * List all of the task directories
     * @return The list of all the existing local directories for stream tasks
     */
        public IEnumerable<DirectoryInfo> listAllTaskDirectories()
        {
            IEnumerable<DirectoryInfo> taskDirectories;
            if (!this.hasPersistentStores || !this.stateDir.Exists)
            {
                taskDirectories = Array.Empty<DirectoryInfo>();
            }
            else
            {
                taskDirectories =
                    this.stateDir.listFiles()
                    .Where(pathname =>
                        pathname.Attributes == FileAttributes.Directory
                        && PathName.IsMatch(pathname.FullName))
                    .Select(p => new DirectoryInfo(p.FullName));
            }

            return taskDirectories ?? Array.Empty<DirectoryInfo>();
        }

        /**
         * Decide if the directory of the task is empty or not
         */
        public bool DirectoryForTaskIsEmpty(TaskId taskId)
        {
            var taskDir = this.DirectoryForTask(taskId);

            return this.TaskDirEmpty(taskDir);
        }

        private bool TaskDirEmpty(DirectoryInfo taskDir)
        {
            var storeDirs = taskDir.EnumerateFileSystemInfos()
                .Where(pathname =>
                !pathname.Name.Equals(LockFileName)
                && !pathname.Name.Equals(StateManagerUtil.CHECKPOINT_FILE_NAME));

            // if the task is stateless, storeDirs would be null
            return storeDirs == null || storeDirs.IsEmpty();
        }

        /**
         * Get or create the directory for the global stores.
         * @return directory for the global stores
         * @throws ProcessorStateException if the global store directory does not exists and could not be created
         */
        public DirectoryInfo GlobalStateDir()
        {
            DirectoryInfo dir = new DirectoryInfo(Path.Combine(this.stateDir.FullName, "global"));

            if (this.hasPersistentStores)
            {
                dir.Create();

                if (!dir.Exists)
                {
                    throw new ProcessorStateException(
                        $"global state directory [{dir.FullName}] doesn't exist and couldn't be created");
                }
            }

            return dir;
        }

        private string LogPrefix()
        {
            return $"stream-thread [{Thread.CurrentThread.Name}]";
        }

        /**
         * Get the lock for the {@link TaskId}s directory if it is available
         * @param taskId task id
         * @return true if successful
         * @throws IOException if the file cannot be created or file handle cannot be grabbed, should be considered as fatal
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool Lock(TaskId taskId)
        {
            if (!this.hasPersistentStores)
            {
                return true;
            }

            FileInfo lockFile;

            // we already have the lock so bail out here
            if (this.locks.TryGetValue(taskId, out var lockAndOwner) && lockAndOwner.OwningThread.Equals(Thread.CurrentThread.Name))
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
                lockFile = new FileInfo(Path.Combine(this.DirectoryForTask(taskId).FullName, LockFileName));
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
                channel = this.GetOrCreateFileChannel(taskId, lockFile.FullName);
            }
            catch (DirectoryNotFoundException)
            {
                // FileChannel.open(..) could throw NoSuchFileException when there is another thread
                // concurrently deleting the parent directory (i.e. the directory of the taskId) of the lock
                // file, in this case we will return immediately indicating locking failed.
                return false;
            }

            FileLock? Lock = this.TryLock(channel);
            if (Lock != null)
            {
                this.locks.Add(taskId, new LockAndOwner(Thread.CurrentThread.Name, Lock));

                this.logger.LogDebug("{} Acquired state dir lock for task {}", this.LogPrefix(), taskId);
            }

            return Lock != null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool LockGlobalState()
        {
            if (!this.hasPersistentStores)
            {
                return true;
            }

            if (this.globalStateLock != null)
            {
                this.logger.LogTrace("{} Found cached state dir lock for the global task", this.LogPrefix());
                return true;
            }

            FileInfo lockFile = new FileInfo(Path.Combine(this.GlobalStateDir().FullName, LockFileName));
            FileStream channel;

            try
            {
                channel = new FileStream(lockFile.FullName, FileMode.Create, FileAccess.Write);
            }
            catch (FileNotFoundException)
            {
                // FileChannel.open(..) could throw NoSuchFileException when there is another thread
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

            this.logger.LogDebug("{} Acquired global state dir lock", this.LogPrefix());

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
            this.globalStateChannel?.Close();
            this.globalStateLock = null;
            this.globalStateChannel = null;

            this.logger.LogDebug("{} Released global state dir lock", this.LogPrefix());
        }

        /**
         * Unlock the state directory for the given {@link TaskId}.
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Unlock(TaskId taskId)
        {
            if (this.locks.TryGetValue(taskId, out var lockAndOwner) && lockAndOwner.OwningThread.Equals(Thread.CurrentThread.Name))
            {
                this.locks.Remove(taskId);
                lockAndOwner.Lock.Release();

                this.logger.LogDebug("{} Released state dir lock for task {}", this.LogPrefix(), taskId);

                if (this.channels.TryGetValue(taskId, out var fileChannel))
                {
                    this.channels.Remove(taskId);

                    if (fileChannel != null)
                    {
                        fileChannel.Close();
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Clean()
        {
            // remove task dirs
            try
            {
                this.CleanRemovedTasks(TimeSpan.Zero, true);
            }
            catch (Exception e)
            {
                // this is already logged within cleanRemovedTasks
                throw new StreamsException(e);
            }

            // remove global dir
            try
            {
                if (this.stateDir.Exists)
                {
                    this.GlobalStateDir().Delete(recursive: true);
                }
            }
            catch (IOException e)
            {
                this.logger.LogError("{} Failed to delete global state directory of {} due to an unexpected exception",
                    this.appId, this.LogPrefix(), e);
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
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void CleanRemovedTasks(TimeSpan cleanupDelay)
        {
            try
            {
                this.CleanRemovedTasks(cleanupDelay, false);
            }
            catch (Exception cannotHappen)
            {
                throw new InvalidOperationException("Should have swallowed exception.", cannotHappen);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void CleanRemovedTasks(TimeSpan cleanupDelayMs, bool manualUserCall)
        {
            var taskDirs = this.ListAllTaskDirectories();
            if (taskDirs == null || !taskDirs.Any())
            {
                return; // nothing to do
            }

            foreach (var taskDir in taskDirs)
            {
                string dirName = taskDir.Name;
                TaskId id = TaskId.Parse(dirName);
                if (!this.locks.ContainsKey(id))
                {
                    Exception? exception = null;
                    try
                    {
                        if (this.Lock(id))
                        {
                            var now = this.context.Clock.UtcNow;
                            DateTime lastModified = taskDir.LastWriteTimeUtc;
                            if (now > lastModified + cleanupDelayMs)
                            {
                                this.logger.LogInformation("{} Deleting obsolete state directory {} for task {} as {}ms has elapsed (cleanup delay is {}ms).",
                                    this.LogPrefix(), dirName, id, now - lastModified, cleanupDelayMs);

                                var toDelete = taskDir.EnumerateFileSystemInfos("*.*", SearchOption.AllDirectories)
                                    .Where(fi => fi.FullName != Path.Combine(taskDir.FullName, LockFileName))
                                    .ToList();

                                toDelete.ForEach(fi => fi.Delete());
                            }
                            else if (manualUserCall)
                            {
                                this.logger.LogInformation("{} Deleting state directory {} for task {} as user calling cleanup.",
                                    this.LogPrefix(), dirName, id);

                                taskDir.EnumerateFileSystemInfos("*.*", SearchOption.AllDirectories)
                                    .Where(fi => fi.FullName != Path.Combine(taskDir.FullName, LockFileName))
                                    .ToList()
                                    .ForEach(fi => fi.Delete());
                            }
                        }
                    }
                    catch (IOException e)
                    {
                        exception = e;
                    }
                    catch (OverlappingFileLockException e)
                    {
                        exception = e;
                    }
                    finally
                    {
                        try
                        {
                            this.Unlock(id);

                            // for manual user call, stream threads are not running so it is safe to delete
                            // the whole directory
                            if (manualUserCall)
                            {
                                taskDir.Delete(recursive: true);
                            }
                        }
                        catch (IOException e)
                        {
                            exception = e;
                        }
                    }

                    if (exception != null && manualUserCall)
                    {
                        this.logger.LogError("{} Failed to release the state directory lock.", this.LogPrefix());
                        throw exception;
                    }
                }
            }
        }

        /**
         * List all of the task directories that are non-empty
         * @return The list of all the non-empty local directories for stream tasks
         */
        public IEnumerable<DirectoryInfo> ListNonEmptyTaskDirectories()
        {
            IEnumerable<DirectoryInfo> taskDirectories = new List<DirectoryInfo>();
            if (!this.hasPersistentStores || !this.stateDir.Exists)
            {
                taskDirectories = Enumerable.Empty<DirectoryInfo>();
            }
            else
            {
                taskDirectories = this.stateDir.EnumerateDirectories()
                    .Where(d =>
                    {
                        if (!d.Attributes.HasFlag(FileAttributes.Directory)
                        || !PathName.IsMatch(d.FullName))
                        {
                            return false;
                        }
                        else
                        {
                            return !this.TaskDirEmpty(d);
                        }
                    });
            }

            return taskDirectories ?? Enumerable.Empty<DirectoryInfo>();
        }

        /**
         * List all of the task directories
         * @return The list of all the existing local directories for stream tasks
         */
        public IEnumerable<DirectoryInfo> ListAllTaskDirectories()
        {
            IEnumerable<DirectoryInfo> taskDirectories = new List<DirectoryInfo>();

            if (!this.hasPersistentStores || !this.stateDir.Exists)
            {
                taskDirectories = Enumerable.Empty<DirectoryInfo>();
            }
            else
            {
                taskDirectories = this.stateDir.EnumerateDirectories()
                    .Where(d => PathName.IsMatch(d.FullName));
            }

            return taskDirectories ?? Enumerable.Empty<DirectoryInfo>();
        }

        private FileStream GetOrCreateFileChannel(TaskId taskId, string lockPath)
        {
            if (!this.channels.ContainsKey(taskId))
            {
                this.channels.Put(taskId, new FileStream(lockPath, FileMode.Create, FileAccess.Write));
            }

            return this.channels[taskId];
        }

        private FileLock? TryLock(FileStream channel)
        {
            try
            {
                channel.Lock();
                return new FileLock(channel);
            }
            catch (IOException)
            {
                return null;
            }
        }
    }
}

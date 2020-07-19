using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Integration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class StateDirectoryTest : IDisposable
    {

        private readonly MockTime time = new MockTime();
        private readonly KafkaStreamsContext context;
        private DirectoryInfo stateDir;
        private readonly string applicationId = "applicationId";
        private readonly StreamsConfig config;
        private StateDirectory directory;
        private DirectoryInfo appDir;

        private void InitializeStateDirectory(bool createStateDirectory, bool withPersistentStore = true)
        {
            stateDir = new DirectoryInfo(Path.Combine(TestUtils.IO_TMP_DIR.FullName, "kafka-" + DateTime.UtcNow.Ticks));

            if (!createStateDirectory)
            {
                Dispose();
            }

            directory = CreateStateDirectory(withPersistentStore);

            appDir = new DirectoryInfo(Path.Combine(stateDir.FullName, applicationId));
        }

        private StateDirectory CreateStateDirectory(bool withPersistentStore)
        {
            var config = this.config;
            config.StateStoreIsPersistent = withPersistentStore;

            return new StateDirectory(
                new LoggerFactory().CreateLogger<StateDirectory>(),
                config);
        }

        public StateDirectoryTest()
        {
            this.config = new StreamsConfig(
                new Dictionary<string, string?>
                {
                    { StreamsConfig.ApplicationIdConfig, applicationId },
                    { StreamsConfig.BootstrapServersConfig, "dummy:1234"},
                    { StreamsConfig.StateDirPathConfig, stateDir.FullName },
                });


            this.context = new KafkaStreamsContext(
                this.config,
                null,
                null,
                null,
                null,
                null,
                new LoggerFactory(),
                new MockTime());
            InitializeStateDirectory(true);

            if (string.IsNullOrWhiteSpace(Thread.CurrentThread.Name))
            {
                Thread.CurrentThread.Name = nameof(StateDirectoryTest);
            }
        }

        public void Dispose()
        {
            stateDir.Refresh();

            if (stateDir.Exists)
            {
                stateDir.Delete(recursive: true);
            }
        }

        [Fact]
        public void ShouldCreateBaseDirectory()
        {
            Assert.True(stateDir.Exists);
            Assert.True(stateDir.Attributes.HasFlag(FileAttributes.Directory));
            Assert.True(appDir.Exists);
            Assert.True(appDir.Attributes.HasFlag(FileAttributes.Directory));
        }

        [Fact]
        public void ShouldCreateTaskStateDirectory()
        {
            TaskId taskId = new TaskId(0, 0);
            var taskDirectory = directory.DirectoryForTask(taskId);
            Assert.True(taskDirectory.Exists);
            Assert.True(taskDirectory.Attributes == FileAttributes.Directory);
        }

        [Fact]
        public void ShouldLockTaskStateDirectory()
        {
            TaskId taskId = new TaskId(0, 0);
            var taskDirectory = directory.DirectoryForTask(taskId);

            directory.Lock(taskId);

            try
            {
                FileStream channel = new FileStream(
                    Path.Combine(taskDirectory.FullName, StateDirectory.LockFileName),
                    FileMode.Create, FileAccess.Write);

                channel.Lock(0, channel.Length);
                Assert.True(false, "shouldn't be able to lock already locked directory");
            }
            catch (Exception e)
            {
                // swallow
            }
            finally
            {
                directory.Unlock(taskId);
            }
        }

        [Fact]
        public void ShouldBeTrueIfAlreadyHoldsLock()
        {
            TaskId taskId = new TaskId(0, 0);
            directory.DirectoryForTask(taskId);
            directory.Lock(taskId);
            try
            {
                Assert.True(directory.Lock(taskId));
            }
            finally
            {
                directory.Unlock(taskId);
            }
        }

        [Fact]
        public void ShouldThrowProcessorStateException()
        {
            TaskId taskId = new TaskId(0, 0);

            stateDir.Delete(recursive: true);

            Assert.Throws<ProcessorStateException>(() => directory.DirectoryForTask(taskId));
        }

        [Fact]
        public void ShouldNotLockDeletedDirectory()
        {
            TaskId taskId = new TaskId(0, 0);

            if (stateDir.Exists)
            {
                stateDir.Delete(recursive: true);
            }

            Assert.False(directory.Lock(taskId));
        }

        [Fact]
        public void ShouldLockMultipleTaskDirectories()
        {
            TaskId taskId = new TaskId(0, 0);
            var task1Dir = directory.DirectoryForTask(taskId);
            TaskId taskId2 = new TaskId(1, 0);
            var task2Dir = directory.DirectoryForTask(taskId2);

            FileStream? channel1 = null;
            FileStream? channel2 = null;

            try
            {
                channel1 = new FileStream(
                    Path.Combine(task1Dir.FullName, StateDirectory.LockFileName),
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.ReadWrite);

                channel2 = new FileStream(
                    Path.Combine(task2Dir.FullName, StateDirectory.LockFileName),
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.ReadWrite);

                directory.Lock(taskId);
                directory.Lock(taskId2);

                channel1.Lock(0, channel1.Length);
                channel2.Lock(0, channel2.Length);
                Assert.True(false, "shouldn't be able to lock already locked directory");
            }
            catch (IOException e)
            {
                Assert.True(true);
            }
            finally
            {
                directory.Unlock(taskId);
                directory.Unlock(taskId2);
                channel1?.Close();
                channel2?.Close();
            }
        }

        [Fact]
        public void ShouldReleaseTaskStateDirectoryLock()
        {
            TaskId taskId = new TaskId(0, 0);
            var taskDirectory = directory.DirectoryForTask(taskId);

            directory.Lock(taskId);
            directory.Unlock(taskId);

            FileStream channel = new FileStream(
                Path.Combine(taskDirectory.FullName, StateDirectory.LockFileName),
                FileMode.Create,
                FileAccess.Write);

            channel.Lock();

            channel.Unlock();
            channel.Close();
        }

        [Fact]
        public void ShouldCleanUpTaskStateDirectoriesThatAreNotCurrentlyLocked()
        {
            TaskId task0 = new TaskId(0, 0);
            TaskId task1 = new TaskId(1, 0);
            TaskId task2 = new TaskId(2, 0);

            try
            {
                new DirectoryInfo(Path.Combine(directory.DirectoryForTask(task0).FullName, "store")).Create();
                new DirectoryInfo(Path.Combine(directory.DirectoryForTask(task1).FullName, "store")).Create();
                new DirectoryInfo(Path.Combine(directory.DirectoryForTask(task2).FullName, "store")).Create();

                Assert.True(new DirectoryInfo(Path.Combine(directory.DirectoryForTask(task0).FullName, "store")).Exists);
                Assert.True(new DirectoryInfo(Path.Combine(directory.DirectoryForTask(task1).FullName, "store")).Exists);
                Assert.True(new DirectoryInfo(Path.Combine(directory.DirectoryForTask(task2).FullName, "store")).Exists);

                directory.Lock(task0);
                directory.Lock(task1);

                var dir0 = new DirectoryInfo(Path.Combine(appDir.FullName, task0.ToString())).FullName;
                var dir1 = new DirectoryInfo(Path.Combine(appDir.FullName, task1.ToString())).FullName;
                var dir2 = new DirectoryInfo(Path.Combine(appDir.FullName, task2.ToString())).FullName;

                var files = directory.ListAllTaskDirectories().Select(d => d.FullName);
                Assert.Equal(new[] { dir0, dir1, dir2 }, files);

                files = directory.ListNonEmptyTaskDirectories().Select(d => d.FullName);
                Assert.Equal(new[] { dir0, dir1, dir2 }, files);

                time.Sleep(5000);
                directory.CleanRemovedTasks(this.context, TimeSpan.Zero);

                files = directory.ListAllTaskDirectories().Select(d => d.FullName);
                Assert.Equal(new[] { dir0, dir1, dir2 }, files);

                files = directory.ListNonEmptyTaskDirectories().Select(d => d.FullName);
                Assert.Equal(new[] { dir0, dir1 }, files);
            }
            finally
            {
                directory.Unlock(task0);
                directory.Unlock(task1);
            }
        }

        [Fact]
        public void ShouldCleanupStateDirectoriesWhenLastModifiedIsLessThanNowMinusCleanupDelay()
        {
            var dir = directory.DirectoryForTask(new TaskId(2, 0));
            new DirectoryInfo(Path.Combine(dir.FullName, "store")).Create();
            Assert.True(new DirectoryInfo(Path.Combine(dir.FullName, "store")).Exists);

            TimeSpan cleanupDelayMs = TimeSpan.FromMilliseconds(60000);
            directory.CleanRemovedTasks(this.context, cleanupDelayMs);
            Assert.True(dir.Exists);
            Assert.Single(directory.ListAllTaskDirectories());
            Assert.Single(directory.ListNonEmptyTaskDirectories());

            var old = time.UtcNow;
            time.Sleep((long)cleanupDelayMs.TotalMilliseconds + 1000);
            var future = time.UtcNow;

            directory.CleanRemovedTasks(this.context, cleanupDelayMs);
            Assert.True(dir.Exists);
            Assert.Single(directory.ListAllTaskDirectories());
            Assert.Empty(directory.ListNonEmptyTaskDirectories());
        }

        [Fact]
        public void ShouldNotRemoveNonTaskDirectoriesAndFiles()
        {
            var otherDir = TestUtils.GetTempDirectory();// stateDir.FullName, "foo");
            directory.CleanRemovedTasks(this.context, TimeSpan.Zero);
            Assert.True(otherDir.Exists);
        }

        [Fact]
        public void ShouldCreateDirectoriesIfParentDoesntExist()
        {
            DirectoryInfo tempDir = TestUtils.GetTempDirectory();
            stateDir = new DirectoryInfo(Path.Combine(tempDir.FullName, "foo", "state-dir"));

            directory = new StateDirectory(
                new LoggerFactory().CreateLogger<StateDirectory>(),
                new StreamsConfig(
                    new Dictionary<string, string?>
                    {
                        { StreamsConfig.ApplicationIdConfig, applicationId },
                        { StreamsConfig.BootstrapServersConfig, "dummy:1234"},
                        { StreamsConfig.StateDirPathConfig, stateDir.FullName },
                        { StreamsConfig.StateDirHasPersistentStoresConfig, bool.TrueString },
                    }));

            appDir = new DirectoryInfo(Path.Combine(tempDir.FullName, applicationId));

            var taskDir = directory.DirectoryForTask(new TaskId(0, 0));
            Assert.True(stateDir.Exists);
            Assert.True(taskDir.Exists);
        }

        [Fact]
        public void ShouldLockGlobalStateDirectory()
        {
            directory.LockGlobalState();

            try
            {
                FileStream channel = new FileStream(
                    Path.Combine(directory.GlobalStateDir().FullName, StateDirectory.LockFileName),
                    FileMode.Create,
                    FileAccess.Write);

                channel.Lock();
                Assert.True(false, "Should have thrown OverlappingFileLockException");
            }
            catch (Exception)
            {
                // swallow
            }
            finally
            {
                directory.UnlockGlobalState();
            }
        }

        [Fact]
        public void ShouldUnlockGlobalStateDirectory()
        {
            directory.LockGlobalState();
            directory.UnlockGlobalState();

            FileStream channel = new FileStream(
                Path.Combine(directory.GlobalStateDir().FullName, StateDirectory.LockFileName),
                FileMode.Create,
                FileAccess.Write);

            // should lock without any exceptions
            channel.Lock();

            // unlock for test cleanup
            channel.Unlock();
            channel.Close();
        }

        [Fact]
        public void ShouldNotLockStateDirLockedByAnotherThread()
        {
            TaskId taskId = new TaskId(0, 0);
            IOException? exceptionOnThread = null;

            Thread thread = new Thread(() =>
            {
                try
                {
                    directory.Lock(taskId);
                }
                catch (IOException e)
                {
                    Interlocked.Exchange(ref exceptionOnThread, e);
                }
            })
            {
                Name = "TestThread"
            };

            thread.Start();
            thread.Join(30000);

            // should not have had an exception during locking on other thread
            Assert.Null(exceptionOnThread);
            Assert.False(directory.Lock(taskId));

            // Need to unlock dir for test cleanup - this could be done better
            // but, i'm lazy atm.
            // TODO: double-latch
            thread = new Thread(() =>
            {
                try
                {
                    directory.Unlock(taskId);
                }
                catch (IOException e)
                {
                    Interlocked.Exchange(ref exceptionOnThread, e);
                }
            })
            {
                Name = "TestThread"
            };

            thread.Start();
            thread.Join(30000);

            Assert.Null(exceptionOnThread);
        }

        [Fact]
        public void ShouldNotUnLockStateDirLockedByAnotherThread()
        {
            TaskId taskId = new TaskId(0, 0);
            CountdownEvent lockLatch = new CountdownEvent(1);
            CountdownEvent unlockLatch = new CountdownEvent(1);
            Exception? exceptionOnThread = null;

            Thread thread = new Thread(() =>
            {
                try
                {
                    directory.Lock(taskId);
                    lockLatch.Signal();
                    unlockLatch.Wait(TimeSpan.FromMinutes(1.0));
                    directory.Unlock(taskId);
                }
                catch (Exception e)
                {
                    Interlocked.Exchange(ref exceptionOnThread, e);
                }
            })
            {
                Name = "TestThread"
            };

            thread.Start();
            lockLatch.Wait(TimeSpan.FromSeconds(5.0));

            Assert.Null(exceptionOnThread);
            directory.Unlock(taskId);
            Assert.False(directory.Lock(taskId));

            unlockLatch.Signal();
            thread.Join(30000);

            Assert.Null(exceptionOnThread);
            Assert.True(directory.Lock(taskId));
            directory.Unlock(taskId);
        }

        [Fact]
        public void ShouldCleanupAllTaskDirectoriesIncludingGlobalOne()
        {
            TaskId id = new TaskId(1, 0);
            directory.DirectoryForTask(id);
            directory.GlobalStateDir();

            var dir0 = new DirectoryInfo(Path.Combine(appDir.FullName, id.ToString())).FullName;
            var globalDir = new DirectoryInfo(Path.Combine(appDir.FullName, "global")).FullName;

            var appDirFiles = appDir.listFiles()
                .Select(p => p.FullName);

            Assert.Equal(new[] { dir0, globalDir }, appDirFiles);

            directory.Clean(this.context);

            Assert.Empty(appDir.listFiles());
        }

        [Fact]
        public void ShouldNotCreateBaseDirectory()
        {
            InitializeStateDirectory(false, withPersistentStore: false);

            stateDir.Refresh();
            appDir.Refresh();

            Assert.False(stateDir.Exists);
            Assert.False(appDir.Exists);
        }

        [Fact]
        public void ShouldNotCreateTaskStateDirectory()
        {
            InitializeStateDirectory(false, withPersistentStore: false);
            TaskId taskId = new TaskId(0, 0);
            Assert.Throws<ProcessorStateException>(() => directory.DirectoryForTask(taskId));
        }

        [Fact]
        public void ShouldNotCreateGlobalStateDirectory()
        {
            InitializeStateDirectory(createStateDirectory: false, withPersistentStore: false);
            var globalStateDir = directory.GlobalStateDir();
            Assert.False(globalStateDir.Exists);
        }

        [Fact]
        public void ShouldLockTaskStateDirectoryWhenDirectoryCreationDisabled()
        {
            InitializeStateDirectory(false);
            TaskId taskId = new TaskId(0, 0);
            Assert.True(directory.Lock(taskId));

            directory.Unlock(taskId);
        }

        [Fact]
        public void ShouldLockGlobalStateDirectoryWhenDirectoryCreationDisabled()
        {
            InitializeStateDirectory(false);
            Assert.True(directory.LockGlobalState());

            directory.UnlockGlobalState();
        }
    }
}

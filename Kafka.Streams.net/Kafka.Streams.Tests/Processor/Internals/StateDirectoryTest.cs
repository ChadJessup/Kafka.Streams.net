using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Integration;
using Kafka.Streams.Tests.Processor.Internals.Assignment;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class StateDirectoryTest
    {

        private MockTime time = new MockTime();
        private DirectoryInfo stateDir;
        private readonly string applicationId = "applicationId";
        private StateDirectory directory;
        private DirectoryInfo appDir;

        private void InitializeStateDirectory(bool createStateDirectory)
        {// throws Exception
            stateDir = new DirectoryInfo(TestUtils.IO_TMP_DIR, "kafka-" + TestUtils.randomString(5));
            if (!createStateDirectory)
            {
                cleanup();
            }
            //            directory = new StateDirectory(
            //                new StreamsConfig(new StreamsConfig() {
            //                {
            //                    Put(StreamsConfig.ApplicationIdConfig, applicationId);
            //            Put(StreamsConfig.BootstrapServersConfig, "dummy:1234");
            //            Put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
            //        }
            //    }),
            //            time, createStateDirectory);
            appDir = new DirectoryInfo(Path.Combine(stateDir.FullName, applicationId));
        }


        public void Before()
        {// throws Exception
            InitializeStateDirectory(true);
        }


        public void Cleanup()
        {// throws Exception
            Utils.Delete(stateDir);
        }

        [Fact]
        public void ShouldCreateBaseDirectory()
        {
            Assert.True(stateDir.Exists);
            Assert.True(stateDir.isDirectory());
            Assert.True(appDir.Exists);
            Assert.True(appDir.isDirectory());
        }

        [Fact]
        public void ShouldCreateTaskStateDirectory()
        {
            TaskId taskId = new TaskId(0, 0);
            var taskDirectory = directory.DirectoryForTask(taskId);
            Assert.True(taskDirectory.Exists);
            Assert.True(taskDirectory.isDirectory());
        }

        [Fact]
        public void ShouldLockTaskStateDirectory()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);
            var taskDirectory = directory.DirectoryForTask(taskId);

            directory.Lock(taskId);

            try
            {
                FileStream channel = new FileStream(
                    Path.Combine(taskDirectory.FullName, StateDirectory.LOCK_FILE_NAME),
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
        {// throws Exception
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
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);

            stateDir.Delete(recursive: true);

            try
            {
                directory.DirectoryForTask(taskId);
                Assert.True(false, "Should have thrown ProcessorStateException");
            }
            catch (ProcessorStateException)
            {
                // swallow
            }
        }

        [Fact]
        public void ShouldNotLockDeletedDirectory()
        {
            TaskId taskId = new TaskId(0, 0);

            stateDir.Delete(recursive: true);
            Assert.False(directory.Lock(taskId));
        }

        [Fact]
        public void ShouldLockMultipleTaskDirectories()
        {
            TaskId taskId = new TaskId(0, 0);
            var task1Dir = directory.DirectoryForTask(taskId);
            TaskId taskId2 = new TaskId(1, 0);
            var task2Dir = directory.DirectoryForTask(taskId2);

            try
            {
                FileStream channel1 = new FileStream(
                    Path.Combine(task1Dir.FullName, StateDirectory.LOCK_FILE_NAME),
                    FileMode.Create,
                    FileAccess.Write);

                FileStream channel2 = new FileStream(Path.Combine(task2Dir.FullName, StateDirectory.LOCK_FILE_NAME),
                    FileMode.Create,
                    FileAccess.Write);

                directory.Lock(taskId);
                directory.Lock(taskId2);

                channel1.Lock(0, channel1.Length);
                channel2.Lock(0, channel2.Length);
                Assert.True(false, "shouldn't be able to lock already locked directory");
            }
            catch (IOException)
            {
                Assert.True(true);
            }
            finally
            {
                directory.Unlock(taskId);
                directory.Unlock(taskId2);
            }
        }

        [Fact]
        public void ShouldReleaseTaskStateDirectoryLock()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);
            var taskDirectory = directory.DirectoryForTask(taskId);

            directory.Lock(taskId);
            directory.Unlock(taskId);

            FileStream channel = new FileStream(
                Path.Combine(taskDirectory.FullName, StateDirectory.LOCK_FILE_NAME),
                FileMode.Create,
                FileAccess.Write);

            channel.Lock(0, channel.Length);
        }

        [Fact]
        public void ShouldCleanUpTaskStateDirectoriesThatAreNotCurrentlyLocked()
        {// throws Exception
            TaskId task0 = new TaskId(0, 0);
            TaskId task1 = new TaskId(1, 0);
            try
            {
                directory.Lock(task0);
                directory.Lock(task1);
                directory.DirectoryForTask(new TaskId(2, 0));

                var files = appDir.listFiles();
                Assert.Equal(3, files.Count());

                time.Sleep(1000);
                directory.CleanRemovedTasks(0);

                files = appDir.listFiles();
                Assert.Equal(2, files.Count());
                Assert.Contains(new FileInfo(Path.Combine(appDir.FullName, task0.ToString())), files);
                Assert.Contains(new FileInfo(Path.Combine(appDir.FullName, task1.ToString())), files);
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
            int cleanupDelayMs = 60000;
            directory.CleanRemovedTasks(cleanupDelayMs);
            Assert.True(dir.Exists);

            time.Sleep(cleanupDelayMs + 1000);
            directory.CleanRemovedTasks(cleanupDelayMs);
            Assert.False(dir.Exists);
        }

        [Fact]
        public void ShouldNotRemoveNonTaskDirectoriesAndFiles()
        {
            var otherDir = TestUtils.GetTempDirectory(stateDir.FullName, "foo");
            directory.CleanRemovedTasks(0);
            Assert.True(otherDir.Exists);
        }

        [Fact]
        public void ShouldListAllTaskDirectories()
        {
            TestUtils.GetTempDirectory(stateDir.FullName, "foo");
            var taskDir1 = directory.DirectoryForTask(new TaskId(0, 0));
            var taskDir2 = directory.DirectoryForTask(new TaskId(0, 1));

            List<DirectoryInfo> dirs = Arrays.asList(directory.ListTaskDirectories());
            Assert.Equal(2, dirs.Count);
            Assert.Contains(taskDir1, dirs);
            Assert.Contains(taskDir2, dirs);
        }

        [Fact]
        public void ShouldCreateDirectoriesIfParentDoesntExist()
        {
            DirectoryInfo tempDir = TestUtils.GetTempDirectory();
            DirectoryInfo stateDir = new DirectoryInfo(Path.Combine(tempDir.FullName, "foo", "state-dir"));
            StateDirectory stateDirectory = new StateDirectory(
                null,
            new StreamsConfig
            {
                ApplicationId = applicationId,
                BootstrapServers = "dummy:1234",
                StateStoreDirectory = stateDir,
            },
            time);

            var taskDir = stateDirectory.DirectoryForTask(new TaskId(0, 0));
            Assert.True(stateDir.Exists);
            Assert.True(taskDir.Exists);
        }

        [Fact]
        public void ShouldLockGlobalStateDirectory()
        {// throws Exception
            directory.LockGlobalState();

            try
            {
                FileStream channel = new FileStream(
                    Path.Combine(directory.GlobalStateDir().FullName, StateDirectory.LOCK_FILE_NAME),
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
                Path.Combine(directory.GlobalStateDir().FullName, StateDirectory.LOCK_FILE_NAME),
                FileMode.Create,
                FileAccess.Write);

            // should lock without any exceptions
            channel.Lock();
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
            });

            thread.Start();
            thread.Join(30000);

            // should not have had an exception during locking on other thread
            Assert.Null(exceptionOnThread);
            Assert.False(directory.Lock(taskId));
        }

        [Fact]
        public void ShouldNotUnLockStateDirLockedByAnotherThread()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);
            CountDownLatch lockLatch = new CountDownLatch(1);
            CountDownLatch unlockLatch = new CountDownLatch(1);
            Exception? exceptionOnThread = null;

            Thread thread = new Thread(() =>
            {
                try
                {
                    directory.Lock(taskId);
                    lockLatch.countDown();
                    unlockLatch.wait();
                    directory.Unlock(taskId);
                }
                catch (Exception e)
                {
                    Interlocked.Exchange(ref exceptionOnThread, e);
                }
            });

            thread.Start();
            lockLatch.wait(5, TimeUnit.SECONDS);

            Assert.Null("should not have had an exception on other thread", exceptionOnThread.Get());
            directory.Unlock(taskId);
            Assert.False(directory.Lock(taskId));

            unlockLatch.countDown();
            thread.Join(30000);

            Assert.Null("should not have had an exception on other thread", exceptionOnThread.Get());
            Assert.True(directory.Lock(taskId));
        }

        [Fact]
        public void ShouldCleanupAllTaskDirectoriesIncludingGlobalOne()
        {
            directory.DirectoryForTask(new TaskId(1, 0));
            directory.GlobalStateDir();

            var files = Arrays.asList(appDir.listFiles());
            Assert.Equal(2, files.Count);

            directory.Clean();

            files = Arrays.asList(appDir.listFiles());
            Assert.Empty(files);
        }

        [Fact]
        public void ShouldNotCreateBaseDirectory()
        {// throws Exception
            InitializeStateDirectory(false);
            Assert.False(stateDir.Exists);
            Assert.False(appDir.Exists);
        }

        [Fact]
        public void ShouldNotCreateTaskStateDirectory()
        {// throws Exception
            InitializeStateDirectory(false);
            TaskId taskId = new TaskId(0, 0);
            var taskDirectory = directory.DirectoryForTask(taskId);
            Assert.False(taskDirectory.Exists);
        }

        [Fact]
        public void ShouldNotCreateGlobalStateDirectory()
        {// throws Exception
            InitializeStateDirectory(false);
            var globalStateDir = directory.GlobalStateDir();
            Assert.False(globalStateDir.Exists);
        }

        [Fact]
        public void ShouldLockTaskStateDirectoryWhenDirectoryCreationDisabled()
        {// throws Exception
            InitializeStateDirectory(false);
            TaskId taskId = new TaskId(0, 0);
            Assert.True(directory.Lock(taskId));
        }

        [Fact]
        public void ShouldLockGlobalStateDirectoryWhenDirectoryCreationDisabled()
        {// throws Exception
            InitializeStateDirectory(false);
            Assert.True(directory.LockGlobalState());
        }
    }
}

using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class StateDirectoryTest
    {

        private MockTime time = new MockTime();
        private File stateDir;
        private readonly string applicationId = "applicationId";
        private StateDirectory directory;
        private readonly File appDir;

        private void InitializeStateDirectory(bool createStateDirectory)
        {// throws Exception
            stateDir = new FileInfo(TestUtils.IO_TMP_DIR, "kafka-" + TestUtils.randomString(5));
            if (!createStateDirectory)
            {
                cleanup();
            }
            //            directory = new StateDirectory(
            //                new StreamsConfig(new StreamsConfig() {
            //                {
            //                    Put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            //            Put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
            //            Put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
            //        }
            //    }),
            //            time, createStateDirectory);
            appDir = new FileInfo(stateDir, applicationId);
        }


        public void Before()
        {// throws Exception
            initializeStateDirectory(true);
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
            File taskDirectory = directory.directoryForTask(taskId);
            Assert.True(taskDirectory.Exists);
            Assert.True(taskDirectory.isDirectory());
        }

        [Fact]
        public void ShouldLockTaskStateDirectory()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);
            File taskDirectory = directory.directoryForTask(taskId);

            directory.Lock(taskId);

            try
            {
                FileChannel channel = FileChannel.open(
                    new FileInfo(taskDirectory, StateDirectory.LOCK_FILE_NAME).toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)


                    channel.tryLock();
                Assert.True(false, "shouldn't be able to lock already locked directory");
            }
            catch (OverlappingFileLockException e)
            {
                // swallow
            }
            finally
            {
                directory.unlock(taskId);
            }
        }

        [Fact]
        public void ShouldBeTrueIfAlreadyHoldsLock()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);
            directory.directoryForTask(taskId);
            directory.Lock(taskId);
            try
            {
                Assert.True(directory.Lock(taskId));
            }
            finally
            {
                directory.unlock(taskId);
            }
        }

        [Fact]
        public void ShouldThrowProcessorStateException()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);

            Utils.Delete(stateDir);

            try
            {
                directory.directoryForTask(taskId);
                Assert.True(false, "Should have thrown ProcessorStateException");
            }
            catch (ProcessorStateException expected)
            {
                // swallow
            }
        }

        [Fact]
        public void ShouldNotLockDeletedDirectory()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);

            Utils.Delete(stateDir);
            Assert.False(directory.Lock(taskId));
        }

        [Fact]
        public void ShouldLockMultipleTaskDirectories()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);
            var task1Dir = directory.directoryForTask(taskId);
            TaskId taskId2 = new TaskId(1, 0);
            var task2Dir = directory.directoryForTask(taskId2);


            FileChannel channel1 = FileChannel.open(
                new FileInfo(task1Dir, StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
            FileChannel channel2 = FileChannel.open(new FileInfo(task2Dir, StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);

            directory.Lock(taskId);
            directory.Lock(taskId2);

            channel1.tryLock();
            channel2.tryLock();
            Assert.True(false, "shouldn't be able to lock already locked directory");

            // was finally
            directory.unlock(taskId);
            directory.unlock(taskId2);
        }

        [Fact]
        public void ShouldReleaseTaskStateDirectoryLock()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);
            var taskDirectory = directory.directoryForTask(taskId);

            directory.Lock(taskId);
            directory.unlock(taskId);

            // FileChannel channel = FileChannel.open(
            //     new FileInfo(taskDirectory, StateDirectory.LOCK_FILE_NAME).toPath(),
            //     StandardOpenOption.CREATE,
            //     StandardOpenOption.WRITE))
            // {
            //     channel.tryLock();
            // }
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
                directory.directoryForTask(new TaskId(2, 0));

                List<FileInfo> files = Arrays.asList(Objects.requireNonNull(appDir.listFiles()));
                Assert.Equal(3, files.Count);

                time.Sleep(1000);
                directory.cleanRemovedTasks(0);

                files = Arrays.asList(Objects.requireNonNull(appDir.listFiles()));
                Assert.Equal(2, files.Count);
                Assert.True(files.Contains(new FileInfo(appDir, task0.ToString())));
                Assert.True(files.Contains(new FileInfo(appDir, task1.ToString())));
            }
            finally
            {
                directory.unlock(task0);
                directory.unlock(task1);
            }
        }

        [Fact]
        public void ShouldCleanupStateDirectoriesWhenLastModifiedIsLessThanNowMinusCleanupDelay()
        {
            File dir = directory.directoryForTask(new TaskId(2, 0));
            int cleanupDelayMs = 60000;
            directory.cleanRemovedTasks(cleanupDelayMs);
            Assert.True(dir.Exists);

            time.Sleep(cleanupDelayMs + 1000);
            directory.cleanRemovedTasks(cleanupDelayMs);
            Assert.False(dir.Exists);
        }

        [Fact]
        public void ShouldNotRemoveNonTaskDirectoriesAndFiles()
        {
            File otherDir = TestUtils.GetTempDirectory(stateDir.toPath(), "foo");
            directory.cleanRemovedTasks(0);
            Assert.True(otherDir.Exists);
        }

        [Fact]
        public void ShouldListAllTaskDirectories()
        {
            TestUtils.GetTempDirectory(stateDir.toPath(), "foo");
            File taskDir1 = directory.directoryForTask(new TaskId(0, 0));
            File taskDir2 = directory.directoryForTask(new TaskId(0, 1));

            List<File> dirs = Arrays.asList(directory.listTaskDirectories());
            Assert.Equal(2, dirs.Count);
            Assert.True(dirs.Contains(taskDir1));
            Assert.True(dirs.Contains(taskDir2));
        }

        [Fact]
        public void ShouldCreateDirectoriesIfParentDoesntExist()
        {
            DirectoryInfo tempDir = TestUtils.GetTempDirectory();
            DirectoryInfo stateDir = new DirectoryInfo(Path.Combine(tempDir.FullName, "foo", "state-dir");
            //            StateDirectory stateDirectory = new StateDirectory(
            //                new StreamsConfig(new StreamsConfig() {
            //                {
            //                    Put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            //            Put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
            //            Put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
            //        }
            //    }),
            //            time, true);
            var taskDir = stateDirectory.directoryForTask(new TaskId(0, 0));
            Assert.True(stateDir.Exists);
            Assert.True(taskDir.Exists);
        }

        [Fact]
        public void ShouldLockGlobalStateDirectory()
        {// throws Exception
            directory.lockGlobalState();

            try
            {
                FileChannel channel = FileChannel.open(
                    new FileInfo(directory.globalStateDir(), StateDirectory.LOCK_FILE_NAME).toPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
                channel.Lock();
                Assert.True(false, "Should have thrown OverlappingFileLockException");
            }
            catch (OverlappingFileLockException expcted)
            {
                // swallow
            }
            finally
            {
                directory.unlockGlobalState();
            }
        }

        [Fact]
        public void ShouldUnlockGlobalStateDirectory()
        {// throws Exception
            directory.lockGlobalState();
            directory.unlockGlobalState();

            //try (
            //    FileChannel channel = FileChannel.open(
            //        new FileInfo(directory.globalStateDir(), StateDirectory.LOCK_FILE_NAME).toPath(),
            //        StandardOpenOption.CREATE,
            //        StandardOpenOption.WRITE)
            //    ) {
            //    // should lock without any exceptions
            //    channel.Lock();
            //}
        }

        [Fact]
        public void ShouldNotLockStateDirLockedByAnotherThread()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);
            AtomicReference<IOException> exceptionOnThread = new AtomicReference<>();
            Thread thread = new Thread(() =>
            {
                try
                {
                    directory.Lock(taskId);
                }
                catch (IOException e)
                {
                    exceptionOnThread.set(e);
                }
            });

            thread.start();
            thread.Join(30000);
            Assert.Null("should not have had an exception during locking on other thread", exceptionOnThread.Get());
            Assert.False(directory.Lock(taskId));
        }

        [Fact]
        public void ShouldNotUnLockStateDirLockedByAnotherThread()
        {// throws Exception
            TaskId taskId = new TaskId(0, 0);
            CountDownLatch lockLatch = new CountDownLatch(1);
            CountDownLatch unlockLatch = new CountDownLatch(1);
            AtomicReference<Exception> exceptionOnThread = new AtomicReference<>();
            Thread thread = new Thread(() =>
            {
                try
                {
                    directory.Lock(taskId);
                    lockLatch.countDown();
                    unlockLatch.await();
                    directory.unlock(taskId);
                }
                catch (Exception e)
                {
                    exceptionOnThread.set(e);
                }
            });
            thread.start();
            lockLatch.await(5, TimeUnit.SECONDS);

            Assert.Null("should not have had an exception on other thread", exceptionOnThread.Get());
            directory.unlock(taskId);
            Assert.False(directory.Lock(taskId));

            unlockLatch.countDown();
            thread.Join(30000);

            Assert.Null("should not have had an exception on other thread", exceptionOnThread.Get());
            Assert.True(directory.Lock(taskId));
        }

        [Fact]
        public void ShouldCleanupAllTaskDirectoriesIncludingGlobalOne()
        {
            directory.directoryForTask(new TaskId(1, 0));
            directory.globalStateDir();

            List<File> files = Arrays.asList(Objects.requireNonNull(appDir.listFiles()));
            Assert.Equal(2, files.Count);

            directory.clean();

            files = Arrays.asList(Objects.requireNonNull(appDir.listFiles()));
            Assert.Equal(0, files.Count);
        }

        [Fact]
        public void ShouldNotCreateBaseDirectory()
        {// throws Exception
            initializeStateDirectory(false);
            Assert.False(stateDir.Exists);
            Assert.False(appDir.Exists);
        }

        [Fact]
        public void ShouldNotCreateTaskStateDirectory()
        {// throws Exception
            initializeStateDirectory(false);
            TaskId taskId = new TaskId(0, 0);
            File taskDirectory = directory.directoryForTask(taskId);
            Assert.False(taskDirectory.Exists);
        }

        [Fact]
        public void ShouldNotCreateGlobalStateDirectory()
        {// throws Exception
            initializeStateDirectory(false);
            File globalStateDir = directory.globalStateDir();
            Assert.False(globalStateDir.Exists);
        }

        [Fact]
        public void ShouldLockTaskStateDirectoryWhenDirectoryCreationDisabled()
        {// throws Exception
            initializeStateDirectory(false);
            TaskId taskId = new TaskId(0, 0);
            Assert.True(directory.Lock(taskId));
        }

        [Fact]
        public void ShouldLockGlobalStateDirectoryWhenDirectoryCreationDisabled()
        {// throws Exception
            initializeStateDirectory(false);
            Assert.True(directory.lockGlobalState());
        }
    }
}
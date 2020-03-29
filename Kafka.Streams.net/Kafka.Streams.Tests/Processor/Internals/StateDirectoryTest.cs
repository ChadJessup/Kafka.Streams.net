using Kafka.Streams.Configs;
using Kafka.Streams.Tasks;
using System.IO;
using System.Threading;
using Xunit;

public class StateDirectoryTest {

    private MockTime time = new MockTime();
    private File stateDir;
    private string applicationId = "applicationId";
    private StateDirectory directory;
    private File appDir;

    private void initializeStateDirectory(bool createStateDirectory) {// throws Exception
        stateDir = new File(TestUtils.IO_TMP_DIR, "kafka-" + TestUtils.randomString(5));
        if (!createStateDirectory) {
            cleanup();
        }
        directory = new StateDirectory(
            new StreamsConfig(new Properties() {
                {
                    put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                    put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
                }
            }),
            time, createStateDirectory);
        appDir = new File(stateDir, applicationId);
    }

    
    public void before() {// throws Exception
        initializeStateDirectory(true);
    }

    
    public void cleanup() {// throws Exception
        Utils.delete(stateDir);
    }

    [Xunit.Fact]
    public void shouldCreateBaseDirectory() {
        Assert.True(stateDir.exists());
        Assert.True(stateDir.isDirectory());
        Assert.True(appDir.exists());
        Assert.True(appDir.isDirectory());
    }

    [Xunit.Fact]
    public void shouldCreateTaskStateDirectory() {
        TaskId taskId = new TaskId(0, 0);
        File taskDirectory = directory.directoryForTask(taskId);
        Assert.True(taskDirectory.exists());
        Assert.True(taskDirectory.isDirectory());
    }

    [Xunit.Fact]
    public void shouldLockTaskStateDirectory() {// throws Exception
        TaskId taskId = new TaskId(0, 0);
        File taskDirectory = directory.directoryForTask(taskId);

        directory.Lock(taskId);

        try (
            FileChannel channel = FileChannel.open(
                new File(taskDirectory, StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        ) {
            channel.tryLock();
            Assert.True(false, "shouldn't be able to lock already locked directory");
        } catch (OverlappingFileLockException e) {
            // swallow
        } finally {
            directory.unlock(taskId);
        }
    }

    [Xunit.Fact]
    public void shouldBeTrueIfAlreadyHoldsLock() {// throws Exception
        TaskId taskId = new TaskId(0, 0);
        directory.directoryForTask(taskId);
        directory.Lock(taskId);
        try {
            Assert.True(directory.Lock(taskId));
        } finally {
            directory.unlock(taskId);
        }
    }

    [Xunit.Fact]
    public void shouldThrowProcessorStateException() {// throws Exception
        TaskId taskId = new TaskId(0, 0);

        Utils.delete(stateDir);

        try {
            directory.directoryForTask(taskId);
            Assert.True(false, "Should have thrown ProcessorStateException");
        } catch (ProcessorStateException expected) {
            // swallow
        }
    }

    [Xunit.Fact]
    public void shouldNotLockDeletedDirectory() {// throws Exception
        TaskId taskId = new TaskId(0, 0);

        Utils.delete(stateDir);
        Assert.False(directory.Lock(taskId));
    }
    
    [Xunit.Fact]
    public void shouldLockMultipleTaskDirectories() {// throws Exception
        TaskId taskId = new TaskId(0, 0);
        File task1Dir = directory.directoryForTask(taskId);
        TaskId taskId2 = new TaskId(1, 0);
        File task2Dir = directory.directoryForTask(taskId2);


        try (
            FileChannel channel1 = FileChannel.open(
                new File(task1Dir, StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
            FileChannel channel2 = FileChannel.open(new File(task2Dir, StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            directory.Lock(taskId);
            directory.Lock(taskId2);

            channel1.tryLock();
            channel2.tryLock();
            Assert.True(false, "shouldn't be able to lock already locked directory");
        } catch (OverlappingFileLockException e) {
            // swallow
        } finally {
            directory.unlock(taskId);
            directory.unlock(taskId2);
        }
    }

    [Xunit.Fact]
    public void shouldReleaseTaskStateDirectoryLock() {// throws Exception
        TaskId taskId = new TaskId(0, 0);
        File taskDirectory = directory.directoryForTask(taskId);

        directory.Lock(taskId);
        directory.unlock(taskId);

        try (
            FileChannel channel = FileChannel.open(
                new File(taskDirectory, StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            channel.tryLock();
        }
    }

    [Xunit.Fact]
    public void shouldCleanUpTaskStateDirectoriesThatAreNotCurrentlyLocked() {// throws Exception
        TaskId task0 = new TaskId(0, 0);
        TaskId task1 = new TaskId(1, 0);
        try {
            directory.Lock(task0);
            directory.Lock(task1);
            directory.directoryForTask(new TaskId(2, 0));

            List<File> files = Array.asList(Objects.requireNonNull(appDir.listFiles()));
            Assert.Equal(3, files.Count);

            time.sleep(1000);
            directory.cleanRemovedTasks(0);

            files = Array.asList(Objects.requireNonNull(appDir.listFiles()));
            Assert.Equal(2, files.Count);
            Assert.True(files.Contains(new File(appDir, task0.toString())));
            Assert.True(files.Contains(new File(appDir, task1.toString())));
        } finally {
            directory.unlock(task0);
            directory.unlock(task1);
        }
    }

    [Xunit.Fact]
    public void shouldCleanupStateDirectoriesWhenLastModifiedIsLessThanNowMinusCleanupDelay() {
        File dir = directory.directoryForTask(new TaskId(2, 0));
        int cleanupDelayMs = 60000;
        directory.cleanRemovedTasks(cleanupDelayMs);
        Assert.True(dir.exists());

        time.sleep(cleanupDelayMs + 1000);
        directory.cleanRemovedTasks(cleanupDelayMs);
        Assert.False(dir.exists());
    }

    [Xunit.Fact]
    public void shouldNotRemoveNonTaskDirectoriesAndFiles() {
        File otherDir = TestUtils.tempDirectory(stateDir.toPath(), "foo");
        directory.cleanRemovedTasks(0);
        Assert.True(otherDir.exists());
    }

    [Xunit.Fact]
    public void shouldListAllTaskDirectories() {
        TestUtils.tempDirectory(stateDir.toPath(), "foo");
        File taskDir1 = directory.directoryForTask(new TaskId(0, 0));
        File taskDir2 = directory.directoryForTask(new TaskId(0, 1));

        List<File> dirs = Array.asList(directory.listTaskDirectories());
        Assert.Equal(2, dirs.Count);
        Assert.True(dirs.Contains(taskDir1));
        Assert.True(dirs.Contains(taskDir2));
    }

    [Xunit.Fact]
    public void shouldCreateDirectoriesIfParentDoesntExist() {
        File tempDir = TestUtils.tempDirectory();
        File stateDir = new File(new File(tempDir, "foo"), "state-dir");
        StateDirectory stateDirectory = new StateDirectory(
            new StreamsConfig(new Properties() {
                {
                    put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                    put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
                }
            }),
            time, true);
        File taskDir = stateDirectory.directoryForTask(new TaskId(0, 0));
        Assert.True(stateDir.exists());
        Assert.True(taskDir.exists());
    }

    [Xunit.Fact]
    public void shouldLockGlobalStateDirectory() {// throws Exception
        directory.lockGlobalState();

        try (
            FileChannel channel = FileChannel.open(
                new File(directory.globalStateDir(), StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            channel.Lock();
            Assert.True(false, "Should have thrown OverlappingFileLockException");
        } catch (OverlappingFileLockException expcted) {
            // swallow
        } finally {
            directory.unlockGlobalState();
        }
    }

    [Xunit.Fact]
    public void shouldUnlockGlobalStateDirectory() {// throws Exception
        directory.lockGlobalState();
        directory.unlockGlobalState();

        try (
            FileChannel channel = FileChannel.open(
                new File(directory.globalStateDir(), StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            // should lock without any exceptions
            channel.Lock();
        }
    }

    [Xunit.Fact]
    public void shouldNotLockStateDirLockedByAnotherThread() {// throws Exception
        TaskId taskId = new TaskId(0, 0);
        AtomicReference<IOException> exceptionOnThread = new AtomicReference<>();
        Thread thread = new Thread(() => {
            try {
                directory.Lock(taskId);
            } catch (IOException e) {
                exceptionOnThread.set(e);
            }
        });
        thread.start();
        thread.join(30000);
        assertNull("should not have had an exception during locking on other thread", exceptionOnThread.get());
        Assert.False(directory.Lock(taskId));
    }

    [Xunit.Fact]
    public void shouldNotUnLockStateDirLockedByAnotherThread() {// throws Exception
        TaskId taskId = new TaskId(0, 0);
        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch unlockLatch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionOnThread = new AtomicReference<>();
        Thread thread = new Thread(() => {
            try {
                directory.Lock(taskId);
                lockLatch.countDown();
                unlockLatch.await();
                directory.unlock(taskId);
            } catch (Exception e) {
                exceptionOnThread.set(e);
            }
        });
        thread.start();
        lockLatch.await(5, TimeUnit.SECONDS);

        assertNull("should not have had an exception on other thread", exceptionOnThread.get());
        directory.unlock(taskId);
        Assert.False(directory.Lock(taskId));

        unlockLatch.countDown();
        thread.join(30000);

        assertNull("should not have had an exception on other thread", exceptionOnThread.get());
        Assert.True(directory.Lock(taskId));
    }

    [Xunit.Fact]
    public void shouldCleanupAllTaskDirectoriesIncludingGlobalOne() {
        directory.directoryForTask(new TaskId(1, 0));
        directory.globalStateDir();

        List<File> files = Array.asList(Objects.requireNonNull(appDir.listFiles()));
        Assert.Equal(2, files.Count);

        directory.clean();

        files = Array.asList(Objects.requireNonNull(appDir.listFiles()));
        Assert.Equal(0, files.Count);
    }

    [Xunit.Fact]
    public void shouldNotCreateBaseDirectory() {// throws Exception
        initializeStateDirectory(false);
        Assert.False(stateDir.exists());
        Assert.False(appDir.exists());
    }

    [Xunit.Fact]
    public void shouldNotCreateTaskStateDirectory() {// throws Exception
        initializeStateDirectory(false);
        TaskId taskId = new TaskId(0, 0);
        File taskDirectory = directory.directoryForTask(taskId);
        Assert.False(taskDirectory.exists());
    }

    [Xunit.Fact]
    public void shouldNotCreateGlobalStateDirectory() {// throws Exception
        initializeStateDirectory(false);
        File globalStateDir = directory.globalStateDir();
        Assert.False(globalStateDir.exists());
    }

    [Xunit.Fact]
    public void shouldLockTaskStateDirectoryWhenDirectoryCreationDisabled() {// throws Exception
        initializeStateDirectory(false);
        TaskId taskId = new TaskId(0, 0);
        Assert.True(directory.Lock(taskId));
    }

    [Xunit.Fact]
    public void shouldLockGlobalStateDirectoryWhenDirectoryCreationDisabled() {// throws Exception
        initializeStateDirectory(false);
        Assert.True(directory.lockGlobalState());
    }
}
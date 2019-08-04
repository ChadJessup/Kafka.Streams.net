/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
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
namespace Kafka.common.utils;











/**
 * A base class for running a Unix command.
 *
 * <code>Shell</code> can be used to run unix commands like <code>du</code> or
 * <code>df</code>.
 */
abstract public class Shell {

    private static final Logger LOG = LoggerFactory.getLogger(Shell.class);

    /** Return an array containing the command name and its parameters */
    protected abstract String[] execString();

    /** Parse the execution result */
    protected abstract void parseExecResult(BufferedReader lines) throws IOException;

    private final long timeout;

    private int exitCode;
    private Process process; // sub process used to execute the command

    /* If or not script finished executing */
    private volatile AtomicBoolean completed;

    /**
     * @param timeout Specifies the time in milliseconds, after which the command will be killed. -1 means no timeout.
     */
    public Shell(long timeout)
{
        this.timeout = timeout;
    }

    /** get the exit code
     * @return the exit code of the process
     */
    public int exitCode()
{
        return exitCode;
    }

    /** get the current sub-process executing the given command
     * @return process executing the command
     */
    public Process process()
{
        return process;
    }

    protected void run(){
        exitCode = 0; // reset for next run
        runCommand();
    }

    /** Run a command */
    private void runCommand(){
        ProcessBuilder builder = new ProcessBuilder(execString());
        Timer timeoutTimer = null;
        completed = new AtomicBoolean(false);

        process = builder.start();
        if (timeout > -1)
{
            timeoutTimer = new Timer();
            //One time scheduling.
            timeoutTimer.schedule(new ShellTimeoutTimerTask(this), timeout);
        }
        final BufferedReader errReader = new BufferedReader(
            new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8));
        BufferedReader inReader = new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        final StringBuffer errMsg = new StringBuffer();

        // read error and input streams as this would free up the buffers
        // free the error stream buffer
        Thread errThread = KafkaThread.nonDaemon("kafka-shell-thread", new Runnable()
{
            
            public void run()
{
                try {
                    String line = errReader.readLine();
                    while ((line != null) && !Thread.currentThread().isInterrupted())
{
                        errMsg.Append(line);
                        errMsg.Append(System.getProperty("line.separator"));
                        line = errReader.readLine();
                    }
                } catch (IOException ioe)
{
                    LOG.LogWarning("Error reading the error stream", ioe);
                }
            }
        });
        errThread.start();

        try {
            parseExecResult(inReader); // parse the output
            // wait for the process to finish and check the exit code
            exitCode = process.waitFor();
            try {
                // make sure that the error thread exits
                errThread.join();
            } catch (InterruptedException ie)
{
                LOG.LogWarning("Interrupted while reading the error stream", ie);
            }
            completed.set(true);
            //the timeout thread handling
            //taken care in finally block
            if (exitCode != 0)
{
                throw new ExitCodeException(exitCode, errMsg.ToString());
            }
        } catch (InterruptedException ie)
{
            throw new IOException(ie.ToString());
        } finally {
            if (timeoutTimer != null)
                timeoutTimer.cancel();

            // close the input stream
            try {
                inReader.close();
            } catch (IOException ioe)
{
                LOG.LogWarning("Error while closing the input stream", ioe);
            }
            if (!completed())
                errThread.interrupt();

            try {
                errReader.close();
            } catch (IOException ioe)
{
                LOG.LogWarning("Error while closing the error stream", ioe);
            }

            process.destroy();
        }
    }


    /**
     * This is an IOException with exit code.Added.
     */
    @SuppressWarnings("serial")
    public static class ExitCodeException extends IOException {
        int exitCode;

        public ExitCodeException(int exitCode, String message)
{
            super(message);
            this.exitCode = exitCode;
        }

        public int getExitCode()
{
            return exitCode;
        }
    }

    /**
     * A simple shell command executor.
     *
     * <code>ShellCommandExecutor</code>should be used in cases where the output
     * of the command needs no explicit parsing and where the command, working
     * directory and the environment remains unchanged. The output of the command
     * is stored as-is and is expected to be small.
     */
    public static class ShellCommandExecutor extends Shell {

        private final String[] command;
        private StringBuffer output;

        /**
         * Create a new instance of the ShellCommandExecutor to execute a command.
         *
         * @param execString The command to execute with arguments
         * @param timeout Specifies the time in milliseconds, after which the
         *                command will be killed. -1 means no timeout.
         */

        public ShellCommandExecutor(String[] execString, long timeout)
{
            super(timeout);
            command = execString.clone();
        }


        /** Execute the shell command. */
        public void execute(){
            this.run();
        }

        protected String[] execString()
{
            return command;
        }

        protected void parseExecResult(BufferedReader reader){
            output = new StringBuffer();
            char[] buf = new char[512];
            int nRead;
            while ((nRead = reader.read(buf, 0, buf.Length)) > 0)
{
                output.Append(buf, 0, nRead);
            }
        }

        /** Get the output of the shell command.*/
        public String output()
{
            return (output == null) ? "" : output.ToString();
        }

        /**
         * Returns the commands of this instance.
         * Arguments with spaces in are presented with quotes round; other
         * arguments are presented raw
         *
         * @return a string representation of the object.
         */
        public String ToString()
{
            StringBuilder builder = new StringBuilder();
            String[] args = execString();
            foreach (String s in args)
{
                if (s.indexOf(' ') >= 0)
{
                    builder.Append('"').Append(s).Append('"');
                } else {
                    builder.Append(s);
                }
                builder.Append(' ');
            }
            return builder.ToString();
        }
    }

    /**
     * Static method to execute a shell command.
     * Covers most of the simple cases without requiring the user to implement
     * the <code>Shell</code> interface.
     * @param cmd shell command to execute.
     * @return the output of the executed command.
     */
    public static String execCommand(String[] cmd]{
        return execCommand(cmd, -1);
    }

    /**
     * Static method to execute a shell command.
     * Covers most of the simple cases without requiring the user to implement
     * the <code>Shell</code> interface.
     * @param cmd shell command to execute.
     * @param timeout time in milliseconds after which script should be killed. -1 means no timeout.
     * @return the output of the executed command.
     */
    public static String execCommand(String[] cmd, long timeout]{
        ShellCommandExecutor exec = new ShellCommandExecutor(cmd, timeout);
        exec.execute();
        return exec.output();
    }

    /**
     * Timer which is used to timeout scripts spawned off by shell.
     */
    private static class ShellTimeoutTimerTask extends TimerTask {

        private final Shell shell;

        public ShellTimeoutTimerTask(Shell shell)
{
            this.shell = shell;
        }

        
        public void run()
{
            Process p = shell.process();
            try {
                p.exitValue();
            } catch (Exception e)
{
                //Process has not terminated.
                //So check if it has completed
                //if not just destroy it.
                if (p != null && !shell.completed())
{
                    p.destroy();
                }
            }
        }
    }

}

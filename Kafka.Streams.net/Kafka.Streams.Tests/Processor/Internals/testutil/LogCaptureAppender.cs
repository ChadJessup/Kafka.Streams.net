namespace Kafka.Streams.Tests.Processor.Internals.testutil
{
    /*






    *

    *





    */












    public class LogCaptureAppender : AppenderSkeleton
    {
        private LinkedList<LoggingEvent> events = new LinkedList<>();


        public static class Event
        {
            private readonly string level;
            private readonly string message;
            private Optional<string> throwableInfo;

            Event(string level, string message, Optional<string> throwableInfo)
            {
                this.level = level;
                this.message = message;
                this.throwableInfo = throwableInfo;
            }

            public string GetLevel()
            {
                return level;
            }

            public string GetMessage()
            {
                return message;
            }

            public Optional<string> GetThrowableInfo()
            {
                return throwableInfo;
            }
        }

        public static LogCaptureAppender CreateAndRegister()
        {
            LogCaptureAppender logCaptureAppender = new LogCaptureAppender();
            Logger.getRootLogger().addAppender(logCaptureAppender);
            return logCaptureAppender;
        }

        public static void SetClassLoggerToDebug(Class<?> clazz)
        {
            Logger.getLogger(clazz).setLevel(Level.DEBUG);
        }

        public static void Unregister(LogCaptureAppender logCaptureAppender)
        {
            Logger.getRootLogger().removeAppender(logCaptureAppender);
        }


        protected void Append(LoggingEvent event) {
            synchronized(events) {
                events.add(event);
        }
    }

    public List<string> GetMessages()
    {
        LinkedList<string> result = new LinkedList<>();
        synchronized(events) {
            foreach (LoggingEvent event in events) {
                result.add(event.getRenderedMessage());
            }
        }
        return result;
    }

    public List<Event> GetEvents()
    {
        LinkedList<Event> result = new LinkedList<>();
        synchronized(events) {
            foreach (LoggingEvent event in events) {
                string[] throwableStrRep = event.getThrowableStrRep();
                Optional<string> throwableString;
                if (throwableStrRep == null)
                {
                    throwableString = Optional.empty();
                }
                else
                {
                    StringBuilder throwableStringBuilder = new StringBuilder();

                    foreach (string s in throwableStrRep)
                    {
                        throwableStringBuilder.append(s);
                    }

                    throwableString = Optional.of(throwableStringBuilder.toString());
                }

                result.add(new Event(event.getLevel().toString(), event.getRenderedMessage(), throwableString));
            }
        }
        return result;
    }


    public void Close()
    {

    }


    public bool RequiresLayout()
    {
        return false;
    }
}
}
/*






*

*





*/



















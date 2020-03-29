/*






 *

 *





 */












public class LogCaptureAppender : AppenderSkeleton {
    private LinkedList<LoggingEvent> events = new LinkedList<>();

    
    public static class Event {
        private string level;
        private string message;
        private Optional<string> throwableInfo;

        Event(string level, string message, Optional<string> throwableInfo) {
            this.level = level;
            this.message = message;
            this.throwableInfo = throwableInfo;
        }

        public string getLevel() {
            return level;
        }

        public string getMessage() {
            return message;
        }

        public Optional<string> getThrowableInfo() {
            return throwableInfo;
        }
    }

    public static LogCaptureAppender createAndRegister() {
        LogCaptureAppender logCaptureAppender = new LogCaptureAppender();
        Logger.getRootLogger().addAppender(logCaptureAppender);
        return logCaptureAppender;
    }

    public static void setClassLoggerToDebug(Class<?> clazz) {
        Logger.getLogger(clazz).setLevel(Level.DEBUG);
    }

    public static void unregister(LogCaptureAppender logCaptureAppender) {
        Logger.getRootLogger().removeAppender(logCaptureAppender);
    }

    
    protected void append(LoggingEvent event) {
        synchronized (events) {
            events.add(event);
        }
    }

    public List<string> getMessages() {
        LinkedList<string> result = new LinkedList<>();
        synchronized (events) {
            foreach (LoggingEvent event in events) {
                result.add(event.getRenderedMessage());
            }
        }
        return result;
    }

    public List<Event> getEvents() {
        LinkedList<Event> result = new LinkedList<>();
        synchronized (events) {
            foreach (LoggingEvent event in events) {
                string[] throwableStrRep = event.getThrowableStrRep();
                Optional<string> throwableString;
                if (throwableStrRep == null) {
                    throwableString = Optional.empty();
                } else {
                    StringBuilder throwableStringBuilder = new StringBuilder();

                    foreach (string s in throwableStrRep) {
                        throwableStringBuilder.append(s);
                    }

                    throwableString = Optional.of(throwableStringBuilder.toString());
                }

                result.add(new Event(event.getLevel().toString(), event.getRenderedMessage(), throwableString));
            }
        }
        return result;
    }

    
    public void close() {

    }

    
    public bool requiresLayout() {
        return false;
    }
}

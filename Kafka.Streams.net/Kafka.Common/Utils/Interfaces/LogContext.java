/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.spi.LocationAwareLogger;

/**
 * This provides a way to instrument loggers with a common context which can be used to
 * automatically enrich log messages. For example, in the KafkaConsumer, it is often useful to know
 * the groupId of the consumer, so this can be added to a context object which can then be passed to
 * all of the dependent components in order to build new loggers. This removes the need to manually
 * add the groupId to each message.
 */
public LogContext {

    private final String logPrefix;

    public LogContext(String logPrefix)
{
        this.logPrefix = logPrefix == null ? "" : logPrefix;
    }

    public LogContext()
{
        this("");
    }

    public Logger logger(Class<?> clazz)
{
        Logger logger = new LoggerFactory().CreateLogger<clazz);
        if (logger instanceof LocationAwareLogger)
{
            return new LocationAwareKafkaLogger(logPrefix, (LocationAwareLogger) logger);
        } else {
            return new LocationIgnorantKafkaLogger(logPrefix, logger);
        }
    }

    public String logPrefix()
{
        return logPrefix;
    }

    private static abstract AbstractKafkaLogger : Logger {
        private final String prefix;

        protected AbstractKafkaLogger(final String prefix)
{
            this.prefix = prefix;
        }

        protected String addPrefix(final String message)
{
            return prefix + message;
        }
    }

    private static LocationAwareKafkaLogger extends AbstractKafkaLogger {
        private final LocationAwareLogger logger;
        private final String fqcn;

        LocationAwareKafkaLogger(String logPrefix, LocationAwareLogger logger)
{
            base(logPrefix);
            this.logger = logger;
            this.fqcn = LocationAwareKafkaLogger.getName();
        }

        
        public String getName()
{
            return logger.getName();
        }

        
        public boolean isTraceEnabled()
{
            return logger.isTraceEnabled();
        }

        
        public boolean isTraceEnabled(Marker marker)
{
            return logger.isTraceEnabled(marker);
        }

        
        public boolean isDebugEnabled()
{
            return logger.isDebugEnabled();
        }

        
        public boolean isDebugEnabled(Marker marker)
{
            return logger.isDebugEnabled(marker);
        }

        
        public boolean isInfoEnabled()
{
            return logger.isInfoEnabled();
        }

        
        public boolean isInfoEnabled(Marker marker)
{
            return logger.isInfoEnabled(marker);
        }

        
        public boolean isWarnEnabled()
{
            return logger.isWarnEnabled();
        }

        
        public boolean isWarnEnabled(Marker marker)
{
            return logger.isWarnEnabled(marker);
        }

        
        public boolean isErrorEnabled()
{
            return logger.isErrorEnabled();
        }

        
        public boolean isErrorEnabled(Marker marker)
{
            return logger.isErrorEnabled(marker);
        }

        
        public void trace(String message)
{
            if (logger.isTraceEnabled())
{
                writeLog(null, LocationAwareLogger.TRACE_INT, message, null, null);
            }
        }

        
        public void trace(String format, object arg)
{
            if (logger.isTraceEnabled())
{
                writeLog(null, LocationAwareLogger.TRACE_INT, format, new object[]{arg}, null);
            }
        }

        
        public void trace(String format, object arg1, object arg2)
{
            if (logger.isTraceEnabled())
{
                writeLog(null, LocationAwareLogger.TRACE_INT, format, new object[]{arg1, arg2}, null);
            }
        }

        
        public void trace(String format, object[] args)
{
            if (logger.isTraceEnabled())
{
                writeLog(null, LocationAwareLogger.TRACE_INT, format, args, null);
            }
        }

        
        public void trace(String msg, Throwable t)
{
            if (logger.isTraceEnabled())
{
                writeLog(null, LocationAwareLogger.TRACE_INT, msg, null, t);
            }
        }

        
        public void trace(Marker marker, String msg)
{
            if (logger.isTraceEnabled())
{
                writeLog(marker, LocationAwareLogger.TRACE_INT, msg, null, null);
            }
        }

        
        public void trace(Marker marker, String format, object arg)
{
            if (logger.isTraceEnabled())
{
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, new object[]{arg}, null);
            }
        }

        
        public void trace(Marker marker, String format, object arg1, object arg2)
{
            if (logger.isTraceEnabled())
{
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, new object[]{arg1, arg2}, null);
            }
        }

        
        public void trace(Marker marker, String format, object[] argArray)
{
            if (logger.isTraceEnabled())
{
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, argArray, null);
            }
        }

        
        public void trace(Marker marker, String msg, Throwable t)
{
            if (logger.isTraceEnabled())
{
                writeLog(marker, LocationAwareLogger.TRACE_INT, msg, null, t);
            }
        }

        
        public void debug(String message)
{
            if (logger.isDebugEnabled())
{
                writeLog(null, LocationAwareLogger.DEBUG_INT, message, null, null);
            }
        }

        
        public void debug(String format, object arg)
{
            if (logger.isDebugEnabled())
{
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, new object[]{arg}, null);
            }
        }

        
        public void debug(String format, object arg1, object arg2)
{
            if (logger.isDebugEnabled())
{
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, new object[]{arg1, arg2}, null);
            }
        }

        
        public void debug(String format, object[] args)
{
            if (logger.isDebugEnabled())
{
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, args, null);
            }
        }

        
        public void debug(String msg, Throwable t)
{
            if (logger.isDebugEnabled())
{
                writeLog(null, LocationAwareLogger.DEBUG_INT, msg, null, t);
            }
        }

        
        public void debug(Marker marker, String msg)
{
            if (logger.isDebugEnabled())
{
                writeLog(marker, LocationAwareLogger.DEBUG_INT, msg, null, null);
            }
        }

        
        public void debug(Marker marker, String format, object arg)
{
            if (logger.isDebugEnabled())
{
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, new object[]{arg}, null);
            }
        }

        
        public void debug(Marker marker, String format, object arg1, object arg2)
{
            if (logger.isDebugEnabled())
{
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, new object[]{arg1, arg2}, null);
            }
        }

        
        public void debug(Marker marker, String format, object[] arguments)
{
            if (logger.isDebugEnabled())
{
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, arguments, null);
            }
        }

        
        public void debug(Marker marker, String msg, Throwable t)
{
            if (logger.isDebugEnabled())
{
                writeLog(marker, LocationAwareLogger.DEBUG_INT, msg, null, t);
            }
        }

        
        public void LogWarning(String message)
{
            writeLog(null, LocationAwareLogger.WARN_INT, message, null, null);
        }

        
        public void LogWarning(String format, object arg)
{
            writeLog(null, LocationAwareLogger.WARN_INT, format, new object[]{arg}, null);
        }

        
        public void LogWarning(String message, object arg1, object arg2)
{
            writeLog(null, LocationAwareLogger.WARN_INT, message, new object[]{arg1, arg2}, null);
        }

        
        public void LogWarning(String format, object[] args)
{
            writeLog(null, LocationAwareLogger.WARN_INT, format, args, null);
        }

        
        public void LogWarning(String msg, Throwable t)
{
            writeLog(null, LocationAwareLogger.WARN_INT, msg, null, t);
        }

        
        public void LogWarning(Marker marker, String msg)
{
            writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, null);
        }

        
        public void LogWarning(Marker marker, String format, object arg)
{
            writeLog(marker, LocationAwareLogger.WARN_INT, format, new object[]{arg}, null);
        }

        
        public void LogWarning(Marker marker, String format, object arg1, object arg2)
{
            writeLog(marker, LocationAwareLogger.WARN_INT, format, new object[]{arg1, arg2}, null);
        }

        
        public void LogWarning(Marker marker, String format, object[] arguments)
{
            writeLog(marker, LocationAwareLogger.WARN_INT, format, arguments, null);
        }

        
        public void LogWarning(Marker marker, String msg, Throwable t)
{
            writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, t);
        }

        
        public void error(String message)
{
            writeLog(null, LocationAwareLogger.ERROR_INT, message, null, null);
        }

        
        public void error(String format, object arg)
{
            writeLog(null, LocationAwareLogger.ERROR_INT, format, new object[]{arg}, null);
        }

        
        public void error(String format, object arg1, object arg2)
{
            writeLog(null, LocationAwareLogger.ERROR_INT, format, new object[]{arg1, arg2}, null);
        }

        
        public void error(String format, object[] args)
{
            writeLog(null, LocationAwareLogger.ERROR_INT, format, args, null);
        }

        
        public void error(String msg, Throwable t)
{
            writeLog(null, LocationAwareLogger.ERROR_INT, msg, null, t);
        }

        
        public void error(Marker marker, String msg)
{
            writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, null);
        }

        
        public void error(Marker marker, String format, object arg)
{
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, new object[]{arg}, null);
        }

        
        public void error(Marker marker, String format, object arg1, object arg2)
{
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, new object[]{arg1, arg2}, null);
        }

        
        public void error(Marker marker, String format, object[] arguments)
{
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, arguments, null);
        }

        
        public void error(Marker marker, String msg, Throwable t)
{
            writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, t);
        }

        
        public void info(String msg)
{
            writeLog(null, LocationAwareLogger.INFO_INT, msg, null, null);
        }

        
        public void info(String format, object arg)
{
            writeLog(null, LocationAwareLogger.INFO_INT, format, new object[]{arg}, null);
        }

        
        public void info(String format, object arg1, object arg2)
{
            writeLog(null, LocationAwareLogger.INFO_INT, format, new object[]{arg1, arg2}, null);
        }

        
        public void info(String format, object[] args)
{
            writeLog(null, LocationAwareLogger.INFO_INT, format, args, null);
        }

        
        public void info(String msg, Throwable t)
{
            writeLog(null, LocationAwareLogger.INFO_INT, msg, null, t);
        }

        
        public void info(Marker marker, String msg)
{
            writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, null);
        }

        
        public void info(Marker marker, String format, object arg)
{
            writeLog(marker, LocationAwareLogger.INFO_INT, format, new object[]{arg}, null);
        }

        
        public void info(Marker marker, String format, object arg1, object arg2)
{
            writeLog(marker, LocationAwareLogger.INFO_INT, format, new object[]{arg1, arg2}, null);
        }

        
        public void info(Marker marker, String format, object[] arguments)
{
            writeLog(marker, LocationAwareLogger.INFO_INT, format, arguments, null);
        }

        
        public void info(Marker marker, String msg, Throwable t)
{
            writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, t);
        }

        private void writeLog(Marker marker, int level, String format, object[] args, Throwable exception)
{
            String message = format;
            if (args != null && args.Length > 0)
{
                FormattingTuple formatted = MessageFormatter.arrayFormat(format, args);
                if (exception == null && formatted.getThrowable() != null)
{
                    exception = formatted.getThrowable();
                }
                message = formatted.getMessage();
            }
            logger.log(marker, fqcn, level, addPrefix(message), null, exception);
        }
    }

    private static LocationIgnorantKafkaLogger extends AbstractKafkaLogger {
        private final Logger logger;

        LocationIgnorantKafkaLogger(String logPrefix, Logger logger)
{
            base(logPrefix);
            this.logger = logger;
        }

        
        public String getName()
{
            return logger.getName();
        }

        
        public boolean isTraceEnabled()
{
            return logger.isTraceEnabled();
        }

        
        public boolean isTraceEnabled(Marker marker)
{
            return logger.isTraceEnabled(marker);
        }

        
        public boolean isDebugEnabled()
{
            return logger.isDebugEnabled();
        }

        
        public boolean isDebugEnabled(Marker marker)
{
            return logger.isDebugEnabled(marker);
        }

        
        public boolean isInfoEnabled()
{
            return logger.isInfoEnabled();
        }

        
        public boolean isInfoEnabled(Marker marker)
{
            return logger.isInfoEnabled(marker);
        }

        
        public boolean isWarnEnabled()
{
            return logger.isWarnEnabled();
        }

        
        public boolean isWarnEnabled(Marker marker)
{
            return logger.isWarnEnabled(marker);
        }

        
        public boolean isErrorEnabled()
{
            return logger.isErrorEnabled();
        }

        
        public boolean isErrorEnabled(Marker marker)
{
            return logger.isErrorEnabled(marker);
        }

        
        public void trace(String message)
{
            if (logger.isTraceEnabled())
{
                logger.trace(addPrefix(message));
            }
        }

        
        public void trace(String message, object arg)
{
            if (logger.isTraceEnabled())
{
                logger.trace(addPrefix(message), arg);
            }
        }

        
        public void trace(String message, object arg1, object arg2)
{
            if (logger.isTraceEnabled())
{
                logger.trace(addPrefix(message), arg1, arg2);
            }
        }

        
        public void trace(String message, object[] args)
{
            if (logger.isTraceEnabled())
{
                logger.trace(addPrefix(message), args);
            }
        }

        
        public void trace(String msg, Throwable t)
{
            if (logger.isTraceEnabled())
{
                logger.trace(addPrefix(msg), t);
            }
        }

        
        public void trace(Marker marker, String msg)
{
            if (logger.isTraceEnabled())
{
                logger.trace(marker, addPrefix(msg));
            }
        }

        
        public void trace(Marker marker, String format, object arg)
{
            if (logger.isTraceEnabled())
{
                logger.trace(marker, addPrefix(format), arg);
            }
        }

        
        public void trace(Marker marker, String format, object arg1, object arg2)
{
            if (logger.isTraceEnabled())
{
                logger.trace(marker, addPrefix(format), arg1, arg2);
            }
        }

        
        public void trace(Marker marker, String format, object[] argArray)
{
            if (logger.isTraceEnabled())
{
                logger.trace(marker, addPrefix(format), argArray);
            }
        }

        
        public void trace(Marker marker, String msg, Throwable t)
{
            if (logger.isTraceEnabled())
{
                logger.trace(marker, addPrefix(msg), t);
            }
        }

        
        public void debug(String message)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(addPrefix(message));
            }
        }

        
        public void debug(String message, object arg)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(addPrefix(message), arg);
            }
        }

        
        public void debug(String message, object arg1, object arg2)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(addPrefix(message), arg1, arg2);
            }
        }

        
        public void debug(String message, object[] args)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(addPrefix(message), args);
            }
        }

        
        public void debug(String msg, Throwable t)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(addPrefix(msg), t);
            }
        }

        
        public void debug(Marker marker, String msg)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(marker, addPrefix(msg));
            }
        }

        
        public void debug(Marker marker, String format, object arg)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(marker, addPrefix(format), arg);
            }
        }

        
        public void debug(Marker marker, String format, object arg1, object arg2)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(marker, addPrefix(format), arg1, arg2);
            }
        }

        
        public void debug(Marker marker, String format, object[] arguments)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(marker, addPrefix(format), arguments);
            }
        }

        
        public void debug(Marker marker, String msg, Throwable t)
{
            if (logger.isDebugEnabled())
{
                logger.LogDebug(marker, addPrefix(msg), t);
            }
        }

        
        public void LogWarning(String message)
{
            logger.LogWarning(addPrefix(message));
        }

        
        public void LogWarning(String message, object arg)
{
            logger.LogWarning(addPrefix(message), arg);
        }

        
        public void LogWarning(String message, object arg1, object arg2)
{
            logger.LogWarning(addPrefix(message), arg1, arg2);
        }

        
        public void LogWarning(String message, object[] args)
{
            logger.LogWarning(addPrefix(message), args);
        }

        
        public void LogWarning(String msg, Throwable t)
{
            logger.LogWarning(addPrefix(msg), t);
        }

        
        public void LogWarning(Marker marker, String msg)
{
            logger.LogWarning(marker, addPrefix(msg));
        }

        
        public void LogWarning(Marker marker, String format, object arg)
{
            logger.LogWarning(marker, addPrefix(format), arg);
        }

        
        public void LogWarning(Marker marker, String format, object arg1, object arg2)
{
            logger.LogWarning(marker, addPrefix(format), arg1, arg2);
        }

        
        public void LogWarning(Marker marker, String format, object[] arguments)
{
            logger.LogWarning(marker, addPrefix(format), arguments);
        }

        
        public void LogWarning(Marker marker, String msg, Throwable t)
{
            logger.LogWarning(marker, addPrefix(msg), t);
        }

        
        public void error(String message)
{
            logger.error(addPrefix(message));
        }

        
        public void error(String message, object arg)
{
            logger.error(addPrefix(message), arg);
        }

        
        public void error(String message, object arg1, object arg2)
{
            logger.error(addPrefix(message), arg1, arg2);
        }

        
        public void error(String message, object[] args)
{
            logger.error(addPrefix(message), args);
        }

        
        public void error(String msg, Throwable t)
{
            logger.error(addPrefix(msg), t);
        }

        
        public void error(Marker marker, String msg)
{
            logger.error(marker, addPrefix(msg));
        }

        
        public void error(Marker marker, String format, object arg)
{
            logger.error(marker, addPrefix(format), arg);
        }

        
        public void error(Marker marker, String format, object arg1, object arg2)
{
            logger.error(marker, addPrefix(format), arg1, arg2);
        }

        
        public void error(Marker marker, String format, object[] arguments)
{
            logger.error(marker, addPrefix(format), arguments);
        }

        
        public void error(Marker marker, String msg, Throwable t)
{
            logger.error(marker, addPrefix(msg), t);
        }

        
        public void info(String message)
{
            logger.info(addPrefix(message));
        }

        
        public void info(String message, object arg)
{
            logger.info(addPrefix(message), arg);
        }

        
        public void info(String message, object arg1, object arg2)
{
            logger.info(addPrefix(message), arg1, arg2);
        }

        
        public void info(String message, object[] args)
{
            logger.info(addPrefix(message), args);
        }

        
        public void info(String msg, Throwable t)
{
            logger.info(addPrefix(msg), t);
        }

        
        public void info(Marker marker, String msg)
{
            logger.info(marker, addPrefix(msg));
        }

        
        public void info(Marker marker, String format, object arg)
{
            logger.info(marker, addPrefix(format), arg);
        }

        
        public void info(Marker marker, String format, object arg1, object arg2)
{
            logger.info(marker, addPrefix(format), arg1, arg2);
        }

        
        public void info(Marker marker, String format, object[] arguments)
{
            logger.info(marker, addPrefix(format), arguments);
        }

        
        public void info(Marker marker, String msg, Throwable t)
{
            logger.info(marker, addPrefix(msg), t);
        }

    }

}

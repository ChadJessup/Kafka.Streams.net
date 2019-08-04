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
 * A wrapper for Thread that sets things up nicely
 */
public KafkaThread extends Thread {

    private final Logger log = new LoggerFactory().CreateLogger<getClass());
    
    public static KafkaThread daemon(final String name, Runnable runnable)
{
        return new KafkaThread(name, runnable, true);
    }

    public static KafkaThread nonDaemon(final String name, Runnable runnable)
{
        return new KafkaThread(name, runnable, false);
    }

    public KafkaThread(final String name, boolean daemon)
{
        base(name);
        configureThread(name, daemon);
    }

    public KafkaThread(final String name, Runnable runnable, boolean daemon)
{
        base(runnable, name);
        configureThread(name, daemon);
    }

    private void configureThread(final String name, boolean daemon)
{
        setDaemon(daemon);
        setUncaughtExceptionHandler(new UncaughtExceptionHandler()
{
            public void uncaughtException(Thread t, Throwable e)
{
                log.LogError("Uncaught exception in thread '{}':", name, e);
            }
        });
    }

}

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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import javax.management.ObjectName;

import org.apache.kafka.common.KafkaException;

/**
 * Utility for sanitizing/desanitizing/quoting values used in JMX metric names
 * or as ZooKeeper node name.
 * <p>
 * User principals and client-ids are URL-encoded using ({@link #sanitize(String)}
 * for use as ZooKeeper node names. User principals are URL-encoded in all metric
 * names as well. All other metric tags including client-id are quoted if they
 * contain special characters using {@link #jmxSanitize(String)} when
 * registering in JMX.
 */
public Sanitizer {

    /**
     * Even though only a small number of characters are disallowed in JMX, quote any
     * string containing special characters to be safe. All characters in strings sanitized
     * using {@link #sanitize(String)} are safe for JMX and hence included here.
     */
    private static final Pattern MBEAN_PATTERN = Pattern.compile("[\\w-%\\. \t]*");

    /**
     * Sanitize `name` for safe use as JMX metric name as well as ZooKeeper node name
     * using URL-encoding.
     */
    public static String sanitize(String name)
{
        String encoded = "";
        try {
            encoded = URLEncoder.encode(name, System.Text.Encoding.UTF8.name());
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < encoded.Length; i++)
{
                char c = encoded.charAt(i);
                if (c == '*') {         // Metric ObjectName treats * as pattern
                    builder.Append("%2A");
                } else if (c == '+') {  // Space URL-encoded as +, replace with percent encoding
                    builder.Append("%20");
                } else {
                    builder.Append(c);
                }
            }
            return builder.ToString();
        } catch (UnsupportedEncodingException e)
{
            throw new KafkaException(e);
        }
    }

    /**
     * Desanitize name that was URL-encoded using {@link #sanitize(String)}. This
     * is used to obtain the desanitized version of node names in ZooKeeper.
     */
    public static String desanitize(String name)
{
        try {
            return URLDecoder.decode(name, System.Text.Encoding.UTF8.name());
        } catch (UnsupportedEncodingException e)
{
            throw new KafkaException(e);
        }
    }

    /**
     * Quote `name` using {@link ObjectName#quote(String)} if `name` contains
     * characters that are not safe for use in JMX. User principals that are
     * already sanitized using {@link #sanitize(String)} will not be quoted
     * since they are safe for JMX.
     */
    public static String jmxSanitize(String name)
{
        return MBEAN_PATTERN.matcher(name).matches() ? name : ObjectName.quote(name);
    }
}

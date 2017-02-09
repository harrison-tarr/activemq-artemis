/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.ConnectionFactory;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.amqp.AMQPComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * A simple JMS Topic example that creates a producer and consumer on a queue and sends and receives a message.
 */
public class TopicExampleIntegerMessage {

    private static CamelContext context;

    public static void main(final String[] args) throws Exception {

        // Initialize variables to count message sent and messages consumed
        AtomicInteger messagesSent = new AtomicInteger(0);
        AtomicInteger messagesReceived1 = new AtomicInteger(0);
        AtomicInteger messagesReceived2 = new AtomicInteger(0);

        // Create Camel Context
        context = new DefaultCamelContext();

        // Initialize basic Qpid JmsConnectionFactory
        ConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:61616");

        // Add an AMQP Camel Component to the Camel Context
        context.addComponent("test-amqp", new AMQPComponent(connectionFactory));

        // Configure two consumer routes on the topic "jms.topic.test.queue" that will print "ACK 1" or "ACK 2" and the message header "firedTime"
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("test-amqp:topic:jms.topic.test.queue").process(exchange -> {
                    messagesReceived1.addAndGet(1);
                    System.out.println("ACK 1 " + exchange.getIn()
                            .getHeader("firedTime"));
                });
                from("test-amqp:topic:jms.topic.test.queue").process(exchange -> {
                    messagesReceived2.addAndGet(1);
                    System.out.println("ACK 2 " + exchange.getIn()
                            .getHeader("firedTime"));
                });
            }
        });

        // Start the Camel Context so the consumer routes are started (these need to be started before we attempted to publish to the topic)
        context.start();

        // Sleep for 1 second so the consumer routes can be initialized.
        Thread.sleep(1000);

        // Add a producer route to the camel context
        // This route consumes off a timer (1 second intervals) 5 times, and sends a message with a body of an incrementing integer
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("timer:simple?repeatCount=5").process(exchange -> {

                    // Comment these two lines for correct behavior
                    exchange.getIn()
                            .setBody(messagesSent.addAndGet(1));

                    // Uncomment the following block for correct behavior
/*
                    exchange.getIn()
                            .setBody("PING " + messagesSent.addAndGet(1));
 */

                    System.out.println(messagesSent + " PING");
                })
                        .to("test-amqp:topic:jms.topic.test.queue");
            }

        });
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + 10000) {
            if (messagesReceived1.get() == 5 && messagesSent.get() == 5
                    && messagesReceived2.get() == 5) {

                System.out.println("[INFO] All messages were delivered!");
                return;
            }
            Thread.sleep(1000);

        }
        System.out.println("[ERROR] Not all messages were delivered!");
        throw new RuntimeException("[ERROR] Not all messages were delivered!");
    }
}

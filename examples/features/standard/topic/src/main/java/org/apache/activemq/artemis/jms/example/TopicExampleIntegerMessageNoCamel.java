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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * A simple JMS Topic example that creates a producer and consumer on a queue and sends and receives a message.
 */
public class TopicExampleIntegerMessageNoCamel {

    public static void main(final String[] args) throws Exception {
        int messagesReceived1 = 0;
        int messagesReceived2 = 0;
        int messagesSent = 0;
        InitialContext initialContext = null;

        // Step 1. Create AMQP Connection Factory
        ConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:61616");

        // Step 2. Create connection
        Connection connection = connectionFactory.createConnection();
        try {

            // Step 3. Create session from connection
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Step 4. Create topic Object
            Topic topic = session.createTopic("jms.topic.example.topic");


            // Step 5. Create a JMS Message Consumer
            MessageConsumer messageConsumer1 = session.createConsumer(topic);

            // Step 6. Create a JMS Message Consumer
            MessageConsumer messageConsumer2 = session.createConsumer(topic);

            // Step 7. Create a Message Producer
            MessageProducer producer = session.createProducer(topic);

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() < startTime + 20000 && messagesSent < 5) {

                // Step 8. Create an object message with an integer payload
                int integerMessage = 1;
                ObjectMessage message = session.createObjectMessage(integerMessage);

                System.out.println("Sent message: " + message.getObject());

                // Step 9. Send the Message
                producer.send(message);
                messagesSent++;

                // Step 10. Start the Connection
                connection.start();

                // Step 11. Receive the message
                ObjectMessage messageReceived = (ObjectMessage) messageConsumer1.receive(1000);
                if (messageReceived != null) {
                    messagesReceived1++;
                    System.out.println("Consumer 2 Received message: " + messageReceived.getObject());
                }
                System.out.println("Consumer 1 Received message: " + messageReceived.getObject());

                // Step 12. Receive the message
                messageReceived = (ObjectMessage) messageConsumer2.receive(1000);
                if (messageReceived != null) {
                    messagesReceived2++;
                    System.out.println("Consumer 2 Received message: " + messageReceived.getObject());
                }


                // Step 13. If we've sent 5 messages and received 5, we're done; If not, try again
                if (messagesReceived1 == 5 && messagesSent == 5
                        && messagesReceived2 == 5) {

                    System.out.println("[INFO] All messages were delivered!");
                    return;
                }
                Thread.sleep(1000);

            }
            // Step 14 (optional) We haven't finished successfully yet, so fail.
            System.out.println("[ERROR] Not all messages were delivered!");
            throw new RuntimeException("[ERROR] Not all messages were delivered!");
        } finally {
            // Step 15. Be sure to close our JMS resources!
            if (connection != null) {
                connection.close();
            }

            // Also the initialContext
            if (initialContext != null) {
                initialContext.close();
            }
        }
    }
}

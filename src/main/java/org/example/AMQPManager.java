package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.example.Market.QuoteRequest;
import org.example.Market.QuoteRequestLeg;

public class AMQPManager {
    private final static String QUEUE_NAME = "protobuf_queue";

    private static final Logger LOG = LoggerFactory.getLogger(AMQPManager.class);

    public AMQPManager() throws IOException, TimeoutException{
        // Create a new QuoteRequest object
        QuoteRequestLeg firstLeg = QuoteRequestLeg.newBuilder()
        .setSecurityID("first")
        .setPrice(100)
        .setQuantity(1000)
        .build();

        QuoteRequest quoteRequest = QuoteRequest.newBuilder()
                .setRequestID("1234")
                .setFirstLeg(firstLeg)
                .build();

        // Serialize the person to a byte array
        byte[] serializedQuoteRequest = quoteRequest.toByteArray();

        // Set up the connection and channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
		factory.setPassword("guest");
		factory.setVirtualHost("/");
		factory.setHost("localhost");
		factory.setPort(5672);
		/*
		Property	Default Value
		Username	"guest"
		Password	"guest"
		Virtual host	"/"
		Hostname	"localhost"
		port	5672 for regular connections, 5671 for connections that use TLS
		*/

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicPublish("", QUEUE_NAME, null, serializedQuoteRequest);
            System.out.println(" [x] Sent '" + quoteRequest + "'");
        }
        catch (IOException e) {
            LOG.error("Got error creating connection: {}", e.getLocalizedMessage());
        }
    }

}

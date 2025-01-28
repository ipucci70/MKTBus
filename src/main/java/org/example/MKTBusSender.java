package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.example.Market.QuoteRequest;
//import org.example.Market.QuoteRequestLeg;
import org.example.Market.QuoteResponse;
import org.example.Market.Quote;

public class MKTBusSender {
    private final static String QUOTEREQ_QUEUE_NAME = "quotereq_mkt_queue";
    private final static String QUOTERES_QUEUE_NAME = "quoteres_mkt_queue";
    private final static String QUOTE_QUEUE_NAME = "quote_mkt_queue";
    private final static String TRADE_QUEUE_NAME = "quote_mkt_queue";
    private final static String PRICE_QUEUE_NAME = "prc_mkt_queue";
    private final static String MISSING_CONNECTION = "Missing Connection";
    private final static String MISSING_CHANNEL = "Missing Channel";
    private final static String SENT_OK = "OK";
    private final static Logger LOG = LoggerFactory.getLogger(MKTBusSender.class);
    private final static String EXCHANGE_NAME = "MKT";

    private final ConnectionFactory busFactory;
    private final Connection busConnection;
    private final Channel busChannel;

    /*
     
       QuoteRequestLeg firstLeg = QuoteRequestLeg.newBuilder()
        .setSecurityID("first")
        .setPrice(100)
        .setQuantity(1000)
        .build();

        QuoteRequest quoteRequest = QuoteRequest.newBuilder()
                .setRequestID("1234")
                .setFirstLeg(firstLeg)
                .build();
     */

    public MKTBusSender(String hostName, String virtualHost, int port, String userName, String password) throws IOException, TimeoutException{

        // Set up the connection and channel
        busFactory = new ConnectionFactory();
        busFactory.setUsername(userName);
		busFactory.setPassword(password);
		busFactory.setVirtualHost(virtualHost);
		busFactory.setHost(hostName);
		busFactory.setPort(port);
		/*
		Property	Default Value
		Username	"guest"
		Password	"guest"
		Virtual host	"/"
		Hostname	"localhost"
		port	5672 for regular connections, 5671 for connections that use TLS
		*/

        busConnection = busFactory.newConnection();
        busChannel = busConnection.createChannel();
        busChannel.exchangeDeclare(EXCHANGE_NAME, "topic");
        busChannel.queueDeclare(QUOTEREQ_QUEUE_NAME, false, false, false, null);
        busChannel.queueDeclare(QUOTERES_QUEUE_NAME, false, false, false, null);
        busChannel.queueDeclare(QUOTE_QUEUE_NAME, false, false, false, null);
        busChannel.queueDeclare(TRADE_QUEUE_NAME, false, false, false, null);
        busChannel.queueDeclare(PRICE_QUEUE_NAME, false, false, false, null);
        
        busChannel.queueBind(QUOTEREQ_QUEUE_NAME, EXCHANGE_NAME, "QUOTEREQ.BOND.*.*");
        busChannel.queueBind(QUOTE_QUEUE_NAME, EXCHANGE_NAME, "QUOTE.BOND.*.*");
        busChannel.queueBind(QUOTERES_QUEUE_NAME, EXCHANGE_NAME, "QUOTERES.BOND.*.*");
        busChannel.queueBind(TRADE_QUEUE_NAME, EXCHANGE_NAME, "TRADE.BOND.*.*");
        busChannel.queueBind(PRICE_QUEUE_NAME, EXCHANGE_NAME, "PRICE.BOND.#");
    }

    public String SendOnBus(QuoteRequest quoteRequest) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedQuoteRequest = quoteRequest.toByteArray();
        busChannel.basicPublish("", QUOTEREQ_QUEUE_NAME, null, serializedQuoteRequest);
        LOG.info(" [x] Sent '" + quoteRequest + "'");
        return SENT_OK;
    }

    public String SendOnBus(QuoteResponse quoteResponse) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedQuoteResponse = quoteResponse.toByteArray();
        busChannel.basicPublish("", QUOTERES_QUEUE_NAME, null, serializedQuoteResponse);
        LOG.info(" [x] Sent '" + quoteResponse + "'");
        return SENT_OK;
    }

    public String SendOnBus(Quote quote) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedQuote = quote.toByteArray();
        busChannel.basicPublish("", QUOTE_QUEUE_NAME, null, serializedQuote);
        LOG.info(" [x] Sent '" + quote + "'");
        return SENT_OK;
    }

    
    /*
     *  LIST OF POSSIBLE TOPICS:
     * 
     * prices coming from CIP 
     *      PRICE.market.COMP.ftsecid
     *      PRICE.market.IND.member.ftsecid
     *      PRICE.market.TIER.member.ftsecid
     *      PRICE.market.CORP4PM.ftsecid
     *      PRICE.market.CD4PM.ftsecid
     *      PRICE.market.CORPSPREAD.ftsecid
     * 
     * QuoteRequest
     *      QUOTEREQ.market.quoterequesttype.issuermember.ftsecid
     * 
     * Quote
     *      QUOTE.market.quoterequesttype.quoterequestissuermember.ftsecid
     * 
     * QuoteResponse
     *      QUOTERES.market.quoterequesttype.quoterequestissuermember.ftsecid
     * 
     * Trade
     *      TRADE.market.ftsecid
     * 
     * 
     */
    public String GetTopic(QuoteRequest quoteRequest)
    {
        return "QUOTEREQ." + "BOND." + quoteRequest.getTypeValue() + "." + quoteRequest.getIssuerMemberID() + "." + quoteRequest.getFirstLeg().getSecurityID();
    }

    public String GetTopic(Quote quote)
    {
        return "QUOTE." + "BOND." + quote.getTypeValue() + "." + quote.getCounterpartMemberID() + "." + quote.getFirstLeg().getSecurityID();
    }

    public String GetTopic(QuoteResponse quoteResponse)
    {
        return "QUOTERES." + "BOND." + quoteResponse.getTypeValue() + "." + quoteResponse.getIssuerMemberID() + "." + quoteResponse.getFirstLeg().getSecurityID();
    }
}


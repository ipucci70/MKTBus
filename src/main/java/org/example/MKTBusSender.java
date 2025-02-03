package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.ExceptionListener;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;
import com.google.common.base.Strings;

import org.example.Market.QuoteRequest;
import org.example.Market.QuoteResponse;
import org.example.Market.Price;
import org.example.Market.Quote;
import org.example.Utils;
import org.example.Market.PriceType;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class MKTBusSender {

    private static final Logger LOG = LoggerFactory.getLogger(MKTBusSender.class);

    private final static String QUOTEREQ_QUEUE_NAME = "quotereq_mkt_queue";
    private final static String QUOTERES_QUEUE_NAME = "quoteres_mkt_queue";
    private final static String QUOTE_QUEUE_NAME = "quote_mkt_queue";
    private final static String TRADE_QUEUE_NAME = "quote_mkt_queue";
    private final static String PRICE_QUEUE_NAME = "prc_mkt_queue";
    private final static String MISSING_CONNECTION = "Missing Connection";
    private final static String MISSING_CHANNEL = "Missing Channel";
    private final static String SEND_ERROR = "Send Error";
    private final static String SENT_OK = "OK";
    private final static String EXCHANGE_NAME = "MKT";
    private static final long MAX_CONNECTION_ATTEMPTS = 50;

    private ConnectionFactory busFactory;
    private Connection busConnection;
    private Channel busChannel;

    private String hostName;
    private String virtualHost;
    private int port;
    private String userName;
    private String password;

    private static int connectionAttempts = 0;

    private static volatile boolean isConnecting;
    private static volatile boolean isConnected;


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

    public MKTBusSender(String hostName, String virtualHost, int port, String userName, String password) 
    {
        this.hostName = hostName;
        this.virtualHost = virtualHost;
        this.port = port;
        this.password = password;
        connectionAttempts = 0;
        isConnecting = false;
        isConnected = false;
		/*
		Property	Default Value
		Username	"guest"
		Password	"guest"
		Virtual host	"/"
		Hostname	"localhost"
		port	5672 for regular connections, 5671 for connections that use TLS
		*/

    }

    public boolean connect()
    {
        try {
            isConnecting =true;
            LOG.info("creating bus factory {}@{}", userName, hostName);
            // Set up the connection and channel
            busFactory = new ConnectionFactory();
            busFactory.setUsername(userName);
            busFactory.setPassword(password);
            busFactory.setVirtualHost(virtualHost);
            busFactory.setHost(hostName);
            busFactory.setPort(port);
            LOG.info("successfully created bus factory {}@{},  connecting", userName, hostName);

            busConnection = busFactory.newConnection();
            LOG.info("successfully created connection {}@{}, creating bus channel", userName, hostName);

            busChannel = busConnection.createChannel();
            busChannel.exchangeDeclare(EXCHANGE_NAME, "topic");
            busChannel.queueDeclare(QUOTEREQ_QUEUE_NAME, false, false, false, null);
            busChannel.queueDeclare(QUOTERES_QUEUE_NAME, false, false, false, null);
            busChannel.queueDeclare(QUOTE_QUEUE_NAME, false, false, false, null);
            busChannel.queueDeclare(TRADE_QUEUE_NAME, false, false, false, null);
            busChannel.queueDeclare(PRICE_QUEUE_NAME, false, false, false, null);
            LOG.info("successfully created bus channel {}@{}, binding queues", userName, hostName);
            
            busChannel.queueBind(QUOTEREQ_QUEUE_NAME, EXCHANGE_NAME, "QUOTEREQ.BOND.*.*");
            busChannel.queueBind(QUOTE_QUEUE_NAME, EXCHANGE_NAME, "QUOTE.BOND.*.*");
            busChannel.queueBind(QUOTERES_QUEUE_NAME, EXCHANGE_NAME, "QUOTERES.BOND.*.*");
            busChannel.queueBind(TRADE_QUEUE_NAME, EXCHANGE_NAME, "TRADE.BOND.*.*");
            busChannel.queueBind(PRICE_QUEUE_NAME, EXCHANGE_NAME, "PRICE.BOND.#");
            isConnecting=false;
            isConnected=true;
            return true;

        } catch(IOException | TimeoutException e){
            LOG.error("Error connecting Bus: {}\n{}", e.getLocalizedMessage(), Utils.stackTraceToString(e));
            tryScheduleReconnection();
            return false;
        }
    }

    private void tryScheduleReconnection() {
        // connectionAttempts starts at 0
        if (connectionAttempts >= MAX_CONNECTION_ATTEMPTS) {
            LOG.error("Connection FAILED after too many attempts");
            return;
        }

        // exponential backoff
        final long backoffTimeMs = Utils.exponentialBackoffTimeMs(connectionAttempts);
        LOG.info("Attempting reconnection in {}ms...", backoffTimeMs);
        new Timer().schedule(new ConnectionTask(this), backoffTimeMs);
    }

    public void disconnect() {
        if (busFactory == null || busConnection==null || busChannel == null) {
            LOG.info("Cannot disconnect: never connected");
            return;
        }

        try {
            isConnected = false;
            busConnection.close();
            busChannel.close();
            busFactory=null;
        } catch (IOException | TimeoutException e) {
            LOG.error("Cannot disconnect: {}\n{}", e.getLocalizedMessage(), Utils.stackTraceToString(e));
        }
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
        try {
            busChannel.basicPublish("", QUOTEREQ_QUEUE_NAME, null, serializedQuoteRequest);
        }
        catch (IOException  e){
            LOG.error("Error sending Quote Request " + quoteRequest + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
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
        try {
            busChannel.basicPublish("", QUOTERES_QUEUE_NAME, null, serializedQuoteResponse);
        }
        catch (IOException  e){
            LOG.error("Error sending Quote Response " + quoteResponse + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
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
        try{
            busChannel.basicPublish("", QUOTE_QUEUE_NAME, null, serializedQuote);
        }
        catch (IOException  e){
            LOG.error("Error sending Quote" + quote + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
        LOG.info(" [x] Sent '" + quote + "'");
        return SENT_OK;
    }

    public String SendOnBus(Price price) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedPrice = price.toByteArray();
        try{
            busChannel.basicPublish("", PRICE_QUEUE_NAME, null, serializedPrice);
        }
        catch (IOException  e){
            LOG.error("Error sending Price " + price + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
        LOG.info(" [x] Sent '" + price + "'");
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

    public String GetTopic(Price price)
    {
        String  priceType;

        switch (price.getType().getNumber()){
            case PriceType.TYPE_Composite_VALUE:
                priceType = new String("COMP");
                break;
            case PriceType.TYPE_Indicative_VALUE:
                priceType = new String("IND");
                break;
            case PriceType.TYPE_Tier_VALUE:
                priceType = new String("TIER");
                break;
            case PriceType.TYPE_CD4PM_VALUE:
                priceType = new String("CD4PM");
                break;
            case PriceType.TYPE_CORP4PM_VALUE:
                priceType = new String("CORP4PM");
                break;
            case PriceType.TYPE_CorporateSpread_VALUE:
                priceType = new String("CORPSPREAD");
                break;
            default:
                priceType = new String("");
                break;
        }
        return "PRICE." + "BOND." + priceType + "." + price.getSecurityID();
    }

    private static class ConnectionTask extends TimerTask {
        private final MKTBusSender busSender;

        ConnectionTask(MKTBusSender busSender) {
            this.busSender = busSender;
        }

        @Override
        public void run() {
            synchronized (this) {
                isConnecting = true;
            }
            connectionAttempts++;

            if (busSender.connect()) {
                LOG.info("Reconnected. Issuing flush...");
            }
        }
    }
/* 
    private static class AMQPConnectionListener implements ExceptionListener {
        private static final Logger LOG = LoggerFactory.getLogger(AMQPConnectionListener.class);

        private final AMQPClient client;

        public AMQPConnectionListener(AMQPClient client) {
            this.client = client;
        }
        @Override
        public void onException(@NotNull JMSException e) {
            LOG.error("Exception listener invoked: {}", e.getLocalizedMessage());
            client.onConnectionLost();
        }
    }
        */
}


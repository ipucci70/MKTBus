package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.beans.ExceptionListener;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;

import org.example.Market.MarketQuoteRequest;
import org.example.Market.MarketQuoteResponse;
import org.example.Market.MarketPrice;
import org.example.Market.MarketQuote;
import org.example.Utils;
import org.example.Market.MarketPriceType;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class MKTBusManager {

    private static final Logger LOG = LogManager.getLogger(MKTBusManager.class);

    private final static String QUOTEREQ_QUEUE_NAME_OUT = "quotereq_mkt_queue_out";
    private final static String QUOTERES_QUEUE_NAME_OUT = "quoteres_mkt_queue_out";
    private final static String QUOTE_QUEUE_NAME_OUT = "quote_mkt_queue_out";
    private final static String TRADE_QUEUE_NAME_OUT = "quote_mkt_queue_out";
    private final static String PRICE_QUEUE_NAME_OUT = "prc_mkt_queue_out";

    private final static String QUOTEREQ_QUEUE_NAME_IN = "quotereq_mkt_queue_in";
    private final static String QUOTERES_QUEUE_NAME_IN = "quoteres_mkt_queue_in";
    private final static String QUOTE_QUEUE_NAME_IN = "quote_mkt_queue_in";
    private final static String TRADE_QUEUE_NAME_IN = "quote_mkt_queue_in";

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

    private ExecutorService threadPool = Executors.newFixedThreadPool(20);

    public MKTBusManager(String hostName, String virtualHost, int port, String userName, String password) 
    {
        this.hostName = hostName;
        this.virtualHost = virtualHost;
        this.port = port;
        this.password = password;
        this.userName = userName;
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
        LOG.info("created Bus Manager host {} port {} username {} ", this.hostName, this.port, this.userName);
    }

    public boolean connect()
    {
        try {
            isConnecting =true;
            LOG.info("creating bus factory {}@{}:{}", userName, hostName, port);
            // Set up the connection and channel
            busFactory = new ConnectionFactory();
            busFactory.setUsername(userName);
            busFactory.setPassword(password);
            busFactory.setVirtualHost(virtualHost);
            busFactory.setHost(hostName);
            busFactory.setPort(port);
            LOG.info("successfully created bus factory {}@{}:{},  connecting", userName, hostName, port);

            busConnection = busFactory.newConnection();
            LOG.info("successfully created connection {}@{}:{}, creating bus channel", userName, hostName, port);

            busChannel = busConnection.createChannel();
            busChannel.exchangeDeclare(EXCHANGE_NAME, "topic");
            busChannel.queueDeclare(QUOTEREQ_QUEUE_NAME_OUT, false, false, false, null);
            busChannel.queueDeclare(QUOTERES_QUEUE_NAME_OUT, false, false, false, null);
            busChannel.queueDeclare(QUOTE_QUEUE_NAME_OUT, false, false, false, null);
            busChannel.queueDeclare(TRADE_QUEUE_NAME_OUT, false, false, false, null);
            busChannel.queueDeclare(PRICE_QUEUE_NAME_OUT, false, false, false, null);
            busChannel.queueDeclare(QUOTEREQ_QUEUE_NAME_IN, false, false, false, null);
            busChannel.queueDeclare(QUOTERES_QUEUE_NAME_IN, false, false, false, null);
            busChannel.queueDeclare(QUOTE_QUEUE_NAME_IN, false, false, false, null);
            busChannel.queueDeclare(TRADE_QUEUE_NAME_IN, false, false, false, null);
        
            LOG.info("successfully created bus channel {}@{}:{}, binding queues", userName, hostName, port);
            
            busChannel.queueBind(QUOTEREQ_QUEUE_NAME_OUT, EXCHANGE_NAME, "QUOTEREQ.BOND.*.*");
            busChannel.queueBind(QUOTE_QUEUE_NAME_OUT, EXCHANGE_NAME, "QUOTE.BOND.*.*");
            busChannel.queueBind(QUOTERES_QUEUE_NAME_OUT, EXCHANGE_NAME, "QUOTERES.BOND.*.*");
            busChannel.queueBind(TRADE_QUEUE_NAME_OUT, EXCHANGE_NAME, "TRADE.BOND.*.*");
            busChannel.queueBind(PRICE_QUEUE_NAME_OUT, EXCHANGE_NAME, "PRICE.BOND.#");
            busChannel.queueBind(QUOTEREQ_QUEUE_NAME_IN, EXCHANGE_NAME, "QUOTEREQ.BOND.*.*");
            busChannel.queueBind(QUOTE_QUEUE_NAME_IN, EXCHANGE_NAME, "QUOTE.BOND.*.*");
            busChannel.queueBind(QUOTERES_QUEUE_NAME_IN, EXCHANGE_NAME, "QUOTERES.BOND.*.*");
            busChannel.queueBind(TRADE_QUEUE_NAME_IN, EXCHANGE_NAME, "TRADE.BOND.*.*");

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
            isConnecting=false;
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

    public boolean waitForConnection() {
        synchronized (this) {
            while (isConnecting) {
                try {
                    this.wait();
                } catch (InterruptedException ignored) { }
            }
        }
        return true;
    }

    //used to send transactions to matching engine
    public String sendOnBus(MarketQuoteRequest marketQuoteRequest) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedQuoteRequest = marketQuoteRequest.toByteArray();
        try {
            busChannel.basicPublish("", QUOTEREQ_QUEUE_NAME_OUT, null, serializedQuoteRequest);
        }
        catch (IOException  e){
            LOG.error("Error sending Quote Request " + marketQuoteRequest + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
        LOG.info(" [x] Sent '" + marketQuoteRequest + "'");
        return SENT_OK;
    }

    //used to send transactions back from matching engine
    public String sendBackOnBus(MarketQuoteRequest marketQuoteRequest) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedQuoteRequest = marketQuoteRequest.toByteArray();
        try {
            busChannel.basicPublish("", QUOTEREQ_QUEUE_NAME_IN, null, serializedQuoteRequest);
        }
        catch (IOException  e){
            LOG.error("Error sending Quote Request " + marketQuoteRequest + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
        LOG.info(" [x] Sent back'" + marketQuoteRequest + "'");
        return SENT_OK;
    }

    //used to send transactions to matching engine
    public String sendOnBus(MarketQuoteResponse marketQuoteResponse) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedQuoteResponse = marketQuoteResponse.toByteArray();
        try {
            busChannel.basicPublish("", QUOTERES_QUEUE_NAME_OUT, null, serializedQuoteResponse);
        }
        catch (IOException  e){
            LOG.error("Error sending Quote Response " + marketQuoteResponse + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
        LOG.info(" [x] Sent '" + marketQuoteResponse + "'");
        return SENT_OK;
    }

    //used to send transactions back from matching engine
    public String sendBackOnBus(MarketQuoteResponse marketQuoteResponse) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedQuoteResponse = marketQuoteResponse.toByteArray();
        try {
            busChannel.basicPublish("", QUOTERES_QUEUE_NAME_IN, null, serializedQuoteResponse);
        }
        catch (IOException  e){
            LOG.error("Error sending Quote Response " + marketQuoteResponse + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
        LOG.info(" [x] Sent '" + marketQuoteResponse + "'");
        return SENT_OK;
    }

    //used to send transactions to matching engine
    public String sendOnBus(MarketQuote marketQuote) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedQuote = marketQuote.toByteArray();
        try{
            busChannel.basicPublish("", QUOTE_QUEUE_NAME_OUT, null, serializedQuote);
        }
        catch (IOException  e){
            LOG.error("Error sending Quote" + marketQuote + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
        LOG.info(" [x] Sent '" + marketQuote + "'");
        return SENT_OK;
    }

    //used to send transactions to matching engine
    public String sendBackOnBus(MarketQuote marketQuote) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedQuote = marketQuote.toByteArray();
        try{
            busChannel.basicPublish("", QUOTE_QUEUE_NAME_IN, null, serializedQuote);
        }
        catch (IOException  e){
            LOG.error("Error sending Quote" + marketQuote + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
        LOG.info(" [x] Sent '" + marketQuote + "'");
        return SENT_OK;
    }

    //used to publish prices
    public String sendOnBus(MarketPrice marketPrice) throws IOException
    {
        if (busConnection==null){
            return MISSING_CONNECTION;
        }
        if (busChannel==null){
            return MISSING_CHANNEL;
        }
        byte[] serializedPrice = marketPrice.toByteArray();
        try{
            busChannel.basicPublish("", PRICE_QUEUE_NAME_OUT, null, serializedPrice);
        }
        catch (IOException  e){
            LOG.error("Error sending Price " + marketPrice + e.getLocalizedMessage(), Utils.stackTraceToString(e));
            return SEND_ERROR;
        }
        LOG.info(" [x] Sent '" + marketPrice + "'");
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
    public String getTopic(MarketQuoteRequest marketQuoteRequest)
    {
        return "QUOTEREQ." + "BOND." + marketQuoteRequest.getTypeValue() + "." + marketQuoteRequest.getIssuerMemberID() + "." + marketQuoteRequest.getFirstLeg().getSecurityID();
    }

    public String getTopic(MarketQuote marketQuote)
    {
        return "QUOTE." + "BOND." + marketQuote.getTypeValue() + "." + marketQuote.getCounterpartMemberID() + "." + marketQuote.getFirstLeg().getSecurityID();
    }

    public String getTopic(MarketQuoteResponse marketQuoteResponse)
    {
        return "QUOTERES." + "BOND." + marketQuoteResponse.getTypeValue() + "." + marketQuoteResponse.getIssuerMemberID() + "." + marketQuoteResponse.getFirstLeg().getSecurityID();
    }

    public String getTopic(MarketPrice marketPrice)
    {
        String  priceType;

        switch (marketPrice.getType().getNumber()){
            case MarketPriceType.TYPE_Composite_VALUE:
                priceType = new String("COMP");
                break;
            case MarketPriceType.TYPE_Indicative_VALUE:
                priceType = new String("IND");
                break;
            case MarketPriceType.TYPE_Tier_VALUE:
                priceType = new String("TIER");
                break;
            case MarketPriceType.TYPE_CD4PM_VALUE:
                priceType = new String("CD4PM");
                break;
            case MarketPriceType.TYPE_CORP4PM_VALUE:
                priceType = new String("CORP4PM");
                break;
            case MarketPriceType.TYPE_CorporateSpread_VALUE:
                priceType = new String("CORPSPREAD");
                break;
            default:
                priceType = new String("");
                break;
        }
        return "PRICE." + "BOND." + priceType + "." + marketPrice.getSecurityID();
    }

    private static class ConnectionTask extends TimerTask {
        private final MKTBusManager busSender;

        ConnectionTask(MKTBusManager busSender) {
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
                isConnecting=false;
                isConnected=true;
            }
        }
    }

    @FunctionalInterface
    public interface QuoteRequestCallback{
        void handle(MarketQuoteRequest marketQuoteRequest);
    }

    @FunctionalInterface
    public interface QuoteCallback{
        void handle(MarketQuote marketQuote);
    }

    @FunctionalInterface
    public interface QuoteResponseCallback{
        void handle(MarketQuoteResponse marketQuoteResponse);
    }

    @FunctionalInterface
    public interface PriceCallback{
        void handle(MarketPrice marketPrice);
    }

    //used by matching engine to receive transactions, be aware that the callback will be running on a separate (pool) thread
    public boolean receiveMarketQuoteRequest(QuoteRequestCallback quoteRequestCallback){

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            threadPool.submit(() -> {
                byte[] body = delivery.getBody();
                try {
                    // Decode the Protocol Buffer message
                    MarketQuoteRequest marketQuoteRequest = MarketQuoteRequest.parseFrom(body);
                    LOG.info("received a quote request from bus" + marketQuoteRequest.toString());
                    quoteRequestCallback.handle(marketQuoteRequest);
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Failed to parse Protocol Buffer message: " + 
                    e.getLocalizedMessage(),
                    Utils.stackTraceToString(e));
                }
            });
        };

        try {
            busChannel.basicConsume(QUOTEREQ_QUEUE_NAME_OUT, true, deliverCallback, consumerTag -> {});
        }catch (IOException e){
            LOG.error("Exception in basicConsume: " + 
            e.getLocalizedMessage(),
            Utils.stackTraceToString(e));
            return false;
        }

        return true;
    }

    //used by access points to receive back transactions, be aware that the callback will be running on a separate (pool) thread
    public boolean receiveBackMarketQuoteRequest(QuoteRequestCallback quoteRequestCallback){

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            threadPool.submit(() -> {
                byte[] body = delivery.getBody();
                try {
                    // Decode the Protocol Buffer message
                    MarketQuoteRequest marketQuoteRequest = MarketQuoteRequest.parseFrom(body);
                    LOG.info("received a quote request from bus" + marketQuoteRequest.toString());
                    quoteRequestCallback.handle(marketQuoteRequest);
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Failed to parse Protocol Buffer message: " + 
                    e.getLocalizedMessage(),
                    Utils.stackTraceToString(e));
                }
            });
        };

        try {
            busChannel.basicConsume(QUOTEREQ_QUEUE_NAME_IN, true, deliverCallback, consumerTag -> {});
        }catch (IOException e){
            LOG.error("Exception in basicConsume: " + 
            e.getLocalizedMessage(),
            Utils.stackTraceToString(e));
            return false;
        }

        return true;
    }

    //used by matching engine to receive transactions, be aware that the callback will be running on a separate (pool) thread
    public boolean receiveMarketQuote(QuoteCallback quoteCallback){

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            threadPool.submit(() -> {
                byte[] body = delivery.getBody();
                try {
                    // Decode the Protocol Buffer message
                    MarketQuote marketQuote = MarketQuote.parseFrom(body);
                    LOG.info("received a quote from bus" + marketQuote.toString());
                    quoteCallback.handle(marketQuote);
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Failed to parse Protocol Buffer message: " + 
                    e.getLocalizedMessage(),
                    Utils.stackTraceToString(e));
                }
            });
        };

        try {
            busChannel.basicConsume(QUOTE_QUEUE_NAME_OUT, true, deliverCallback, consumerTag -> {});
        }catch (IOException e){
            LOG.error("Exception in basicConsume: " + 
            e.getLocalizedMessage(),
            Utils.stackTraceToString(e));
            return false;
        }

        return true;
    }

    //used by access points to receive transactions, be aware that the callback will be running on a separate (pool) thread
    public boolean receiveBackMarketQuote(QuoteCallback quoteCallback){

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            threadPool.submit(() -> {
                byte[] body = delivery.getBody();
                try {
                    // Decode the Protocol Buffer message
                    MarketQuote marketQuote = MarketQuote.parseFrom(body);
                    LOG.info("received a quote from bus" + marketQuote.toString());
                    quoteCallback.handle(marketQuote);
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Failed to parse Protocol Buffer message: " + 
                    e.getLocalizedMessage(),
                    Utils.stackTraceToString(e));
                }
            });
        };

        try {
            busChannel.basicConsume(QUOTE_QUEUE_NAME_IN, true, deliverCallback, consumerTag -> {});
        }catch (IOException e){
            LOG.error("Exception in basicConsume: " + 
            e.getLocalizedMessage(),
            Utils.stackTraceToString(e));
            return false;
        }

        return true;
    }

    //used by matching engine to receive transactions, be aware that the callback will be running on a separate (pool) thread
    public boolean receiveMarketQuoteResponse(QuoteResponseCallback quoteResponseCallback){

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            threadPool.submit(() -> {
                byte[] body = delivery.getBody();
                try {
                    // Decode the Protocol Buffer message
                    MarketQuoteResponse marketQuoteResponse = MarketQuoteResponse.parseFrom(body);
                    LOG.info("received a quote response from bus" + marketQuoteResponse.toString());
                    quoteResponseCallback.handle(marketQuoteResponse);
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Failed to parse Protocol Buffer message: " + 
                    e.getLocalizedMessage(),
                    Utils.stackTraceToString(e));
                }
            });
        };

        try {
            busChannel.basicConsume(QUOTERES_QUEUE_NAME_OUT, true, deliverCallback, consumerTag -> {});
        }catch (IOException e){
            LOG.error("Exception in basicConsume: " + 
            e.getLocalizedMessage(),
            Utils.stackTraceToString(e));
            return false;
        }

        return true;
    }

    //used by access points to receive back transactions, be aware that the callback will be running on a separate (pool) thread
    public boolean receiveBackMarketQuoteResponse(QuoteResponseCallback quoteResponseCallback){

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            threadPool.submit(() -> {
                byte[] body = delivery.getBody();
                try {
                    // Decode the Protocol Buffer message
                    MarketQuoteResponse marketQuoteResponse = MarketQuoteResponse.parseFrom(body);
                    LOG.info("received a quote response from bus" + marketQuoteResponse.toString());
                    quoteResponseCallback.handle(marketQuoteResponse);
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Failed to parse Protocol Buffer message: " + 
                    e.getLocalizedMessage(),
                    Utils.stackTraceToString(e));
                }
            });
        };

        try {
            busChannel.basicConsume(QUOTERES_QUEUE_NAME_IN, true, deliverCallback, consumerTag -> {});
        }catch (IOException e){
            LOG.error("Exception in basicConsume: " + 
            e.getLocalizedMessage(),
            Utils.stackTraceToString(e));
            return false;
        }

        return true;
    }
    //used to read prices received by the CIP, be aware that the callback will be running on a separate (pool) thread
    public boolean receiveMarketPrice(PriceCallback priceCallback){

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            threadPool.submit(() -> {
                byte[] body = delivery.getBody();
                try {
                    // Decode the Protocol Buffer message
                    MarketPrice marketPrice = MarketPrice.parseFrom(body);
                    LOG.info("received a price from bus" + marketPrice.toString());
                    priceCallback.handle(marketPrice);
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Failed to parse Protocol Buffer message: " + 
                    e.getLocalizedMessage(),
                    Utils.stackTraceToString(e));
                }
            });
        };

        try {
            busChannel.basicConsume(PRICE_QUEUE_NAME_OUT, true, deliverCallback, consumerTag -> {});
        }catch (IOException e){
            LOG.error("Exception in basicConsume: " + 
            e.getLocalizedMessage(),
            Utils.stackTraceToString(e));
            return false;
        }

        return true;
    }
}


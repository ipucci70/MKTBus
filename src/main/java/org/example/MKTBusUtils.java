package org.example;


import org.example.Market.MarketInternalInfo;
import org.example.Utils;

public class MKTBusUtils {
    public static MarketInternalInfo setCreationTimeInInternals(MarketInternalInfo marketInternalInfo){
        MarketInternalInfo.Builder marketInternalInfoBuilder = marketInternalInfo.toBuilder();

        long timeNow = Utils.getTimeNowMicroseconds();

        marketInternalInfoBuilder.setTransactionCreationTime(timeNow);
        return marketInternalInfoBuilder.build();
    }

    public static MarketInternalInfo setSentOnBusTimeInInternals(MarketInternalInfo marketInternalInfo){
        MarketInternalInfo.Builder marketInternalInfoBuilder = marketInternalInfo.toBuilder();

        long timeNow = Utils.getTimeNowMicroseconds();

        marketInternalInfoBuilder.setSentOnBusTime(timeNow);
        return marketInternalInfoBuilder.build();
    }

    public static MarketInternalInfo setTakenFromBusTimeInInternals(MarketInternalInfo marketInternalInfo){
        MarketInternalInfo.Builder marketInternalInfoBuilder = marketInternalInfo.toBuilder();

        long timeNow = Utils.getTimeNowMicroseconds();

        marketInternalInfoBuilder.setTakenFromBusTime(timeNow);
        return marketInternalInfoBuilder.build();
    }

    public static MarketInternalInfo setProcessedTimeInInternals(MarketInternalInfo marketInternalInfo){
        MarketInternalInfo.Builder marketInternalInfoBuilder = marketInternalInfo.toBuilder();

        long timeNow = Utils.getTimeNowMicroseconds();

        marketInternalInfoBuilder.setProcessedTime(timeNow);
        return marketInternalInfoBuilder.build();
    }

    public static MarketInternalInfo setSentBackToBusTimeInInternals(MarketInternalInfo marketInternalInfo){
        MarketInternalInfo.Builder marketInternalInfoBuilder = marketInternalInfo.toBuilder();

        long timeNow = Utils.getTimeNowMicroseconds();

        marketInternalInfoBuilder.setSentBackToBusTime(timeNow);
        return marketInternalInfoBuilder.build();
    }

    public static MarketInternalInfo setTakenBackFromBusTimeInInternals(MarketInternalInfo marketInternalInfo){
        MarketInternalInfo.Builder marketInternalInfoBuilder = marketInternalInfo.toBuilder();

        long timeNow = Utils.getTimeNowMicroseconds();

        marketInternalInfoBuilder.setTakenBackFromBusTime(timeNow);
        return marketInternalInfoBuilder.build();
    }

    public static MarketInternalInfo setCloseTransactionTimeInInternals(MarketInternalInfo marketInternalInfo){
        MarketInternalInfo.Builder marketInternalInfoBuilder = marketInternalInfo.toBuilder();

        long timeNow = Utils.getTimeNowMicroseconds();

        marketInternalInfoBuilder.setCloseTransactionTime(timeNow);
        return marketInternalInfoBuilder.build();
    }

    // transactionid is formed as <prefix>-<local time in nanoseconds>-<counter 6 decimal digits>
    public static String createNewTransactionID(String prefix, int counter)
    {
        long timeNowNanoSeconds = Utils.getTimeNowNanoseconds();
        String transactionID = prefix + "-" + String.format("%15d", timeNowNanoSeconds) + "-" + String.format("%06d", counter);

        return transactionID;
    }

    public static MarketInternalInfo setTransactionIDInInternals(MarketInternalInfo marketInternalInfo, String transactionID){
        MarketInternalInfo.Builder marketInternalInfoBuilder = marketInternalInfo.toBuilder();

        marketInternalInfoBuilder.setTransactionID(transactionID);
        return marketInternalInfoBuilder.build();
    }
}

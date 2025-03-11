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
}

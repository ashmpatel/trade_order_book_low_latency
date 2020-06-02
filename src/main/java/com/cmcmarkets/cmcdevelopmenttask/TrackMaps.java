package com.cmcmarkets.cmcdevelopmenttask;

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TrackMaps {

    private ChronicleMap<String, IntValue> ordersTotal = null;
    private ChronicleMap<LongValue, IntValue> totalForOrders = null;
    private ChronicleMap<LongValue, String> order = null;
    private ChronicleMap<String, Map> symbolToSetOfOrderId = null;
    private Map<String, ReentrantReadWriteLock> locks;
    private Side buyOrSell;

    public TrackMaps(ChronicleMap<String, IntValue> orders_total, ChronicleMap<LongValue, IntValue> total_for_order,
                     ChronicleMap<LongValue, String> order, ChronicleMap<String, Map> symbolToSetOfOrderId,
                     Map<String, ReentrantReadWriteLock> locks,
                     Side buyOrSell) {
        this.ordersTotal = orders_total;
        this.totalForOrders = total_for_order;
        this.order = order;
        this.symbolToSetOfOrderId = symbolToSetOfOrderId;
        this.locks = locks;
        this.buyOrSell=buyOrSell;
    }

    public Map<String, ReentrantReadWriteLock> getLocks() {
        return locks;
    }

    public Side getBuyOrSell() {
        return buyOrSell;
    }

    public ChronicleMap<String, IntValue> getOrdersTotal() {
        return ordersTotal;
    }
    public ChronicleMap<LongValue, IntValue> getTotalForOrders() {
        return totalForOrders;
    }

    public ChronicleMap<LongValue, String> getOrder() {
        return order;
    }
    public ChronicleMap<String, Map> getSymbolToSetOfOrderId() {
        return symbolToSetOfOrderId;
    }

}

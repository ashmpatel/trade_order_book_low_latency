package com.cmcmarkets.cmcdevelopmenttask;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TrackMaps2 {

    private Map<String, Integer> ordersTotal = null;
    private Map<Long, Integer> totalForOrders = null;
    private Map<Long, String> order = null;
    private Map<String, Map<Long,Integer>> symbolToSetOfOrderId = null;
    private Map<String, ReentrantReadWriteLock> locks;
    private Side buyOrSell;

    public TrackMaps2(Map<String, Integer> orders_total, Map<Long, Integer> total_for_order,
                      Map<Long, String> order, Map<String, Map<Long,Integer>> symbolToSetOfOrderId,
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

    public Map<String, Integer> getOrdersTotal() {
        return ordersTotal;
    }
    public Map<Long, Integer> getTotalForOrders() {
        return totalForOrders;
    }

    public Map<Long, String> getOrder() {
        return order;
    }
    public Map<String, Map<Long,Integer>> getSymbolToSetOfOrderId() {
        return symbolToSetOfOrderId;
    }

}

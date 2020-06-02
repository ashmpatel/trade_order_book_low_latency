package com.cmcmarkets.cmcdevelopmenttask;

import net.openhft.chronicle.core.values.IntValue;

import java.util.Map;

public interface OrderHandler {

    void addOrder(Order order);

    void modifyOrder(OrderModification orderModification);

    void removeOrder(long orderId);

    double getCurrentPrice(String symbol, int quantity, Side side);

    /**
     * Please implement this method so we are able to create an instance
     * of your OrderHandler implementation.
     */
    static OrderHandler createInstance() {
        return new OrderHandlerImpl();
    }

    /* ive added this for testing purposes but not needed otherwise */
    Map<IntValue, Map>  getBuyOrders();

    Map<IntValue, Map>  getSellOrders();


}

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
        return new OrderHandlerImpl3();
    }

    /* ive added this for testing purposes but not needed otherwise */
    long getBuyOrders();

    long getSellOrders();


    int getBuyPriceFor(String symbol, long orderId);

    int getSellPriceFor(String symbol, long orderId);

    int getBuyQuantityFor(String symbol, int price);

    int getSellQuantityFor(String symbol, int price);

    long getOrderId(String symbol, int price);

}

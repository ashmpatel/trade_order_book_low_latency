package com.cmcmarkets.cmcdevelopmenttask;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * Keeps track of the accumultaed quantity and orders for the same price.
 * This is then used for both buy and sell sides as I maintain the difference of buy and sell orders
 * in the maps
 */

public class AccumulatedOrder implements Serializable {

    private LongAdder totalOrders;
    /* I do not need to store Price herer as I already have it in the upper level map.
      I did this for debugging purposes
     */
    private int price;
    private long totalQuantity;
    private long orderId;

    public AccumulatedOrder() {
        this.totalOrders = new LongAdder();
        this.price = 0;
        this.totalQuantity = 0;
        this.orderId= 0;
    }

    public AccumulatedOrder(LongAdder totalOrders, int price, long totalQuantity , long orderId) {
        this.totalOrders = totalOrders;
        this.price = price;
        this.totalQuantity = totalQuantity;
        this.orderId = orderId;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public LongAdder getTotalOrders() {
        return totalOrders;
    }

    public void setTotalOrders(LongAdder totalOrders) {
        this.totalOrders = totalOrders;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public long getTotalQuantity() {
        return totalQuantity;
    }

    public void setTotalQuantity(long totalQuantity) {
        this.totalQuantity = totalQuantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccumulatedOrder that = (AccumulatedOrder) o;
        return orderId == that.orderId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId);
    }
}

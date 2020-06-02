package com.cmcmarkets.cmcdevelopmenttask;

import java.io.Serializable;
import java.util.Objects;

public class Order  {
    private final long orderId;
    private final String symbol;
    private final Side side;
    private final int price;
    private final int quantity;

    public Order(long orderId, String symbol, Side side, int price, int quantity) {
        this.orderId = orderId;
        this.symbol = symbol;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
    }

    public long getOrderId() {
        return orderId;
    }

    public String getSymbol() {
        return symbol;
    }

    public Side getSide() {
        return side;
    }

    public int getPrice() {
        return price;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return price == order.price &&
                Objects.equals(symbol, order.symbol) &&
                side == order.side;
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, side, price);
    }
}

package com.cmcmarkets.cmcdevelopmenttask;

import com.sun.tools.javac.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class OrderHandlerImpl3 implements OrderHandler {

    private static final String SEPARATOR= ":";
    private static TrackMaps2 BUY_SIDE_MAPS = null;
    private static TrackMaps2 SELL_SIDE_MAPS = null;

    // SYMBOL:Price -> Total Quantity
    private static final Map<String, Integer> buy_orders_total = new ConcurrentHashMap<>();
    // order id to the SYMBOL:PRICE
    private static final Map<Long, String> buy_order = new ConcurrentHashMap<>();
    // total quantity for each unique order
    private static final Map<Long, Integer> buy_total_for_order = new ConcurrentHashMap<>();
    // SYmbol-> map(orderid, price)
    private static final Map<String, Map<Long,Integer>>  buySymbolToSetOfOrderId = new ConcurrentHashMap<>();

    private static final Map<String, Integer> sell_orders_total = new ConcurrentHashMap<>();
    private static final Map<Long, String> sell_order = new ConcurrentHashMap<>();
    private static final Map<Long, Integer> sell_total_for_order = new ConcurrentHashMap<>();
    private static final Map<String, Map<Long,Integer>> sellSymbolToSetOfOrderId = new ConcurrentHashMap<>();

    // reduce contention by separating the buy and sell lock sides
    private static final Map<String, ReentrantReadWriteLock> buySideLocks = new ConcurrentHashMap<>();
    private static final Map<String, ReentrantReadWriteLock> sellSidelocks = new ConcurrentHashMap<>();

    static {
        BUY_SIDE_MAPS = new TrackMaps2(buy_orders_total, buy_total_for_order, buy_order,  buySymbolToSetOfOrderId, buySideLocks, Side.BUY);
        SELL_SIDE_MAPS = new TrackMaps2(sell_orders_total,  sell_total_for_order, sell_order, sellSymbolToSetOfOrderId, sellSidelocks, Side.SELL);
    }

    public int getBuyPriceFor(String symbol, long orderId){
        return buySymbolToSetOfOrderId.get(symbol).get(orderId);
    }

    public int getSellPriceFor(String symbol, long orderId){
        return sellSymbolToSetOfOrderId.get(symbol).get(orderId);
    }

    public int getBuyQuantityFor(String symbol, int price) {
        return buy_orders_total.get(createSymbol(symbol,price));
    }

    public int getSellQuantityFor(String symbol, int price) {
        return sell_orders_total.get(createSymbol(symbol,price));
    }

    public long getOrderId(String symbol, int price) {
        Map<Long,Integer> orders= buySymbolToSetOfOrderId.get(symbol);
        for ( Long orderId: orders.keySet()) {
            if (orders.get(orderId)== price) return orderId;
        }
        return 0;
    }

    public OrderHandlerImpl3() {
        buy_orders_total.clear();
        // order id to the SYMBOL:PRICE:Buy/Sell
        buy_order.clear();
        // total quantity for each unique order
        buy_total_for_order.clear();
        // SYmbol-> map(orderid, price)
        buySymbolToSetOfOrderId.clear();

        sell_orders_total.clear();
        sell_order.clear();
        sell_total_for_order.clear();
        sellSymbolToSetOfOrderId.clear();

        // reduce contention by separating the buy and sell lock sides
        buySideLocks.clear();
        sellSidelocks.clear();
    }

    public static void main(String args[]) {
        OrderHandler orderHandler = new OrderHandlerImpl2();

        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));
        orderHandler.addOrder(new Order(2L, "MSFT", Side.SELL, 19, 4));
        orderHandler.addOrder(new Order(3L, "MSFT", Side.SELL, 21, 16));
        orderHandler.addOrder(new Order(4L, "MSFT", Side.SELL, 21, 1));
        orderHandler.addOrder(new Order(5L, "MSFT", Side.SELL, 22, 7));

        orderHandler.addOrder(new Order(6L, "MSFT", Side.BUY, 13, 5));

        orderHandler.modifyOrder(new OrderModification(6L, 15, 10));

        orderHandler.addOrder(new Order(7L, "MSFT", Side.BUY, 15, 20));
        orderHandler.removeOrder(7L);

        orderHandler.addOrder(new Order(8L, "MSFT", Side.BUY, 10, 13));
        orderHandler.addOrder(new Order(9L, "MSFT", Side.BUY, 10, 13));


        // get the prices and display the price
        double avgPrice = orderHandler.getCurrentPrice("MSFT", 6, Side.SELL);
        System.out.println(avgPrice);

        avgPrice = orderHandler.getCurrentPrice("MSFT", 17, Side.SELL);
        System.out.println(avgPrice);

        avgPrice = orderHandler.getCurrentPrice("MSFT", 30, Side.SELL);
        System.out.println(avgPrice);

        avgPrice = orderHandler.getCurrentPrice("MSFT", 10, Side.BUY);
        System.out.println(avgPrice);

        avgPrice = orderHandler.getCurrentPrice("TEST", 10, Side.BUY);
        System.out.println(avgPrice);

    }

    /* does the updating of the price when a new order comes in */
    private void updateSide(@NotNull Order order) {

        final TrackMaps2 getMapsToUse;

        // which maps to look at
        if (order.getSide() == Side.BUY) {
            getMapsToUse = BUY_SIDE_MAPS;
        } else {
            getMapsToUse = SELL_SIDE_MAPS;
        }

        // orders map will never have a clash as its always a new order id
        getMapsToUse.getOrder().compute(order.getOrderId(), (key, value) -> {
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

            if (value == null) {
                value = createSymbol(order.getSymbol(), order.getPrice());
                getMapsToUse.getLocks().put(value, lock);
            }

            ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
            // update all the other structures by locking on Symbol:Price:Buy/Sell side
            // so the lock is held at quite a granular level i.e SYMBOL:PRICE:BUY or SELL
            writeLock.lock();
            try {
                Integer currentQuantity = getMapsToUse.getOrdersTotal().putIfAbsent(value, order.getQuantity());
                // if we already have orders at this price level then update the total quantity for this symbol+priev+buy/sell side
                if (currentQuantity!=null) getMapsToUse.getOrdersTotal().replace(value, currentQuantity + order.getQuantity());
                // unique map for order -> totalQuantity
                getMapsToUse.getTotalForOrders().put(order.getOrderId(), order.getQuantity());

                // track Symbol -> order id->price. Rather then use a set, im using a map and its keys to simulate a set
                // and i get the concurrency then
                ConcurrentHashMap<Long, Integer> orderIds = new ConcurrentHashMap<Long, Integer>();
                orderIds.put(order.getOrderId(), order.getPrice());

                Map<Long, Integer> orderIdsCurrent = getMapsToUse.getSymbolToSetOfOrderId().putIfAbsent(order.getSymbol(), orderIds);
                // if the key was already there then we have a map already for the Value so update it
                if (orderIdsCurrent != null) orderIdsCurrent.put(order.getOrderId(), order.getPrice());

            } finally {
                writeLock.unlock();
            }
            return value;
        });
    }


    @Override
    public void modifyOrder(@NotNull OrderModification orderModification) {

        Pair<String,TrackMaps2> mapsAndSide = getMapsAndBuyORSellSide(orderModification.getOrderId());

        // symbol will be SYMBOL:PRICE:BUY/SELL side
        final String symbol = mapsAndSide.fst;
        final TrackMaps2 getMapsToUse = mapsAndSide.snd;
        final Side buyOrSell = getMapsToUse.getBuyOrSell();

        // cant find the order, do nothing
        if (symbol==null) return;

        ReentrantReadWriteLock.WriteLock writeLock  = getMapsToUse.getLocks().get(symbol).writeLock();

        // update all the other structures by locking on Symbol:Price:Buy/Sell side
        // so the lock is held at quite a granular level i.e SYMBOL:PRICE:BUY or SELL
        writeLock.lock();

        try {
            // get previous order
            Integer totalForThisSymbolAndPrice = getMapsToUse.getOrdersTotal().get(symbol);
            // replace the total for this unique order and put in new one
            Integer prevTotalForThisOrder = getMapsToUse.getTotalForOrders().replace(orderModification.getOrderId(), orderModification.getNewQuantity());
            totalForThisSymbolAndPrice = totalForThisSymbolAndPrice - prevTotalForThisOrder;

            if (totalForThisSymbolAndPrice == 0) getMapsToUse.getOrdersTotal().remove(symbol);

            String oldSymbol = symbol.substring(0, symbol.indexOf(SEPARATOR));
            // update the prev total at the old price as the quantity is now changes for the old price
            getMapsToUse.getOrdersTotal().replace(symbol, totalForThisSymbolAndPrice);
            // put in the new price total
            getMapsToUse.getOrdersTotal().put(createSymbol(oldSymbol, orderModification.getNewPrice()), orderModification.getNewQuantity());

            // map order id to new SYMBOL:PRICE
            getMapsToUse.getOrder().replace(orderModification.getOrderId(), createSymbol(oldSymbol, orderModification.getNewPrice()));
            // update the price for this symbol->orderid->price
            Map<Long, Integer> orderIds = getMapsToUse.getSymbolToSetOfOrderId().get(oldSymbol);
            orderIds.replace(orderModification.getOrderId(), orderModification.getNewPrice());
        } finally {
            writeLock.unlock();
        }

    }


    @Override
    public void removeOrder(long orderId) {

        final Pair<String,TrackMaps2> mapsAndSide = getMapsAndBuyORSellSide(orderId);

        // symbol will be SYMBOL:PRICE:BUY/SELL side
        final String symbol = mapsAndSide.fst;
        final TrackMaps2 getMapsToUse= mapsAndSide.snd;

        // order doesn't exist on buy or sell
        if (symbol==null) return;

        ReentrantReadWriteLock.WriteLock writeLock = getMapsToUse.getLocks().get(symbol).writeLock();
        // update all the other structures by locking on Symbol:Price:Buy/Sell side
        // so the lock is held at quite a granular level i.e SYMBOL:PRICE:BUY or SELL
        writeLock.lock();

        String oldSymbol = symbol.substring(0, symbol.indexOf(SEPARATOR));
        try {
            // update the order total
            Integer totalForThisSymbolAndPrice = getMapsToUse.getOrdersTotal().get(symbol);
            Integer totalForThisOrder = getMapsToUse.getTotalForOrders().remove(orderId);
            totalForThisSymbolAndPrice = totalForThisSymbolAndPrice - totalForThisOrder;
            if (totalForThisSymbolAndPrice==0) getMapsToUse.getOrdersTotal().remove(symbol);
            else getMapsToUse.getOrdersTotal().replace(symbol, totalForThisSymbolAndPrice);

            getMapsToUse.getOrder().remove(orderId);
            // remove the price for this symbol->orderid->price
            getMapsToUse.getSymbolToSetOfOrderId().get(oldSymbol).remove(orderId);
        }
        finally {
            writeLock.unlock();
        }
    }

    @Override
    public double getCurrentPrice(String symbol, int quantity, Side side) {

        double avgPriceForOrder = 0;

        final TrackMaps2 getMapsToUse;

        // no use looking for quantity of a symbol or if the symbol doesn't exist
        // also avoid divide by 0 error as in the avg calc we use the quantity value
        if (quantity==0) return 0;

        // which maps to look at
        if (side == Side.BUY) {
            getMapsToUse = BUY_SIDE_MAPS;
        } else {
            getMapsToUse= SELL_SIDE_MAPS;
        }

        Map<Long,Integer> symbolToOrderIdAndPrice = getMapsToUse.getSymbolToSetOfOrderId().get(symbol);

        // if no prices for this symbol at all then return
        if (symbolToOrderIdAndPrice==null) return 0;

        // work on a copy of the prices for this symbol we got from the map
        // This way, no issues if this map is being modified afterwards
        Set<Integer> orderIdsForSymbol = symbolToOrderIdAndPrice.values().stream().parallel().collect(Collectors.toSet());
        List<Integer> sortedOrderIdsForSymbolByPrice = new LinkedList(orderIdsForSymbol);
        Collections.sort(sortedOrderIdsForSymbolByPrice);

        if (side == Side.BUY) {
            Collections.reverse(sortedOrderIdsForSymbolByPrice);
        }

        long totQuantity= quantity;
        // final price to be returned
        double totPrice=0;

        Integer totalQuantityOfSymbolAtThisPrice=0;

        ReentrantReadWriteLock.ReadLock readLock = null;

        // go through the quantities for this symbol+price+BUY/SELL, the list has been sorted for Sell and Buy appropriately
        for(Integer price: sortedOrderIdsForSymbolByPrice) {
            String value = createSymbol(symbol, price);

            readLock = getMapsToUse.getLocks().get(value).readLock();
            // update all the other structures by locking on Symbol:Price:Buy/Sell side
            // so the lock is held at quite a granular level i.e SYMBOL:PRICE:BUY or SELL
            readLock.lock();
            try {
                totalQuantityOfSymbolAtThisPrice = getMapsToUse.getOrdersTotal().get(value);
            }
            finally {
                readLock.unlock();
            }

            // we can get a null here as we got the list of symbol->orderId->price but in the mean time
            // this map can be modified by modifyorder and this order->price removed
            // so when we go to look it up, this can fail, so protect with null check
            if (totalQuantityOfSymbolAtThisPrice!=null) {
                // if we have filled the order then don't do any more work
                if (totQuantity <= totalQuantityOfSymbolAtThisPrice) {
                    totPrice = totPrice + totQuantity * price;
                    // soon as order is fullfilled, don't loop any more and return the result
                    break;
                } else {
                    // remaining order to fill so do the calcs
                    totQuantity = totQuantity - totalQuantityOfSymbolAtThisPrice;
                    totPrice = totPrice + totalQuantityOfSymbolAtThisPrice * price;
                }
            }
        }

        // final result
        avgPriceForOrder = totPrice/quantity;

        return avgPriceForOrder;
    }


    private Pair<String,TrackMaps2> getMapsAndBuyORSellSide(long orderId) {
        final Pair<String,TrackMaps2> mapsAndSide;

        // symbol will be SYMBOL:PRICE:BUY/SELL side
        String symbol = buy_order.get(orderId);
        // which maps to look at
        if (symbol != null) {
            mapsAndSide = new Pair(symbol, BUY_SIDE_MAPS);
        } else {
            symbol = sell_order.get(orderId);
            mapsAndSide= new Pair(symbol, SELL_SIDE_MAPS);
        }
        return mapsAndSide;

    }

    // checks if the order exists
    private boolean orderIdExists(long orderId) {
        Pair<String,TrackMaps2> mapsAndSide = getMapsAndBuyORSellSide(orderId);
        return mapsAndSide.fst!=null;
    }

    public void addOrder(@NotNull Order order) {
        if (orderIdExists(order.getOrderId())) return;
        updateSide(order);
    }

    @NotNull
    private String createSymbol(String symbolName, int price) {
        return symbolName + SEPARATOR + price;
    }

    //* accessors for debugging */
    public long  getBuyOrders() {
        return buy_total_for_order.size();
    }

    public long getSellOrders() {
        return sell_total_for_order.size();

    }

}



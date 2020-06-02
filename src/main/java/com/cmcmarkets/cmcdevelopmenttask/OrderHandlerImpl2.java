package com.cmcmarkets.cmcdevelopmenttask;

import com.sun.tools.javac.util.Pair;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class OrderHandlerImpl2 implements OrderHandler {

    private static final String SEPARATOR= ":";
    private static TrackMaps BUY_SIDE_MAPS = null;
    private static TrackMaps SELL_SIDE_MAPS = null;

    // SYMBOL:Price:buy/Sell -> Total Quantity
    private static final ChronicleMap<String, IntValue> buy_orders_total =  ChronicleMap
            .of(String.class, IntValue.class)
            .name("buy-orders-bid-side")
            .entries(10)
            .averageKeySize(4)
            .create();

    // order id to the SYMBOL:PRICE:Buy/Sell
    private static final ChronicleMap<LongValue, String> buy_order =  ChronicleMap
            .of(LongValue.class, String.class)
            .name("buy-orders-bid-side")
            .entries(10)
            .averageValueSize("MSFT".getBytes().length) //this is just a rough value but I can get it exact is needed
            .create();

    // total quantity for each unique order
    private static final ChronicleMap<LongValue, IntValue> buy_total_for_order =  ChronicleMap
            .of(LongValue.class, IntValue.class)
            .name("buy-orders-bid-side")
            .entries(10)
            .create();

    // SYmbol-> map(orderid, price)
    private static final ChronicleMap<String, Map>  buySymbolToSetOfOrderId = ChronicleMap
            .of(String.class, Map.class)
            .name("buy-orders-bid-side")
            .entries(10)
            .averageKeySize(4)
            .averageValueSize(1000) //this is just a rough value but I can get it exact is needed
            .create();

    private static final ChronicleMap<String, IntValue> sell_orders_total =  ChronicleMap
            .of(String.class, IntValue.class)
            .name("sell-orders-ask-side")
            .entries(10)
            .averageKeySize(4)
            .create();

    private static final ChronicleMap<LongValue, String> sell_order = ChronicleMap
            .of(LongValue.class, String.class)
            .name("sell-orders-ask-side")
            .entries(10)
            .averageValueSize("MSFT".getBytes().length) //this is just a rough value but I can get it exact is needed
            .create();

    private static final ChronicleMap<LongValue, IntValue> sell_total_for_order =  ChronicleMap
            .of(LongValue.class, IntValue.class)
            .name("sell-orders-ask-side")
            .entries(10)
            .create();


    private static final ChronicleMap<String, Map> sellSymbolToSetOfOrderId = ChronicleMap
            .of(String.class, Map.class)
            .name("sell-orders-ask-side")
            .entries(10)
            .averageKeySize(4)
            .averageValueSize(1000) //this is just a rough value but I can get it exact is needed
            .create();

    // reduce contention by separating the buy and sell lock sides
    private static final Map<String, ReentrantReadWriteLock> buySideLocks = new ConcurrentHashMap<>();
    private static final Map<String, ReentrantReadWriteLock> sellSidelocks = new ConcurrentHashMap<>();

    static {
        BUY_SIDE_MAPS = new TrackMaps(buy_orders_total, buy_total_for_order, buy_order,  buySymbolToSetOfOrderId, buySideLocks, Side.BUY);
        SELL_SIDE_MAPS = new TrackMaps(sell_orders_total,  sell_total_for_order, sell_order, sellSymbolToSetOfOrderId, sellSidelocks, Side.SELL);
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

        orderHandler.removeOrder(100L);

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

        final TrackMaps getMapsToUse;

        // which maps to look at
        if (order.getSide() == Side.BUY) {
            getMapsToUse = BUY_SIDE_MAPS;
        } else {
            getMapsToUse = SELL_SIDE_MAPS;
        }

        LongValue orderId = createLongValue(order.getOrderId());

        // orders map will never have a clash as its always a new order id
        getMapsToUse.getOrder().compute(orderId, (key, value) -> {

            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

            value = createSymbol(order.getSymbol(), order.getPrice());
            getMapsToUse.getLocks().put(value, lock);

            ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
            // update all the other structures by locking on Symbol:Price:Buy/Sell side
            // so the lock is held at quite a granular level i.e SYMBOL:PRICE:BUY or SELL
            writeLock.lock();
            try {
                IntValue quantity = createIntValue(order.getQuantity());

                IntValue currentQuantity = getMapsToUse.getOrdersTotal().get(value);
                if(currentQuantity==null) {
                    getMapsToUse.getOrdersTotal().put(value, quantity);
                }
                else {
                    // if we already have orders at this price level then update the total quantity for this symbol+price
                    IntValue newQuantity = createIntValue(currentQuantity.getValue() + order.getQuantity());
                    getMapsToUse.getOrdersTotal().replace(value, newQuantity);
                }

                // unique map for order -> totalQuantity
                getMapsToUse.getTotalForOrders().put(orderId, quantity);

                // track Symbol -> order id->price. Rather then use a set, im using a map and its keys to simulate a set
                // and i get the concurrency then
                Map<Long, Integer> orderIds =  new ConcurrentHashMap<Long,Integer>();
                orderIds.put(order.getOrderId(), order.getPrice());

                Map<Long, Integer> orderIdsCurrent = getMapsToUse.getSymbolToSetOfOrderId().get(order.getSymbol());
                // if the key was already there then we have a map already for the Value so update it
                if (orderIdsCurrent != null) {
                    orderIdsCurrent.put(order.getOrderId(), order.getPrice());
                    getMapsToUse.getSymbolToSetOfOrderId().replace(order.getSymbol(), orderIdsCurrent);
                }
                else {
                    getMapsToUse.getSymbolToSetOfOrderId().put(order.getSymbol(), orderIds);
                }

            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                writeLock.unlock();
            }
            return value;
        });
    }

    @Override
    public void modifyOrder(@NotNull OrderModification orderModification) {

        Pair<String,TrackMaps> mapsAndSide = getMapsAndBuyORSellSide(orderModification.getOrderId());

        // symbol will be SYMBOL:PRICE
        final String symbol = mapsAndSide.fst;
        final TrackMaps getMapsToUse = mapsAndSide.snd;

        // cant find the order, do nothing
        if (symbol==null) return;

        ReentrantReadWriteLock.WriteLock writeLock  = getMapsToUse.getLocks().get(symbol).writeLock();

        // update all the other structures by locking on Symbol:Price:Buy/Sell side
        // so the lock is held at quite a granular level i.e SYMBOL:PRICE:BUY or SELL
        writeLock.lock();

        try {
            // get previous order
            IntValue totalForThisSymbolAndPrice = getMapsToUse.getOrdersTotal().get(symbol);
            // replace the total for this unique order and put in new one
            LongValue orderId = createLongValue(orderModification.getOrderId());
            IntValue quantity = createIntValue(orderModification.getNewQuantity());

            // adjust the quantity that this order contributed to the total for this Symbol and Side i.e overall totals
            IntValue prevTotalForThisOrder = getMapsToUse.getTotalForOrders().replace(orderId, quantity);
            totalForThisSymbolAndPrice.setValue(totalForThisSymbolAndPrice.getValue() - prevTotalForThisOrder.getValue());
            // if the total for this symbol is 0, kep the map clean and delete it
            if (totalForThisSymbolAndPrice.getValue() == 0) getMapsToUse.getOrdersTotal().remove(symbol);
            // update the prev total at the old price as the quantity is now changes for the old price
            getMapsToUse.getOrdersTotal().replace(symbol, totalForThisSymbolAndPrice);

            // put in the new price total
            IntValue newQuantity =  createIntValue(orderModification.getNewQuantity());
            String oldSymbol = symbol.substring(0, symbol.indexOf(SEPARATOR));
            String oldSymbolLabel = createSymbol(oldSymbol, orderModification.getNewPrice());
            getMapsToUse.getOrdersTotal().put(oldSymbolLabel, newQuantity);

            // map order id to its new Price
            getMapsToUse.getOrder().replace(orderId, oldSymbolLabel);

            // update the price for this symbol->orderid->price
            getMapsToUse.getSymbolToSetOfOrderId().get(oldSymbol).replace(orderModification.getOrderId(), orderModification.getNewPrice());
        } finally {
            writeLock.unlock();
        }

    }


    @Override
    public void removeOrder(long orderId) {

        final Pair<String,TrackMaps> mapsAndSide = getMapsAndBuyORSellSide(orderId);

        // symbol will be SYMBOL:PRICE
        final String symbol = mapsAndSide.fst;
        // order doesn't exist on buy or sell
        if (symbol==null) return;

        final TrackMaps getMapsToUse= mapsAndSide.snd;

            ReentrantReadWriteLock.WriteLock writeLock = getMapsToUse.getLocks().get(symbol).writeLock();
            // update all the other structures by locking on Symbol:Price:Buy/Sell side
            // so the lock is held at quite a granular level i.e SYMBOL:PRICE:BUY or SELL
            writeLock.lock();

            String oldSymbol = symbol.substring(0, symbol.indexOf(SEPARATOR));
            try {

                //create the orderId intvalue
                LongValue orderIdL = createLongValue(orderId);

                // update the running total and removes this orderid from the other maps
                // create the orderId intvalue
                IntValue prevTotalForThisSymbolAndPrice = getMapsToUse.getOrdersTotal().get(symbol);
                // update the 1-1 mapping for orderid->quantity
                IntValue totalForThisOrder = getMapsToUse.getTotalForOrders().remove(orderIdL);
                // update  running total
                prevTotalForThisSymbolAndPrice.setValue(prevTotalForThisSymbolAndPrice.getValue() - totalForThisOrder.getValue());
                // keep the maps clean
                if (prevTotalForThisSymbolAndPrice.getValue()==0) getMapsToUse.getOrdersTotal().remove(symbol);
                 else getMapsToUse.getOrdersTotal().replace(symbol, prevTotalForThisSymbolAndPrice);

                 // no need to keep track of this orderid->price
                getMapsToUse.getOrder().remove(orderIdL);

                // remove the price for this symbol->orderid->price mapping
                getMapsToUse.getSymbolToSetOfOrderId().get(oldSymbol).remove(orderIdL);
            }
            finally {
                writeLock.unlock();
            }
    }

    @Override
    public double getCurrentPrice(String symbol, int quantity, Side side) {

        // no use looking for quantity of a symbol or if the symbol doesn't exist
        // also avoid divide by 0 error as in the avg calc we use the quantity value
        if (quantity==0) return 0;

        final TrackMaps getMapsToUse;

        // which maps to look at
        if (side == Side.BUY) {
            getMapsToUse = BUY_SIDE_MAPS;
        } else {
            getMapsToUse = SELL_SIDE_MAPS;
        }

        Map<LongValue,IntValue> symbolToOrderIdAndPrice = getMapsToUse.getSymbolToSetOfOrderId().get(symbol);
        // if no prices for this symbol at all then return
        if (symbolToOrderIdAndPrice==null) return 0;

        // work on a copy of the prices for this symbol we got from the map
        // This way, no issues if this map is being modified afterwards
        Set<IntValue> orderIdsForSymbol = symbolToOrderIdAndPrice.values().stream().parallel().collect(Collectors.toSet());
        List<Integer> sortedOrderIdsForSymbolByPrice = new LinkedList(orderIdsForSymbol);
        Collections.sort(sortedOrderIdsForSymbolByPrice);

        if (side == Side.BUY) Collections.reverse(sortedOrderIdsForSymbolByPrice);

        LongValue totQuantity = createLongValue(quantity);

        // final price to be returned
        double totPrice=0;

        IntValue totalQuantityOfSymbolAtThisPrice = null;

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

            // we should not get a null here as we got the list of symbol->orderId->price
            if (totalQuantityOfSymbolAtThisPrice!=null) {
                // if we have filled the order then don't do any more work
                if (totQuantity.getValue() <= totalQuantityOfSymbolAtThisPrice.getValue()) {
                    totPrice = totPrice + totQuantity.getValue() * price;
                    // soon as order is fulfilled, don't loop any more and return the result
                    break;
                } else {
                    // remaining order to fill so do the calcs
                    totQuantity.setValue(totQuantity.getValue() - totalQuantityOfSymbolAtThisPrice.getValue());
                    totPrice = totPrice + totalQuantityOfSymbolAtThisPrice.getValue() * price;
                }
            }
        }

        return totPrice/quantity;
    }

    private LongValue createLongValue(long longVal) {
        LongValue longValSet = Values.newHeapInstance(LongValue.class);;
        longValSet.setValue(longVal);
        return longValSet;
    }

    private IntValue createIntValue(int intVal) {
        IntValue intValSet = Values.newHeapInstance(IntValue.class);;
        intValSet.setValue(intVal);
        return intValSet;
    }

    private Pair<String,TrackMaps> getMapsAndBuyORSellSide(long orderId) {
        final Pair<String,TrackMaps> mapsAndSide;

        LongValue orderIdL= Values.newHeapInstance(LongValue.class);;
        orderIdL.setValue(orderId);
        // symbol will be SYMBOL:PRICE:BUY/SELL side
        String symbol = buy_order.get(orderIdL);
        // which maps to look at
        if (symbol != null) {
            mapsAndSide = new Pair(symbol, BUY_SIDE_MAPS);
        } else {
            symbol = sell_order.get(orderIdL);
            mapsAndSide= new Pair(symbol, SELL_SIDE_MAPS);
        }
        return mapsAndSide;

    }

    // checks if the order exists
    private boolean orderIdExists(long orderId) {
        Pair<String,TrackMaps> mapsAndSide = getMapsAndBuyORSellSide(orderId);
        return mapsAndSide.fst!=null;
    }

    public void addOrder(@NotNull Order order) {
        if (orderIdExists(order.getOrderId())) return;
        updateSide(order);
    }

    @NotNull
    private String createSymbol(String symbolName, int price) {
        return symbolName + SEPARATOR + price + SEPARATOR;
    }

    //* accessors for debugging */
    public Map<IntValue, Map>  getBuyOrders() {
       return null;
    }

    public Map<IntValue, Map>   getSellOrders() {
        return null;

    }

}


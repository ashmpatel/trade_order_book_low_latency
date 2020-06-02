package com.cmcmarkets.cmcdevelopmenttask;

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class OrderHandlerImpl implements OrderHandler {

    private ChronicleMap<IntValue, Map> buy_orders = ChronicleMap
            .of(IntValue.class, Map.class)
            .name("buy-orders-bid-side")
            .entries(10)
            .averageValueSize(100) //this is just a rough value but I can get it exact is needed
            .create();

    private ChronicleMap<IntValue, Map> sell_orders = ChronicleMap
            .of(IntValue.class, Map.class)
            .name("sell-orders-ask-side")
            .entries(10)
            .averageValueSize(100)
            .create();

    private ChronicleMap<Long, String> mapForOrdeIdToSymbolsBuySide = ChronicleMap
            .of(Long.class, String.class)
            .name("map_from_order_id_to_symbol_buy")
            .entries(10)
            .averageValueSize(100)
            .create();

    private ChronicleMap<Long, String> mapForOrdeIdToSymbolsSellSide = ChronicleMap
            .of(Long.class, String.class)
            .name("map_from_order_id_to_symbol_sell")
            .entries(10)
            .averageValueSize(100)
            .create();

    /**
     * noargs constructor so the tests can get an implementation
     */
    public OrderHandlerImpl() {
        buy_orders.clear();
        sell_orders.clear();
        mapForOrdeIdToSymbolsBuySide.clear();
        mapForOrdeIdToSymbolsSellSide.clear();
    }

    /**
     * Updates the Buy Or Sell side map. Increments the order quantity, totalorders and set the price is its a new Order
     *
     * @param order
     * @param updateMap
     */
    private void updateSide(Order order, Map<IntValue, Map> updateMap, Map<Long, String> mapForOrdeIdToSymbols, Side side) {

        // rather then using string, im using IntValue from chronicleMap as it works faster with this
        IntValue symbol;
        symbol=genHash(order.getSymbol());

        // MSFT-> value( map (Price-> Set<AccumulatedOrders> ) )

        updateMap.compute(symbol, (key, value) -> {
            // IF this symbol is new then add the map and order to the set
            if (value == null) {
                AccumulatedOrder newOrder = new AccumulatedOrder();
                newOrder.setTotalQuantity(order.getQuantity());
                newOrder.getTotalOrders().increment();
                newOrder.setPrice(order.getPrice());
                newOrder.setOrderId(order.getOrderId());
                value = new HashMap<Integer, Set>();
                Set tradesForSymbolAtThisPrice = new HashSet();
                tradesForSymbolAtThisPrice.add(newOrder);
                value.put(order.getPrice(), tradesForSymbolAtThisPrice);
            } else {

                // This Symbol exists BUT no Orders at this price
                Set<AccumulatedOrder> allOrdersForSymbolAndPrice = (Set<AccumulatedOrder>) value.get(order.getPrice());
                // if we have Not seen this price before whic is the most likely case then ADD an entry otherwise replace
                if (allOrdersForSymbolAndPrice == null) {
                    // create the new entry
                    AccumulatedOrder newOrder = new AccumulatedOrder();
                    newOrder.setTotalQuantity(order.getQuantity());
                    newOrder.getTotalOrders().increment();
                    newOrder.setPrice(order.getPrice());
                    newOrder.setOrderId(order.getOrderId());

                    // add to the set of orders for this price
                    Set tradesForSymbolAtThisPrice = new HashSet();
                    tradesForSymbolAtThisPrice.add(newOrder);
                    Map newPriceValueMap = new HashMap();
                    newPriceValueMap.put(order.getPrice(), tradesForSymbolAtThisPrice);
                    value.putAll(newPriceValueMap);

                } else {
                    // symbol exists, price exists so we just need to add to the order list
                    AccumulatedOrder newOrder = new AccumulatedOrder();
                    newOrder.setTotalQuantity(order.getQuantity());
                    newOrder.getTotalOrders().increment();
                    newOrder.setPrice(order.getPrice());
                    newOrder.setOrderId(order.getOrderId());
                    allOrdersForSymbolAndPrice.add(newOrder);
                    // replace the order of the same price with the added order to the set
                    value.replace(order.getPrice(), allOrdersForSymbolAndPrice);
                }
            }

            // at this point I can maintain a running total of symbol->price->TotalQuantity
            // This is a possible improvement to this code.

            // keep the other maps in sync to track which order id is for which symbol and which side
            mapForOrdeIdToSymbols.compute(order.getOrderId(), (key2, value2) -> {
                value2 = order.getSymbol();

                return value2;
            });

            return value;

        });
    }


    /**
     * Adds an order to the order book , either buy or sell order book
     * Same sort of processing works on both order lists so refactored to use the same method and pass in the maps
     *
     * @param order
     */
    @Override
    public void addOrder(Order order) {

        // don't add dupe order ids
        String symbol = mapForOrdeIdToSymbolsBuySide.get(order.getOrderId());
        if (symbol!=null) {
            return;
        } else {
            symbol = mapForOrdeIdToSymbolsSellSide.get(order.getOrderId());
            if (symbol != null) {
                return;
            }
        }

        // can add a check for NOT adding order with 0 quantity as these cant be used for pricing

        // we either add order to the buy side or sell side
        if (order.getSide() == Side.BUY) {
            updateSide(order, buy_orders, mapForOrdeIdToSymbolsBuySide, Side.BUY);
        } else {
            updateSide(order, sell_orders, mapForOrdeIdToSymbolsSellSide, Side.SELL);
        }

    }

    /**
     * Modified an order , either buy or sell order book
     *
     * @param orderModification
     */
    @Override
    public void modifyOrder(OrderModification orderModification) {

        Map<IntValue, Map> mapToUpdate;
        Map<Long, String> mapForOrderIdToSymbol;

        /*
        Workout out of the order is on the buy or sell side using the map.
        This avoids searching all the orders as we just search the Buy or sell side map.
        One way of d.i.y lock striping. Guava tripe map would be useful but its not threadsafe as
        not as good as ChroncileMap if you want a distributed setup
        */
        String symbolSet = mapForOrdeIdToSymbolsBuySide.get(orderModification.getOrderId());
        if (symbolSet != null) {
            mapToUpdate = buy_orders;
            mapForOrderIdToSymbol = mapForOrdeIdToSymbolsBuySide;
        } else {
            symbolSet = mapForOrdeIdToSymbolsSellSide.get(orderModification.getOrderId());
            mapToUpdate = sell_orders;
            mapForOrderIdToSymbol = mapForOrdeIdToSymbolsSellSide;
        }

        // just need to get the first value
        String symbol = symbolSet;

        // we have a symbol to use as lookup
        if (symbol != null) {
            IntValue symbolCode = Values.newHeapInstance(IntValue.class);
            symbolCode.setValue(symbol.hashCode());

            // modify the order on the Correct map set above in the IF
            mapToUpdate.compute(symbolCode, (key, value) -> {
                // get all the orders for the symbol at the Price of the new Order
                Map<Long, Set> allOrdersForSymbolAndPrice = (Map<Long, Set>) value;

                Set<AccumulatedOrder> allOrdersForThisPrice;

                // walk through all orders for the Symbol to see if the order id matches
                for (Map.Entry<Long, Set> entry : allOrdersForSymbolAndPrice.entrySet()) {

                    allOrdersForThisPrice = entry.getValue();

                    LongAdder temp = new LongAdder();
                    LongAdder newQuantity = new LongAdder();
                    newQuantity.add(orderModification.getNewQuantity());
                    AccumulatedOrder newOrder;

                    for (AccumulatedOrder order : allOrdersForThisPrice) {
                        // found the order with the Order id so set new price and new quantity
                        if (order.getOrderId() == orderModification.getOrderId()) {
                            newOrder = new AccumulatedOrder(temp, orderModification.getNewPrice(), orderModification.getNewQuantity(), orderModification.getOrderId());

                            // if the price has not changed then just replace the set of orders for this price otherwise we have to put in a new value for the map
                            if (order.getPrice() == orderModification.getNewPrice()) {
                                allOrdersForThisPrice.remove(order);
                                allOrdersForThisPrice.add(newOrder);
                            } else {
                                // as we have modified this price for this order in the set of orders, remove this order for this price from the map
                                allOrdersForThisPrice.remove(order);
                                Set existingOrdersForThisPrice = allOrdersForSymbolAndPrice.get(orderModification.getNewPrice());
                                if (existingOrdersForThisPrice == null) {
                                    existingOrdersForThisPrice = new HashSet<AccumulatedOrder>();
                                }
                                // add the new order to any existing orders for this price and put it in the overall map
                                existingOrdersForThisPrice.add(newOrder);
                                value.putIfAbsent(orderModification.getNewPrice(), existingOrdersForThisPrice);

                                if (allOrdersForThisPrice.size()==0) {
                                    // done keep empty order lists for prices that don't have orders.
                                    // keep the maps clean.
                                    value.remove(order.getPrice());
                                }

                                // This null case will not happen as we are modifying an order so we will always find prices for it
                                // just some defensive coding
                                String symbolSetNew = mapForOrderIdToSymbol.get(orderModification.getOrderId());
                                if (symbolSetNew == null) {
                                    // the old order was replaced so remove this order from the list of orders->symbols
                                    mapForOrderIdToSymbol.remove(order.getOrderId());
                                    mapForOrderIdToSymbol.put(orderModification.getOrderId(), symbolSetNew);
                                } else {
                                    mapForOrderIdToSymbol.replace(orderModification.getOrderId(), symbolSetNew);
                                }

                            }

                        }
                    }

                }
                return value;
            });

        }

    }


    /**
     * generates a hash from the string
     * @param symbol
     * @return
     */
    private IntValue genHash(String symbol) {
        IntValue symbolCode=null;
        if (symbol!=null) {
            symbolCode = Values.newHeapInstance(IntValue.class);
            symbolCode.setValue(symbol.hashCode());
        }
        return symbolCode;
    }

    @Override
    public void removeOrder(long orderId) {
        Map<IntValue, Map> mapToUpdate=null;
        Map<Long, String> mapForOrderIdToSymbol;

        /*
         Workout out of the order is on the buy or sell side using the map.
         This avoids searching all the orders as we just search the Buy or sell side map.
         */
        String symbol = mapForOrdeIdToSymbolsBuySide.get(orderId);
        boolean orderExists=false;

        // the order is is on the buy side
        if (symbol!=null) {
            mapToUpdate = buy_orders;
            mapForOrderIdToSymbol = mapForOrdeIdToSymbolsBuySide;
            orderExists=true;
        } else {
            // check sell side
            symbol = mapForOrdeIdToSymbolsSellSide.get(orderId);
            // order is on the sell side
            mapToUpdate = sell_orders;
            mapForOrderIdToSymbol = mapForOrdeIdToSymbolsSellSide;
            if (symbol!=null) orderExists=true;

        }

        // if order id does not exist, we cant remove it
        if (!orderExists) return;

        IntValue symbolHash = genHash(symbol);

        boolean[] found = new boolean[1];

        // we now know if the order is is Buy or sell side as well as for what Symbol
        // so we can limit the search to the symbols for that order is and buy/sell side
        mapToUpdate.compute(symbolHash, (key, value) -> {

            // get all the orders for the symbol at the Price of the new Order
            Map<Integer,Set> allOrdersForSymbolAndPrice = (Map<Integer,Set>) value;

            Set<AccumulatedOrder> allOrdersForThisPrice = null;

            AccumulatedOrder orderToRemove = null;
            Integer price=0;
            boolean notDeleted=true;
            // walk through all orders for the Symbol to see if the order id matches
            if (notDeleted) {
                for (Map.Entry<Integer, Set> entry : allOrdersForSymbolAndPrice.entrySet()) {
                    if (notDeleted) {
                        // set of Orders for this symbol and price
                        price = entry.getKey();
                        allOrdersForThisPrice = entry.getValue();
                        for (AccumulatedOrder order : allOrdersForThisPrice) {
                            // found the order with the Order id so Remove this order
                            // do the remove atomically i.e from the map and the other map from order id to symbol
                            if (order.getOrderId() == orderId) {
                                found[0] = mapForOrderIdToSymbol.keySet().removeIf(keyOrderId -> keyOrderId == orderId);
                                orderToRemove = order;
                                notDeleted = false;
                            }
                        }
                    }
                }
            }

            if (found[0]) {
                allOrdersForThisPrice.remove(orderToRemove);
                if (allOrdersForThisPrice.size()==0) {
                    allOrdersForSymbolAndPrice.remove(price);
                    mapForOrderIdToSymbol.remove(orderToRemove.getOrderId());

                } else {
                    allOrdersForSymbolAndPrice.replace(price, allOrdersForThisPrice);
                }
                value=allOrdersForSymbolAndPrice;
            }
            return value;
        });

    }


    @Override
    public double getCurrentPrice(String symbol, int quantity, Side side) {

        Map<IntValue, Map> mapToSearch;

        // pass a value out of a lambda
        double avgPriceForOrder = 0;

        // the order is is on the buy side
        if (side == Side.BUY) {
            mapToSearch = buy_orders;
        } else {
            // order is on the sell side
            mapToSearch = sell_orders;
        }

        // no use looking for quantity of a symbol or if the symbol doesn't exist
        // also avoid divide by 0 error as in the avg calc we use the quantity value
        if ((quantity==0) || ( !mapToSearch.containsKey(genHash(symbol)))) return 0;

        // get all the orders for this symbol.
        // I know that other threads could modify the maps while we are iterating thru it
        // but the requirement is not there to stop avoiding updates so I don't lock
        // Other option is to make a copy of this result so then Im working on a copy so no locking needed
        Map<Integer,Set> allOrdersForSymbolAndPrice=mapToSearch.get(genHash(symbol));

        Map<Integer, Set> result;

        /* buy and sell need different sorting order. Sort in parallel */
        if (side == Side.SELL) {
            result = allOrdersForSymbolAndPrice.entrySet().stream().parallel()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        } else {
            result = allOrdersForSymbolAndPrice.entrySet().stream().parallel()
                    .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder())) // just passing in reverse order as BUY side needs that logic
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        }

        // is the order fulfilled yet or not
        boolean orderFulfilled = false;

        long totQuantityAtThisPrice=0;
        long totQuantity= quantity;

        // final price to be returned
        double totPrice=0;

        for (Map.Entry<Integer, Set> entry : result.entrySet()) {
            Set<AccumulatedOrder> orders = entry.getValue();
            Integer price = entry.getKey();

            // sum all quantities as this price
            totQuantityAtThisPrice = orders.stream()
                    .parallel() // don't need this for this small example but just showing that for larger data sets, this is useful
                    .map(x -> x.getTotalQuantity())
                    .reduce(0L, ArithmeticUtils::add);

            // if we have filled the order then don't do any more work
            if (totQuantity <= totQuantityAtThisPrice) {
                totPrice = totPrice + totQuantity * price;
                // soon as order is fullfilled, don't loop any more and return the result
                break;
            } else {
                // remaining order to fill so do the calcs
                totQuantity = totQuantity - totQuantityAtThisPrice;
                totPrice = totPrice + totQuantityAtThisPrice * price;

            }

        }
        // final result
        avgPriceForOrder= totPrice/quantity;
        // System.out.println("Avg price : " +  avgPriceForOrder);

        return avgPriceForOrder;
    }

    //* accessors for debugging */
    public Map<IntValue, Map>  getBuyOrders() {
        return buy_orders;
    }

    public Map<IntValue, Map>  getSellOrders() {
        return sell_orders;
    }

    public Map<Long, String>  getOrderIdToSymbolBuy() {
        return mapForOrdeIdToSymbolsBuySide;
    }

    public Map<Long, String>  getOrderIdToSymbolSell() {
        return mapForOrdeIdToSymbolsSellSide;
    }

}

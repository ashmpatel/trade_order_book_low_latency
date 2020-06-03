package com.cmcmarkets.cmcdevelopmenttask;


import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.values.Values;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class ExampleData {
    static  OrderHandler orderHandler;

    @Before
    public void createOrderHandler() {
        // this clears the maps as well
        orderHandler = OrderHandler.createInstance();
    }


    @Test
    // test for null case
    public void testGettingAPriceWhenNoneExistSellSide() {
        double currentPrice= orderHandler.getCurrentPrice("MSFT", 6, Side.SELL);
        assertEquals("Price is 0 for SELL side",0, Double.compare(currentPrice,0));

    }

    @Test
    // test for null case
    public void testGettingAPriceWhenNoneExistBuySide() {
        double currentPrice= orderHandler.getCurrentPrice("MSFT", 6, Side.BUY);
        assertEquals("Price is 0 for BUY side",0, Double.compare(currentPrice,0));

    }

    @Test
    // test for single case add SELl
    public void testGettingASinglePriceSell() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));

        double currentPrice= orderHandler.getCurrentPrice("MSFT", 6, Side.SELL);
        assertEquals("Price is 19",0, Double.compare(currentPrice,19));
    }

    @Test
    // test for single case add BUY
    public void testGettingASinglePriceBuy() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 19, 8));

        double currentPrice= orderHandler.getCurrentPrice("MSFT", 6, Side.BUY);
        assertEquals("Price is 19",0, Double.compare(currentPrice,19));
    }

    @Test
    // test for single case add SELL but look for BUY
    public void testGettingASinglePriceButWrongSideBUY() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));

        double currentPrice= orderHandler.getCurrentPrice("MSFT", 6, Side.BUY);
        assertEquals("Price is 0",0, Double.compare(currentPrice,0));
    }

    @Test
    // test for single case add BUY but look for SELL
    public void testGettingASinglePriceButWrongSideSELL() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 19, 8));

        double currentPrice= orderHandler.getCurrentPrice("MSFT", 6, Side.SELL);
        assertEquals("Price is 0",0, Double.compare(currentPrice,0));
    }

    @Test
    // test for single case add BUY but look for SELL
    public void testAddingASingleBUYOrderResultinOneEntry() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 19, 8));
        long testData=orderHandler.getBuyOrders();

        assertEquals("One entry in buy orders",true, testData==1);
    }

    @Test
    public void testAddingASingleSELLOrderResultsiOneEntry() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));
        long testData=orderHandler.getSellOrders();

        assertEquals("One entry in sell orders",true, testData==1);
    }

    @Test
    public void testWhenBuyOrderIsPlacedSellOrdersAreNotAffected() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 19, 8));
        long testData= testData=orderHandler.getSellOrders();

        assertEquals("No entry in SELL orders",true, testData==0);
    }

    @Test
    public void testWhenSellOrderIsPlacedBuyOrdersAreNotAffected() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));
        long testData = testData=orderHandler.getBuyOrders();

        assertEquals("No entry in BUY orders",true, testData==0);
    }

    @Test
    // if an order Id is duplicated, ignore the second insert. Modify can be used to update orders
    public void testAddDupeOrderIdSellSide() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 25, 8));
        long testData =orderHandler.getSellOrders();

        assertEquals("One entry in sell orders",true, testData==1);
    }


    @Test
    // if an order Id is duplicated, ignore the second insert. Modify can be used to update orders
    public void testAddDupeOrderIdBuySide() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 19, 8));
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 25, 8));
        long testData=orderHandler.getBuyOrders();

        assertEquals("One entry in buy orders",true, testData==1);
    }

    @Test
    public void testAddingTwoSellOrders() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));
        orderHandler.addOrder(new Order(2L, "MSFT", Side.SELL, 25, 8));

        double currentPrice= orderHandler.getCurrentPrice("MSFT", 6, Side.SELL);
        assertEquals("Price is 19.0",0, Double.compare(currentPrice,19.0));

    }

    @Test
    public void testAddingTwoSellOrdersCountCheck() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));
        orderHandler.addOrder(new Order(2L, "MSFT", Side.SELL, 25, 8));

        long testData=orderHandler.getSellOrders();
        IntValue symbol = Values.newHeapInstance(IntValue.class);
        symbol.setValue("MSFT".hashCode());

        assertEquals("Size is 2",true, testData==2);

    }

    @Test
    public void testAddingTwoBuyOrdersCountCheck() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 19, 8));
        orderHandler.addOrder(new Order(2L, "MSFT", Side.BUY, 25, 8));

        long testData=orderHandler.getBuyOrders();
        IntValue symbol = Values.newHeapInstance(IntValue.class);
        symbol.setValue("MSFT".hashCode());

        assertEquals("Size is 2",true, testData==2);

    }

    @Test
    public void testAddingTwoSellOrdersForDiffSymbolsCountCheck() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));
        orderHandler.addOrder(new Order(2L, "TRI", Side.SELL, 25, 18));

        long sizeOne=orderHandler.getSellQuantityFor("MSFT", 19);

        long sizeTwo=orderHandler.getSellQuantityFor("TRI", 25);


        assertEquals("Size is 1 when we place different SELL orders",true, sizeOne==8 && sizeTwo==18 );

    }

    @Test
    public void testAddingTwoBuyOrdersForDiffSymbolsCountCheck() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 19, 8));
        orderHandler.addOrder(new Order(2L, "TRI", Side.BUY, 25, 80));

        long sizeOne=orderHandler.getBuyQuantityFor("MSFT", 19);

        long sizeTwo=orderHandler.getBuyQuantityFor("TRI", 25);

        assertEquals("Size is 2 when we place different BUY orders",true, sizeOne==8 && sizeTwo==80 );

    }

    @Test
    public void testAddingTwoBuyOrdersForDiffBuyAndSellCountCheck() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 19, 8));
        orderHandler.addOrder(new Order(2L, "MSFT", Side.SELL, 25, 8));

        long sizeOne=orderHandler.getBuyOrders();

        long sizeTwo=orderHandler.getSellOrders();

        assertEquals("Size is 2 when we place different BUY orders",true, sizeOne==1 && sizeTwo==1 );

    }

    @Test
    public void testRemoveOrderWhenNoneExists() {
        orderHandler.removeOrder(1L);
        long testDataBuy=orderHandler.getBuyOrders();
        long testDataSell=orderHandler.getSellOrders();

        assertEquals("Size is 0 for both buy and sell",true, testDataBuy==0 && testDataSell==0);

    }


    @Test
    public void testRemoveOrderWhenOneExistsBuy() {
        orderHandler.addOrder(new Order(2L, "TRI", Side.BUY, 25, 8));
        orderHandler.removeOrder(2L);

        IntValue symbol = Values.newHeapInstance(IntValue.class);
        symbol.setValue("TRI".hashCode());

        long sizeOne=orderHandler.getBuyOrders();

        long sizeTwo=orderHandler.getSellOrders();


        assertEquals("Size is 0 for both buy and sell",true, sizeOne==0 && sizeTwo==0 );

    }


    @Test
    public void testRemoveOrderWhenOneExistsSell() {
        orderHandler.addOrder(new Order(2L, "TRI", Side.SELL, 25, 8));
        orderHandler.removeOrder(2L);

        IntValue symbol = Values.newHeapInstance(IntValue.class);
        symbol.setValue("TRI".hashCode());

        long sizeOne=orderHandler.getSellOrders();

        long sizeTwo=orderHandler.getBuyOrders();


        assertEquals("Size is 0 for both buy and sell",true, sizeOne ==0 && sizeTwo==0 );

    }


    @Test
    public void testRemoveOrderWhenOneExistsBuyLeaveSellUntouched() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 11, 8));
        orderHandler.addOrder(new Order(2L, "TRI", Side.BUY, 25, 8));

        orderHandler.removeOrder(2L);

        IntValue symbol = Values.newHeapInstance(IntValue.class);
        symbol.setValue("TRI".hashCode());

        long sizeOne=orderHandler.getBuyOrders();

        long sizeTwo=orderHandler.getSellOrders();


        assertEquals("Size is 0 for both buy and sell",true, sizeOne==0 && sizeTwo==1 );

    }

    @Test
    public void testRemoveOrderWhenOneExistsBuyLeaveBuyUntouched() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 11, 8));
        orderHandler.addOrder(new Order(2L, "MSFT", Side.BUY, 25, 8));

        orderHandler.removeOrder(1L);

        long sizeOne=orderHandler.getSellOrders();

        long sizeTwo=orderHandler.getBuyOrders();


        assertEquals("Size is 0 for both buy and sell",true, sizeOne==0 && sizeTwo==1 );

    }

    @Test
    public void testRemoveOrderWhenExists() {
        orderHandler.removeOrder(7L);

        long sizeOne=orderHandler.getBuyOrders();
        long sizeTwo=orderHandler.getSellOrders();

        assertEquals("Size is 0 for both buy and sell",true, sizeOne==0 && sizeTwo==0 );

    }

    @Test
    public void testModifyBuyOrder() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.BUY, 19, 8));
        orderHandler.modifyOrder(new OrderModification(1L, 15, 10));

        long sizeOne=orderHandler.getBuyOrders();
        long sizeTwo=orderHandler.getSellOrders();

        long price=orderHandler.getBuyPriceFor("MSFT", 1L);
        long quantity=orderHandler.getBuyQuantityFor("MSFT", 15);
        long orderId= orderHandler.getOrderId("MSFT", 15);


        assertEquals(true,price==15);
        assertEquals(true,quantity==10);
        assertEquals(true,orderId==1L);


        assertEquals("Size is 0 for both buy and sell",true, sizeOne==1 && sizeTwo==0 );

    }

    @Test
    public void testModifySellOrder() {
        orderHandler.addOrder(new Order(1L, "MSFT", Side.SELL, 19, 8));
        orderHandler.modifyOrder(new OrderModification(1L, 15, 10));

        long testData=orderHandler.getSellOrders();

        IntValue symbol = Values.newHeapInstance(IntValue.class);
        symbol.setValue("MSFT".hashCode());
/*
        Set sizeOne = (Set)testData.get(symbol).get(15);
        Iterator iter= sizeOne.iterator();
        AccumulatedOrder retrievedOrder= (AccumulatedOrder) iter.next();

        assertEquals(true,retrievedOrder.getPrice()==15);
        assertEquals(true,retrievedOrder.getTotalQuantity()==10);
        assertEquals(true,retrievedOrder.getOrderId()==1L);

        testData=orderHandler.getBuyOrders();
        int sizeTwo = testData;

        assertEquals("Size is 0 for both buy and sell",true, sizeOne.size()==1 && sizeTwo==0 );
*/

    }

    /**
     * Below are the tests using the ExampleData provided and usign the pdf to match the expected results
     */
    @Test
    public void testWithExampleData1() {
        ExampleData.buildExampleOrderBookFromReadMe(orderHandler);
        double currentPrice= orderHandler.getCurrentPrice("MSFT", 6, Side.SELL);
        boolean result = ExampleData.thresholdBasedFloatsComparison(currentPrice, new Double(19.0));

        assertEquals("Price is as expected",true, result );
    }

    @Test
    public void testWithExampleData2() {
        ExampleData.buildExampleOrderBookFromReadMe(orderHandler);
        double currentPrice= orderHandler.getCurrentPrice("MSFT", 30, Side.SELL);
        boolean result = ExampleData.thresholdBasedFloatsComparison(currentPrice, new Double(20.233));

        assertEquals("Price is as expected",true, result );

    }


    @Test
    public void testWithExampleData3() {
        ExampleData.buildExampleOrderBookFromReadMe(orderHandler);
        double currentPrice= orderHandler.getCurrentPrice("MSFT", 17, Side.SELL);
        boolean result = ExampleData.thresholdBasedFloatsComparison(currentPrice, new Double(19.588));

        assertEquals("Price is as expected",true, result );

    }

    @Test
    public void testWithExampleData4() {
        ExampleData.buildExampleOrderBookFromReadMe(orderHandler);
        double currentPrice= orderHandler.getCurrentPrice("MSFT", 10, Side.BUY);
        boolean result = ExampleData.thresholdBasedFloatsComparison(currentPrice, new Double(15));

        assertEquals("Price is as expected",true, result );

    }

    @Test
    public void testWithExampleDataRemoveAllDataAndGetPrice() {
        ExampleData.buildExampleOrderBookFromReadMe(orderHandler);

        orderHandler.removeOrder(1L);
        orderHandler.removeOrder(2L);
        orderHandler.removeOrder(3L);
        orderHandler.removeOrder(4L);
        orderHandler.removeOrder(5L);
        orderHandler.removeOrder(6L);
        orderHandler.removeOrder(7L);
        orderHandler.removeOrder(8L);
        orderHandler.removeOrder(9L);

        double currentPrice= orderHandler.getCurrentPrice("MSFT", 10, Side.BUY);

        assertEquals("Price is as expected",0, Double.compare(currentPrice,0) );

    }


    /**
     * Handy function to compare doubles using a threshold as cant use == for Double values
     * @param f1
     * @param f2
     * @return
     */
    private static boolean thresholdBasedFloatsComparison(double f1, double f2)
    {
        final double THRESHOLD = .001;
        boolean numbersAreEqual=false;

        if (Math.abs(f1 - f2) < THRESHOLD) {
            numbersAreEqual = true;
        }

        return numbersAreEqual;
    }

    /**
     * Submits a series of orders for MSFT. The resulting Order Book for MSFT
     * is the one shown in Table 1 of the ReadMe document.
     * */
    public static void buildExampleOrderBookFromReadMe(OrderHandler orderHandler) {

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
    }

}

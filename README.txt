Ashish Patel.

Thanks for going through this code and tests.
Id really appreciate any comments you may have on my code and design.

I used Oracle jdk 1.8 for this work.

Ive created 2 implementations:
OrderHandlerImpl is what is used by the test class ExampleData.

OrderHandlerImpl2 uses locks and keeps a running total of the symbol+price. Maintains all the other structures.This wont
deadlock as the lock is always fetched in the same order.
Pls run the main method here to see the prices from the example data.


Assumptions:
1) Code may be run multithreaded so Ive used data structures accordingly.
2) Most of the work is adding Orders for the same price so I've built the structure accordingly i.e
map(symbol->price->...)
rather then map(orderid->symbol..) or map(symbol->orderid->...)
From the pdf, it says 5-500 price levels for an symbol so this is what I chose this structure.
Modifications are around 4 before a symbol is removed.
It is common for symbols to be created during the day so I've designed this for the most performant part i.e
I do not do sorting of the price levels here. I don't use a TreeMap to store the detailed Orders etc.
There is nothing mentioned about the frequency of getCurrentPrice.

3) I do NOT keep the orders sorted on insert as this will keep sorts going on all the time for each insert
and thus keep this thread running a long time and add latency to the Insert path.
Also I don't know the frequency of getCurrentPrice calls.
If needed, I can keep an accumulator running of the orders as well.

4) I only sort when fetching the price ( getCurrentPrice). This would ideally be running in a
separate thread so will not bother the insert thread.
I also do the summing in parallel using Streams - I know I don't need parallel for 4 entries but just to show it
can be done.
5) Ive added tests for both buy and sell side, null test, boundary cases and happy path tests.
 I can always add more given time but I think i've covered the major cases.
6) I do not use ReEntrantLock, etc as Im using the compute method which provides atomic updates.
I could do this for getCurrentPrice as the data can change while im looping through the current orders.
I can use locking here OR make a copy of the orders Im processing but as there was no requirement, I didn't do so.

7) As for GC, I do not use Strings as this is a major cause of collecting garbage even when its interned.
Instead I use hashes for the String (or can use CharSequence) and ChronicleMap which can be used to store the data Offheap so no gc for this.
8) I haven't tested it multithreaded but ive designed for it.
9) I implemented lock striping by keeping the buy and sell orders in separate maps as well as tracking the
order id->symbol.
this gives a fast way to check if the order exists rather then traversing all the symbols and checking for the order.
I use use the orderid->symbol map in the getCurrentPrice method to go to the Symbol orders directly rather then iterating.
10) In real operation id put an lmax-disruptor in front to feed the adding of orders and another thread keeping a summary of price levels per symbol.
11) The code will run better in a single thread if low latency is needed rather then multi threaded.
It would be easy to pin the process to one cpu and handle a subset of symbols in each disruptor feeding one process for inserting etc.
12) I used my own class AccumulatedOrder as i was going to keep a constant running summary of orders but this is to show that it
can be done like that. Note: I do not store the symbol in here as it's already the key to the map thus reducing storage.
13) I can add a symbolHash -> symbol map but as this was not needed so I didn't so so i.e we only use getPrice and not a different
function to DISPLAY the prices for the symbol but this is an easy addition.
14) Id use SBE ( simple binary exchange) rather then FIX for the Orders.

Thank you.
I really enjoyed this test.
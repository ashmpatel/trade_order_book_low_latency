There are 3 implementations:
1) OrderHandlerImpl is the naive, basic implementation. No locking and no runnign total of Symbol+ price is kept.
2) OrderHandlerImpl2 - uses low latency maps and concurrency to keep the locking to a minimum.
3) OrderHandlerImpl3 - uses the jdk Concurrency maps to do the same work i.e no dependence on low latency maps.

If you have any suggestions to improve the lockign performance, pls raise a pull request.

Thanks,
Ash.
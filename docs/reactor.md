# Threading and Schedulers

* How to subscribe to publisher already on different scheduler?
    * Running on self-created thread means needing to take care of it
    * Would be better to have Reactor take care of it itself
* More info on how schedulers communicate
    * Bounded queues
    * Where backpressure happens?
    * How to affect backpressure?
    * On what thread actions happen?
        * Actions of publishers
        * Actions of subscribers
* Aspects mentioned in description of
    * doOn*
    * on*
    * other types
* Types of actions
    * Types
        * on*
        * doOn*
        * other types
    * Aspects
        * (mentioned in description types of methods)
* Assembly time vs execution time
    * When running subscription on separate thread mono is assembled in main?
        * Does it depend on where flux is instantiated? Outside or inside of thread?
        * How to check what happens in assembly?
* Where operators work?
    * Most continue working in the thread on which previous operator executed
    * What happens in other than 'most'?
    * Fusion affects this - fused operators for sure work on the same thread
* What happens to Scheduler if action in operator throws exception?
    * does it recreate new worker thread?
* subscribeOn vs publishOn

# Caveats to remember from docs

* Turning blocking into non-blocking with fromCallable, use boundedElastic for this
* Not every scheduler fits everything
* 


# Investigation

What is the difference between successful and failed reconnect? 

date: 2021-09-01

last successful reconnect: 23:42:02.892
last message heard:        23:42:07.033
first failed reconnect:    23:42:12.575

Hypothesis:
1
wrong communication between threads, 
order of messages not preserved, that's why state is wrong; 
internal reactive state apparently since
2
fromCallable not fired for some reason
publishing of blocking wrappers for callable
perhaps all should happen on the same thread? so "new single"
but JMS callbacks are on separate pool still - can be avoided, lowered?

3 
too many context factories created, or contexts - wrong caching of these
plenty of threads visible in JVM

4
wrong handling of void methods, should be in fromCallable 
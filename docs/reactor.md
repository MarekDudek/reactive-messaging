
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
  * What happens in other then 'most'?
  * Fussion affects this - fused operators for sure work on the same thread
* What happens to Scheduler if action in operator throws exception?
  * does it recreate new worker thread?
*********************************
* Adaptive Schedulers for Storm *
*********************************

General Informations
--------------------
For detailed info about the software, refer to
Leonardo Aniello, Roberto Baldoni, Leonardo Querzoni, "Adaptive Online Scheduling in Storm", 7th ACM International Conference on Distributed Event-Based Systems (DEBS 2013)
http://www.dis.uniroma1.it/~midlab/articoli/ABQ13storm.pdf

Licence
-------
This software is released under the Eclipse Public License - v 1.0 (see LICENSE file).

Content
-------
The directory src includes all the java sources required for the compilation.

Requirements
------------
Libraries required for the compilation:
- Storm 0.8.1 libraries
- Commons DBCP 1.4
- Commons Pool 1.5.6

Installation
------------
1. Copy the jar resulting from the compiltion and the jars of Commons DBCP 1.4 and Commons Pool 1.5.6 into the lib directory of storm installation.
2. Update storm.yaml for nimbus service in order to use one of these schedulers.
  - For the offline scheduler, add the following line storm.scheduler: "midlab.storm.scheduler.OfflineScheduler"
  - For the online scheduler, add the following line storm.scheduler: "midlab.storm.scheduler.OnlineScheduler"

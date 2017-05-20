# CS171 Final Project
# Geon Lee
# Ethan Wang
# Spring 2017

<pre>
Note: All call are lower case except filenames

CLI call format:
<PRM>
ip exit
ip stop
ip resume
ip replicate [filename]
ip merge [pos1] [pos2]
ip total [pos1 pos2 ..]
ip print

PRM call format:
<PRM>
ip prm [object]
ip prepare
ip ack
ip accept 

Port Numbers:
5001 - CLI
5002 - Map1
5003 - Map2
5004 - Reduce
5005 - PRM
(same IP)

Setup files:
prm: list of ip/port of other PRM
cli: unnecessary, assuming we already know the ports
</pre>

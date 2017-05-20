# CS171 Final Project
# Geon Lee
# Ethan Wang
# Spring 2017

Note: All call are lower case except filenames

CLI call format:
<PRM>
exit
stop
resume
replicate [filename]
merge [pos1] [pos2]
total [pos1 pos2 ..]
print

PRM call format:
<PRM>
prm [object]
propose
ack
accept 

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

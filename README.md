CS171 Final Project S17
==============
### Geon Lee
### Ethan Wang

TODO
==============
* mappers (2)
* reducer (1)
* PRM: fails if you don't start all ~3 PRM's within 5 seconds
* PRM: fails if you use large positions for merge [pos1] [pos2]

Azure Virtual Machines
==============
### ssh [login] --- [ip address]:
* ssh   ethangeon@52.161.104.183  ---  	10.0.1.4
* ssh   ethangeon@52.161.108.70   ---  10.0.1.5
* ssh   ethangeon@52.161.109.240  --- 10.0.1.6
### Port Numbers:
* 5001 - CLI
* 5002 - Map1
* 5003 - Map2
* 5004 - Reduce
* 5005 - PRM

Setup Files
==============
* prm: list of ip addrs of PRM
* cli: none

Commands
==============
### CLI commands:
* exit
* stop
* resume
* replicate [filename]
* merge [pos1] [pos2]
* total [pos1 pos2 ..]
* print
### PRM internal calls:
* prepare
* ack
* accept

Other Information
==============
### Notes:
* all commands/calls use lower-case except filenames
### Azure Links:
* http://portal.azure.com
* https://docs.microsoft.com/en-us/azure/virtual-machines/linux/quick-create-portal
### Getting IP Addr:
* ip addr show
* check eth0: inet
### Getting ssh-key:
* cat ~/.ssh/id_rsa.pub

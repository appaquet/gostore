Description
================================================================================
GoStore is a distributed system framework build in [Go](http://www.golang.org/). It was originally created
as a core for distributed file system that I did for my software engineering degree project. Since the 
communication core was generic enough, I decided to give it a try and create other services with it. 
Right now, the system has a very partial distributed file system service, an embryo of a cluster 
management service and an embryo of an in-memory & persistent database.

GoStore uses a custom made RPCs communication core that uses a custom binary protocol over 
UDP and TCP. Services can be binded on this communication core to remotely call procedures on 
other nodes of the cluster. Calls are async by default, but can be sync by adding callbacks to RPC calls. 
Depending on the size of calls payload, messages are sent to other nodes over UDP (timeouts/retries 
can me binded to calls) or TCP.

Disclaimer
================================================================================
This is a toy project. It's FAR from being production ready and it's really not polished. You have to 
understand that it has been created for an engineering degree project and conception/analysis/technical
report was the main priority of my project (as asked by my school). There are a lot of refactoring to be 
done, specially in the cluster component and in the communication component. The file system needs
a full refactoring in order to separate headers management from data management to simplify 
replication and failures management.

Main components
================================================================================

pkg/comm
--------
Services communication core. Services are binded to this component and uses it to send
async or sync RPCs to other nodes of the cluster. Listens on both UDP and TCP ports for incoming calls
redirected to binded services.

pkg/cluster
-----------
Keeps the current state of the cluster and its nodes. Used to resolve nodes in rings using
consistent hashing.

pkg/sevices/fs
---------------
A partially implemented distributed file system. Supports reads, writes, deletes, exists,
directory create, directory children list and file header retrieval. The file system can be called using a
REST API. The file system eventually replicate files, but doesn't have any node failure recovery support.

pkg/services/cls
-----------------
An embryo of a cluster management service. This service will make sure every node
knows the state of all other nodes in the cluster using one master that will gossip nodes events.

pkg/services/db
----------------
An embryo of an in-memory, but persistent distributed system. This will be where file
headers of the file system service will be stored. 

tests/fs
---------
Integration tests for the file system service. Not all tests are passing since they rely on timing 
(timeout tests). 


Author
======================================================================================
Andre-Philippe Paquet (http://www.appaquet.com/, Email: me @ my domain dot com)


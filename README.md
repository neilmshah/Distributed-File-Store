# file-transfer-app

This Repository contains code to set up a distributed file sharing system amongst clusters. RAFT algorithm has been used as consensus algorithm. The cluster will have following capabilities :
   
    1. To store and pass messages between clusters in distributed fashion.
    2. Ability to fetch data even from private network in the cluster
    3. To build a fault-tolerant and robust system.
    4. In case the requested data is unavailable, system should be able to forward request to other nodes.
    5. System should be able to handle large files and  be able to process it fast

### Database Design

  We used in-memory cocurrent hashmap to store the raw data (bytes) with Key as <File_Name>#<ChunkID>#<SequenceID> and value is the          FileMetaData contantaining bytes[] and seqMax

### Architecture Diagram

* [Link to Architecture Diagram](https://github.com/manogna-mujje/file-transfer-app/blob/master/arc.png)
<img src="https://github.com/manogna-mujje/file-transfer-app/blob/master/arc.png" />

## Start up steps.

    1. Need a Network switch to connect to the network.
    2. Set up the config.jsoo as your your IP's
    3. Start RaftServer.java class with program arguments as your IP in config.json
    4. Start ProxyServer.java
    5. Start DBServer.java
    5. Run Client.java


## Team Members:
* Manogna Mujje
* Sricheta RUj
* Vishnu Narayana
* Anav Sharma

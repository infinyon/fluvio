* Set up

    - Start SC Server
    - Create 2 SPU CRD (5001,5002).
    - SPU should not be running.


* Topic Creation
  - No SPU should be running.
  - Create Topic1 with Partition = 1, Replication = 2.
  - Topic should have replica map with 2 replicas.
    - Resolution should be Ok
  - 1 Partition should be created.
    - Should have Spec wiht leader and 2 replicas.
    - Status 
      - should be Offline
      - should have Leader with offset of -1
      - No replicas

* Turn on leader SPU.  For partition
  - Status's resolution should be online
  - Status's Leader should have offsets with 0
  - Status's Replica should have replica id with offsets of -1

* Turn on follower SPU.  For partition
  - Statu's Replica should have offsets of 0


* Follower failure
  - Turn off follower SPU
  - Status's Replica's status should stay Online
  - Turn on follower SPU

* Leader Election
  - Turn off Leader SPU
  - Spec should be changed to follower SPU
  - Statu's leader should be changed
  - Statu's Replica should have value of leader

* Follower up:
  - Parition with 2 replicas. 
  - Shutdown all SPU. This should set partition status to ElectionNoLeaderFounded
  - Bring up follower SPU.
  - It should do election (doesn't)

# Issues
[ ] Follower doesn't catch up
    - 2 SPU.
    - Create a record.  2 SPU should have same replicas
    - Shutdown follower.  Create record.  Leader's EOF and HW should be greater than follower.
    - Bring back follower.  Follower should sync with Leader



# Terms

Source - Source cluster where the data is being mirrored from
Target - Target cluster where the data is being mirrored to

# Network connectivity

There is a single multiple connection between the Source and Target SPU.  This connection is used for all mirroring requests.  The connection is initiated by the Source SPU.

# Sequence

1. Source SPU connect to to the Target cluster on multiple connection.
   1. Target SPU will be leader of the replica.
2. Source SPU send a request to the Target SPU with the following information to sync mirror:
    - Replica id
    - HW, LEO
3. Target SPU will respond it's current HW, LEO for replica.
4. Source SPU will send largest possible record sets to the Target SPU given target's HW, LEO.
5. Target SPU will write to the local replica and update it's LEO.  Once it is done. It will send a response to the Source SPU with the following information:
    - Replica id
    - HW, LEO
6. Repeat step 4 and 5 until the Target SPU's LEO is equal to the Source SPU's LEO.
7. If there are any errors or network is disconnected, then step 1 will be repeated.


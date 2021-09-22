# SPU Test Scenario

## A Health Check

SPU = 1

### A Simple Health Check
* Bring up SC
* Bring up SPU
* Create custom SPU 5001
* SPU status should be online
* Bring down SPU
* SPU status should be offline

### SC crash
* Bring up SC and SPU
* SPU status should be up
* Kill SC first then SPU
* SPU status should still be up since SC didn't have change to receive status from SPU
* Bring back up SC
* SPU status should be change to offline after health check resync period

## Bring up 2 SPU

* Bring up SC and SPU
* Create 2 custom SPU
* 2nd SPU status is initially empty
* After while, 2nd SPU's status should be offline since we didn't bring up 2nd SPU yet

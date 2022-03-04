package main

import (
	"pushschedule/src/config"
	"pushschedule/src/part"
	"strconv"
	"time"
)

func main() {	
	scheduling_dtime, _ := strconv.Atoi(config.Get("SCHEDULING_CHECK_DELAY"))
	for {
		go part.CheckScheduledPushData()
		go part.CheckRetargetQueueData()
		go part.CheckPushAutoData()
		time.Sleep(time.Minute * time.Duration(scheduling_dtime))
	}
}

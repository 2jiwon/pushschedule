package main

import (
	"pushschedule/src/config"
	scheduled "pushschedule/src/part"
	"strconv"
	"time"
)

func main() {
	scheduling_dtime, _ := strconv.Atoi(config.Get("SCHEDULING_CHECK_DELAY"))
	for {
		go scheduled.CheckScheduledPushData()
		time.Sleep(time.Minute * time.Duration(scheduling_dtime))
	}
}

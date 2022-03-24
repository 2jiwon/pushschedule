package main

import (
	"pushschedule/src/common"
	"pushschedule/src/config"
	"pushschedule/src/helper"
	"pushschedule/src/part"
	"strconv"
	"time"
)

func main() {
	defer func() {
		if v := recover(); v != nil {
			helper.Log("ERROR", "MAIN ERROR", "")
			common.SendJandiMsg("스케쥴링 푸시 > 메인 에러 발생", "스케쥴링 푸시 > 메인 에러 발생")
		}
	}()
	scheduling_dtime, _ := strconv.Atoi(config.Get("SCHEDULING_CHECK_DELAY"))
	for {
		go part.CheckScheduledPushData()
		go part.CheckRetargetQueueData()
		go part.CheckPushAutoData()
		time.Sleep(time.Minute * time.Duration(scheduling_dtime))
	}
}

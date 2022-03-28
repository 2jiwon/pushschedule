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
		go part.CheckScheduledPushData() //스케쥴링 푸쉬 데이터 체크
		go part.CheckRetargetQueueData() //IOS 리타겟 큐 푸쉬 데이터 체크
		go part.CheckPushAutoData()      // 자동화 푸시 데이터 체크
		time.Sleep(time.Minute * time.Duration(scheduling_dtime))
	}
}

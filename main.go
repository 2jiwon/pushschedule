package main

import (
	"fmt"
	"pushschedule/src/config"
	"pushschedule/src/helper"
	"pushschedule/src/mysql"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func main() {
	scheduling_dtime, _ := strconv.Atoi(config.Get("SCHEDULING_CHECK_DELAY"))
	for {
		go CheckScheduledPushData()
		time.Sleep(time.Minute * time.Duration(scheduling_dtime))
	}
}

/*
 * 스케쥴링 푸쉬 데이터 체크
 */
func CheckScheduledPushData() {
	fmt.Println("체크 시작")
	// KST로 timezone 설정
	now := time.Now()
	now = now.In(time.FixedZone("KST", 9*60*60))

	// 현재 홀수 주 인지 짝수 주인지 체크
	_, week := now.ISOWeek()
	evenWeek := helper.IsEvenWeek(week)
	// 요일 체크
	weekDay := int(now.Weekday())

	// hhmm 형식으로 현재 시간 변환 (단, 기준 시간은 5분 후로 설정)
	unix_now := now.Unix()
	from_now := now.Add(time.Minute * 5)
	now_timestamp := from_now.Unix()
	hours, minutes, _ := from_now.Clock()
	currentTime := fmt.Sprintf("%d%02d", hours, minutes)

	// 메시지 데이터 집어넣을 테이블
	tb_msg := "push_msg_data"

	// 스케쥴 테이블에서 데이터 가져오기
	tb_schedule := "BYAPPS2015_push_schedule_data"
	sql := fmt.Sprintf("SELECT * FROM %s", tb_schedule)
	mrows, tRecord := mysql.Query(sql)
	if tRecord > 0 {
		for _, mrow := range mrows {
			if evenWeek == true { // 이번주가 짝수 주이면
				if mrow["weekly"] == "biweek" { // 가져온 값이 홀수 주 일때
					continue
				}
			} else { // 이번주가 홀수 주이면
				if mrow["weekly"] == "biweeks" { // 가져온 값이 짝수 주 일때
					continue
				}
			}

			daily := strings.Split(mrow["daily"], "|")
			isContinue := false
			for _, day := range daily { // 해당 날짜인지 체크
				if dayValue, err := strconv.Atoi(day); err == nil {
					if dayValue != weekDay {
						isContinue = true
						continue
					} else {
						isContinue = false
						break
					}
				}
			}
			if isContinue == true {
				continue
			}

			// 5분 후로 설정한 시간과 일치하는지 체크
			timely, _ := strconv.Atoi(mrow["timely"])
			currTime, _ := strconv.Atoi(currentTime)
			if timely != currTime {
				continue
			}

			// push_msg_data에 데이터 삽입
			f := map[string]interface{}{
				"app_id":        mrow["app_id"],
				"push_type":     "schedule",
				"msg_type":      mrow["msg_type"],
				"server_group":  mrow["server_group"],
				"app_lang":      mrow["app_lang"],
				"os":            helper.ConvOS(mrow["os"]),
				"title":         mrow["title"],
				"notice_title":  mrow["notice_title"],
				"msg":           mrow["msg"],
				"ios_msg":       mrow["ios_msg"],
				"attach_img":    mrow["attach_img"],
				"link_url":      mrow["link_url"],
				"gcm_color":     mrow["gcm_color"],
				"target_option": mrow["target_option"],
				"fcm":           mrow["fcm"],
				"schedule_time": strconv.FormatInt(now_timestamp, 10),
				"reg_time":      strconv.FormatInt(unix_now, 10),
			}
			res, res_idx := mysql.Insert(tb_msg, f, true)
			if res < 1 {
				helper.Log("error", "scheduled_push.CheckScheduledPushData", fmt.Sprintf("메시지 데이터 삽입 실패-%s", mrow))
			} else {
				// push_msg_sends_ 에 데이터 삽입
				InsertPushMSGSendsData(res_idx, mrow["app_id"])
			}
		}
	}
}

// 앱 아이디 기준으로 테이블 이름 가져오기
func GetTable(tb_name string, app_id string) string {
	// a-z가 아닐 경우
	test, _ := regexp.MatchString("^[a-z]", app_id)
	if test == false {
		tb_name += "0"
	} else {
		tb_name += string(app_id[0])
	}
	return tb_name
}

// 메시지 전송 데이터 삽입하기
func InsertPushMSGSendsData(push_idx int, app_id string) {
	users_table := GetTable("push_users_", app_id)
	msg_table := GetTable("push_msg_sends_", app_id)

	sql := fmt.Sprintf("SELECT * FROM %s WHERE app_id = '%s'", users_table, app_id)
	mrows, tRecord := mysql.Query(sql)
	if tRecord > 0 {
		for _, mrow := range mrows {
			data := map[string]interface{}{
				"push_idx":   push_idx,
				"app_id":     mrow["app_id"],
				"app_udid":   mrow["app_udid"],
				"mem_id":     mrow["app_shop_id"],
				"shop_no":    mrow["app_shop_no"],
				"push_token": mrow["device_id"],
				"app_os":     helper.ConvOS(mrow["app_os"]),
			}
			res, _ := mysql.Insert(msg_table, data, false)
			if res < 1 {
				helper.Log("error", "scheduled_push.InsertPushMSGSendsData", fmt.Sprintf("메시지 전송 데이터 삽입 실패-%s", mrow))
			}
		}
	}
}

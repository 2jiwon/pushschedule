package part

import (
	"fmt"
	"pushschedule/src/common"
	"pushschedule/src/config"
	"pushschedule/src/helper"
	"pushschedule/src/mysql"
	"strconv"
	"strings"
	"time"
)

/*
 * 스케쥴링 푸쉬 데이터 체크
 */
func CheckScheduledPushData() {
	defer func() {
		if v := recover(); v != nil {
			helper.Log("Error", "CheckScheduledPushData Error", "")
			common.SendJandiMsg("스케쥴링 푸시 > 스케쥴링 푸시 실행 에러", "스케쥴링 푸시 실행 에러 발생")
		}
	}()
	fmt.Println("체크 시작")

	now := time.Now()
	// KST로 timezone 설정
	now = now.In(time.FixedZone("KST", 9*60*60))
	// time parse를 위한 location 지정
	loc, _ := time.LoadLocation("Asia/Seoul")
	formatted_now := now.Format("1504")

	// 현재 홀수 주 인지 짝수 주인지 체크
	_, week := now.ISOWeek()
	evenWeek := helper.IsEvenWeek(week)
	// 요일 체크
	weekDay := int(now.Weekday())

	// hhmm 형식으로 현재 시간 변환 (기준 시간은 5분)
	schedule_time_limit := config.Get("SCHEDULING_LIMIT")
	mins, _ := time.ParseDuration(schedule_time_limit + "m")
	mins_limit := now.Add(-mins)
	formatted_mins := mins_limit.Format("1504")

	// 스케쥴링 테이블
	const tb_push_schedule_data = "BYAPPS2015_push_schedule_data"

	// 스케쥴 테이블에서 데이터 가져오기
	sql := fmt.Sprintf("SELECT * FROM %s", tb_push_schedule_data)
	mrows, tRecord := mysql.Query("master", sql)
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
			isContinue := true
			for _, day := range daily { // 해당 날짜인지 체크
				if dayValue, err := strconv.Atoi(day); err == nil {
					if dayValue == weekDay {
						isContinue = false
						break
					}
				}
			}
			if isContinue == true {
				continue
			}

			// 제한 시간 내의 데이터인지 체크
			if mrow["timely"] < formatted_mins || mrow["timely"] > formatted_now {
				continue
			}

			// 스케쥴타임 넣기 위한 포맷변경
			schedule_time_data := fmt.Sprintf("%s%s", now.Format("20060102"), mrow["timely"])
			schedule_time, _ := time.ParseInLocation("200601021504", schedule_time_data, loc)

			//schdule_time으로 발송 여부 체크
			sql = fmt.Sprintf("SELECT idx FROM %s WHERE schdule_time='%v' AND app_id='%s' AND send_group='%s'", common.TB_push_msg_data, schedule_time.Unix(), mrow["app_id"], mrow["idx"])
			idx, _ := strconv.Atoi(mysql.GetOne("master", sql))
			if idx == 0 {
				// push_msg_data에 데이터 삽입
				f := map[string]interface{}{
					"state":         "A",
					"app_id":        mrow["app_id"],
					"push_type":     "push",
					"msg_type":      mrow["msg_type"],
					"send_group":    mrow["idx"],
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
					"schedule_time": schedule_time.Unix(),
					"reg_time":      now.Unix(),
				}

				insert_res, res_idx := mysql.Insert("master", common.TB_push_msg_data, f, true)
				if insert_res < 1 {
					helper.Log("error", "scheduled.CheckScheduledPushData", fmt.Sprintf("push_msg_data Insert 실패-%s", mrow))
					common.SendJandiMsg("scheduled.CheckScheduledPushData", fmt.Sprintf("push_msg_data Insert 실패-%s", mrow["app_id"]))
				} else {
					// push_msg_sends_{} 에 데이터 삽입
					total_cnt, and_cnt, ios_cnt := common.InsertPushMSGSendsData(res_idx, mrow["app_id"])
					if total_cnt == 0 {
						helper.Log("error", "scheduled.CheckScheduledPushData", fmt.Sprintf("push_msg_sends_%s Insert된 내역이 없음", mrow["app_id"]))
					} else {
						// push_msg_data에 state와 발송수 업데이트
						d := map[string]interface{}{
							"state":    "R",
							"send_and": and_cnt,
							"send_ios": ios_cnt,
						}
						update_res := mysql.Update("master", common.TB_push_msg_data, d, "idx='"+strconv.Itoa(res_idx)+"'")
						if update_res < 1 {
							helper.Log("error", "scheduled.CheckScheduledPushData", fmt.Sprintf("push_msg_data Update 실패- idx : %d", res_idx))
						}
					}
				}
			}
		}
	} else {
		helper.Log("error", "scheduled.CheckScheduledPushData", fmt.Sprintf("%s에서 수집된 데이터가 없음", tb_push_schedule_data))
	}
}

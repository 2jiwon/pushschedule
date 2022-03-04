package part

import (
	"fmt"
	"pushschedule/src/helper"
	"pushschedule/src/mysql"
	"strconv"
	"time"
)

/*
 * 리타겟 큐 푸쉬 데이터 체크
 */
 func CheckRetargetQueueData() {
	fmt.Println("리타겟 큐 체크 시작")
	// KST로 timezone 설정
	now := time.Now()
	now = now.In(time.FixedZone("KST", 9*60*60))
	now_timestamp := now.Unix()

	// hhmm 형식으로 현재 시간 변환 (단, 기준 시간은 1분 후)
	one_mins_later := now.Add(time.Minute * 1)
	one_mins_later_timestamp := one_mins_later.Unix()
	formatted_min := fmt.Sprintf("%d%02d%02d%02d%02d", one_mins_later.Year(), one_mins_later.Month(), one_mins_later.Day(), one_mins_later.Hour(), one_mins_later.Minute())
	formatted_hour := fmt.Sprintf("%d%02d%02d%02d", one_mins_later.Year(), one_mins_later.Month(), one_mins_later.Day(), one_mins_later.Hour())
	fmt.Println(formatted_min)

	// 메시지 데이터 집어넣을 테이블
	push_msg_data_table := "push_msg_data"

	// 리타겟큐 테이블에서 데이터 가져오기
	retarget_queue_table := "BYAPPS_retarget_queue"	
	sql := fmt.Sprintf("SELECT * FROM %s WHERE schedule_time like '%s'", retarget_queue_table, formatted_hour + "%")
	mrows, tRecord := mysql.Query("ma", sql)
	if tRecord > 0 {
		for _, mrow := range mrows {
			fmt.Println("schedule_time: ", mrow["schedule_time"])
			// 예약된 시간과 현재로부터 1분 후 시간이 동일하면 push_msg_data에 데이터 삽입
			if mrow["schedule_time"] == formatted_min {	
				fmt.Println("yes")
				f := map[string]interface{}{
					"app_id":        mrow["app_id"],
					"push_type":     "retarget",
					"msg_type":      "retarget",
					"server_group":  helper.GetRandom(),
					"app_lang":      mrow["lang"],
					"os":            helper.ConvOS(mrow["app_os"]),
					"title":         mrow["product_name"],
					"notice_title":  "↓ 두 손가락으로 당겨주세요 ↓",
					"msg":           mrow["product_name"],
					"ios_msg":       mrow["product_name"],
					"attach_img":    mrow["img_url"],
					"link_url":      mrow["link_url"],
					"schedule_time": strconv.FormatInt(one_mins_later_timestamp, 10),
					"reg_time":      strconv.FormatInt(now_timestamp, 10),
				}
				res, res_idx := mysql.Insert("master", push_msg_data_table, f, true)
				if res < 1 {
					helper.Log("error", "retarget_queue.CheckScheduledPushData", fmt.Sprintf("메시지 데이터 삽입 실패-%s", mrow))
				} else {
					// push_msg_sends_ 에 데이터 삽입
					go InsertOnePushMSGSendsData(res_idx, mrow["app_id"], mrow["app_udid"])
				}
			}
		}
	}
}

// 메시지 전송 데이터 삽입하기
func InsertOnePushMSGSendsData(push_idx int, app_id string, app_udid string) {
	fmt.Println("insert 시작")
	push_users_table := helper.GetTable("push_users_", app_id)
	push_msg_table := helper.GetTable("push_msg_sends_", app_id)

	sql := fmt.Sprintf("SELECT * FROM %s WHERE app_id = '%s' AND app_udid = '%s'", push_users_table, app_id, app_udid)
	mrows, tRecord := mysql.Query("master", sql)
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
			res, _ := mysql.Insert("master", push_msg_table, data, false)
			if res < 1 {
				helper.Log("error", "retarget_queue.InsertOnePushMSGSendsData", fmt.Sprintf("메시지 전송 데이터 삽입 실패-%s", mrow))
			}
		}
	}
}
package part

import (
	"fmt"
	"pushschedule/src/common"
	"pushschedule/src/config"
	"pushschedule/src/helper"
	"pushschedule/src/mysql"
	"strconv"
	"time"
)

// 리타겟큐 테이블
const tb_retarget_queue = "BYAPPS_retarget_queue"

/*
 * 리타겟 큐 푸쉬 데이터 체크
 */
 func CheckRetargetQueueData() {
	fmt.Println("리타겟 큐 체크 시작")

	now := time.Now()
	// KST로 timezone 설정
	now = now.In(time.FixedZone("KST", 9*60*60))
	now_timestamp := now.Unix()
	formatted_now := fmt.Sprintf("%d%02d%02d%02d%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())

	// hhmm 형식으로 제한 시간 변환 (제한시간은 env에서 가져옴)
	retarget_queue_time_limit := config.Get("RETARGET_QUEUE_LIMIT")
	mins, _ := time.ParseDuration(retarget_queue_time_limit + "m")
	time_limit := now.Add(-mins)
	limit_timestamp := time_limit.Unix()
	formatted_min := fmt.Sprintf("%d%02d%02d%02d%02d", time_limit.Year(), time_limit.Month(), time_limit.Day(), time_limit.Hour(), time_limit.Minute())
	
	fmt.Println(formatted_min)

	// 리타겟큐 테이블에서 state가 R이고, 스케쥴타임이 제한시간 ~ 현재 사이인 데이터만 가져오기
	sql := fmt.Sprintf("SELECT * FROM %s WHERE schedule_time >= %v AND schedule_time <= %v AND state='%s'", tb_retarget_queue, formatted_min, formatted_now, "R")
	mrows, tRecord := mysql.Query("ma", sql)
	if tRecord > 0 {
		for _, mrow := range mrows {
			// idx 정보 먼저 저장
			target_idx, _ := strconv.Atoi(mrow["idx"])
			
			// 상품정보 가져오기
			product_info, err := common.GetProductFromByapps(mrow["app_id"], "custom", mrow["product_code"])
			fmt.Println("retarget_queue > 상품정보 > ", product_info)
			fmt.Println("retarget_queue > 에러 > ", err)

			if err == false {
				fmt.Println("schedule_time: ", mrow["schedule_time"])

				// 예약된 시간이 일치하면 push_msg_data에 데이터 삽입
				if mrow["schedule_time"] == formatted_min {
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
						"send_ios":      1,
						"schedule_time": strconv.FormatInt(limit_timestamp, 10),
						"reg_time":      strconv.FormatInt(now_timestamp, 10),
					}
					
					res, res_idx := mysql.Insert("master", common.TB_push_msg_data, f, true)
					if res < 1 {
						helper.Log("error", "retarget_queue.CheckRetargetQueueData", fmt.Sprintf("메시지 데이터 삽입 실패-%s", mrow))
					} else {
						// push_msg_sends_ 에 데이터 삽입
						go InsertOnePushMSGSendsData(res_idx, mrow["app_id"], mrow["app_udid"])

						// 대기열에서 데이터 제거
						DeleteRetargetQueueData(target_idx)
					}
				}
			} else {
				helper.Log("error", "retarget_queue.CheckRetargetQueueData", "상품 정보 취득 실패, 발송 실패 처리")
				// 상품정보를 못 가져왔으면 처리결과를 실패로 업데이트
				d := map[string]interface{}{
					"state" : "N",
				}
				update_queue := mysql.Update("ma", tb_retarget_queue, d, "idx='" + mrow["idx"] + "'")
				if update_queue < 1 {
					helper.Log("error", "retarget_queue.CheckRetargetQueueData", "retarget_queue > state 업데이트 실패")
				}
			}
		}
	}
}

// 대기열에서 데이터 제거
func DeleteRetargetQueueData(idx int) {
	fmt.Println("target_idx: ", idx)
	sql := fmt.Sprintf("DELETE FROM %s WHERE idx = '%d'", tb_retarget_queue, idx)
	_, record := mysql.Query("ma", sql)
	if record != 0 {
		helper.Log("error", "retarget_queue.DeleteRetargetQueueData", "retarget_queue 대기열 제거 실패")
		common.SendJandiMsg("retarget_queue.DeleteRetargetQueueData", "retarget_queue 대기열 제거 실패")
	}
}

// 메시지 전송 데이터 삽입하기
func InsertOnePushMSGSendsData(push_idx int, app_id string, app_udid string) {
	fmt.Println("insert 시작")
	tb_push_users := helper.GetTable("push_users_", app_id)
	tb_push_msg := helper.GetTable("push_msg_sends_", app_id)

	sql := fmt.Sprintf("SELECT * FROM %s WHERE app_id = '%s' AND app_udid = '%s'", tb_push_users, app_id, app_udid)
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
			
			res, _ := mysql.Insert("master", tb_push_msg, data, false)
			if res < 1 {
				helper.Log("error", "retarget_queue.InsertOnePushMSGSendsData", fmt.Sprintf("메시지 전송 데이터 삽입 실패-%s", mrow))
			}
		}
	}
}
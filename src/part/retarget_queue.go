package part

import (
	"fmt"
	"pushschedule/src/common"
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

	// 리타겟큐 테이블에서 state가 NULL인 데이터만 가져오기
	retarget_queue_table := "BYAPPS_retarget_queue"	
	sql := fmt.Sprintf("SELECT * FROM %s WHERE schedule_time like '%s' AND state IS NULL", retarget_queue_table, formatted_hour + "%")
	mrows, tRecord := mysql.Query("ma", sql)
	if tRecord > 0 {
		for _, mrow := range mrows {
			// idx 정보 먼저 저장
			target_idx, _ := strconv.Atoi(mrow["idx"])

			// 품절 체크
			cafe24_api_info := common.GetCafe24ApiInfo(mrow["app_id"])
			fmt.Println(cafe24_api_info)
			call_url := fmt.Sprintf("https://%s.cafe24api.com/api/v2/admin/products/%s", cafe24_api_info["mall_id"], mrow["product_code"])
			product_info, err := common.CallCafe24Api("GET", call_url, cafe24_api_info["access_token"])
			fmt.Println(product_info)
			if err != nil {
				helper.Log("error", "retarget_queue.CheckRetargetQueueData", "상품 정보 취득 실패")
			} else {
				product_data := product_info["product"].(map[string]interface{})
				if product_data["price"] == "0" || product_data["display"] == "F" || product_data["selling"] == "F" {
					helper.Log("error", "retarget_queue.CheckRetargetQueueData", "상품 상태 품절")
					// 품절이면 대기열에서 데이터 제거
					DeleteRetargetQueueData(target_idx)
					continue
				}
			}

			// 예약된 시간과 현재로부터 1분 후 시간이 동일하면 push_msg_data에 데이터 삽입
			fmt.Println("schedule_time: ", mrow["schedule_time"])
			if mrow["schedule_time"] <= formatted_min {
				d := map[string]interface{}{
					"state" : "R",
				}
				res_rq := mysql.Update("ma", retarget_queue_table, d, "idx='" + mrow["idx"] + "'")
				if res_rq < 1 {
					helper.Log("error", "retarget_queue.CheckRetargetQueueData", "retarget_queue state 업데이트 실패")
				} else {
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
						helper.Log("error", "retarget_queue.CheckRetargetQueueData", fmt.Sprintf("메시지 데이터 삽입 실패-%s", mrow))
					} else {
						// push_msg_sends_ 에 데이터 삽입
						go InsertOnePushMSGSendsData(res_idx, mrow["app_id"], mrow["app_udid"])

						// 대기열에서 데이터 제거
						DeleteRetargetQueueData(target_idx)
					}
				}
			}
		}
	}
}

// 대기열에서 데이터 제거
func DeleteRetargetQueueData(idx int) {
	fmt.Println("target_idx: ", idx)
	sql := fmt.Sprintf("DELETE FROM BYAPPS_retarget_queue WHERE idx = '%d'", idx)
	_, record := mysql.Query("ma", sql)
	if record != 0 {
		helper.Log("error", "retarget_queue.DeleteRetargetQueueData", "retarget_queue 대기열 제거 실패")
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
package part

import (
	"encoding/json"
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

// 리타겟팅 설정 정보(menu_type=9 일때 menu_content)가 있는 테이블
const tb_language_menu_data = "BYAPPS2015_language_menu_data"

/*
 * IOS 리타겟 큐 푸쉬 데이터 체크
 */
func CheckRetargetQueueData() {
	defer func() {
		if v := recover(); v != nil {
			helper.Log("Error", "CheckRetargetQueueData Error", "")
			common.SendJandiMsg("스케쥴링 푸시 > 리타겟큐 실행 에러", "스케쥴링 푸시 > 리타겟큐 실행 에러 발생")
		}
	}()

	//fmt.Println("리타겟 큐 체크 시작")

	now := time.Now()
	// KST로 timezone 지정
	now = now.In(time.FixedZone("KST", 9*60*60))
	// time parse를 위한 location 지정
	loc, _ := time.LoadLocation("Asia/Seoul")
	now_timestamp := now.Unix()
	formatted_now := now.Format("200601021504")

	// hhmm 형식으로 제한 시간 변환 (제한시간은 env에서 가져옴)
	retarget_queue_time_limit := config.Get("RETARGET_QUEUE_LIMIT")
	mins, _ := time.ParseDuration(retarget_queue_time_limit + "m")
	time_limit := now.Add(-mins)
	formatted_min := time_limit.Format("200601021504")

	// 리타겟큐 테이블에서 state가 R이고, 스케쥴타임이 제한시간~현재 사이인 데이터만 가져오기
	sql := fmt.Sprintf("SELECT * FROM %s WHERE schedule_time >= %v AND app_os='ios' AND schedule_time <= %v AND state='%s'", tb_retarget_queue, formatted_min, formatted_now, "R")
	mrows, tRecord := mysql.Query("ma", sql)
	if tRecord > 0 {
		for _, mrow := range mrows {
			// idx 정보 먼저 저장
			target_idx, _ := strconv.Atoi(mrow["idx"])

			// 앱서비스와 부가서비스가 유효한지 체크
			if common.IsAppValid(mrow["app_id"]) == true && common.IsServiceValid("ma", mrow["app_id"]) == true {
				// 스케쥴 타임을 넣기 위한 포맷 변환
				timeD := mrow["schedule_time"]
				schedule_time, _ := time.ParseInLocation("200601021504", timeD, loc)

				// 상품정보 가져오기
				_, chk := common.GetProductFromByapps(mrow["app_id"], "custom", mrow["product_code"])
				// 상품정보가 존재하면 
				if chk == true {
					// #USER# 변수 변환을 위해 회원 아이디 가져오기
					app_shop_id := "고객"
					sql = fmt.Sprintf("SELECT app_shop_id FROM %s WHERE app_udid='%s' ORDER BY idx DESC", common.GetTable("push_users_", mrow["app_id"]), mrow["app_udid"])
					srow, sRecord := mysql.GetRow("master", sql)
					if sRecord > 0 {
						app_shop_id = srow["app_shop_id"]
					}

					// 기본 메시지를 상품명으로 담고 시작
					msg := mrow["product_name"]
					ios_msg := mrow["product_name"]

					// language_menu_data에서 리타켓 데이터 가져오기
					sql = fmt.Sprintf("SELECT menu_content FROM %s WHERE app_id='%s' AND menu_type='9'", tb_language_menu_data, mrow["app_id"])
					vrow, vRecord := mysql.GetRow("master", sql)
					if vRecord > 0 {
						content := make(map[string]interface{})
						json.Unmarshal([]byte(vrow["menu_content"]), &content)
						// 변수 변환을 위한 데이터
						data := map[string]string{
							"USER":    app_shop_id,
							"PRODUCT": mrow["product_name"],
						}
						// 메시지 내에 #USER#, #PRODUCT# 변수 변환
						msg = common.ConvertProductInfo(fmt.Sprintf("%v", content["msg"+mrow["send_no"]]), data)
						ios_msg = common.ConvertProductInfo(fmt.Sprintf("%v", content["ios_msg"+mrow["send_no"]]), data)

						// push_msg_data에 데이터 삽입
						f := map[string]interface{}{
							"state":         "A",
							"app_id":        mrow["app_id"],
							"push_type":     "retarget",
							"msg_type":      "retarget",
							"send_group":    mrow["idx"],
							"app_lang":      mrow["lang"],
							"os":            helper.ConvOS(mrow["app_os"]),
							"title":         mrow["product_name"],
							"notice_title":  "↓ 두 손가락으로 당겨주세요 ↓",
							"msg":           msg,
							"ios_msg":       ios_msg,
							"attach_img":    mrow["img_url"],
							"link_url":      mrow["link_url"],
							"send_ios":      1,
							"schedule_time": schedule_time.Unix(),
							"reg_time":      now_timestamp,
						}
						insert_res, res_idx := mysql.Insert("master", common.TB_push_msg_data, f, true)
						if insert_res < 1 {
							helper.Log("error", "retarget_queue.CheckRetargetQueueData", fmt.Sprintf("메시지 데이터 삽입 실패-%s", mrow))
						} else {
							// push_msg_sends_ 에 데이터 삽입
							result := InsertOnePushMSGSendsData(res_idx, mrow["app_id"], mrow["app_udid"])
							if result == true {
								// push_msg_data에 state 업데이트
								d := map[string]interface{}{
									"state": "R",
								}
								update_res := mysql.Update("master", common.TB_push_msg_data, d, "idx='"+strconv.Itoa(res_idx)+"'")
								if update_res < 1 {
									helper.Log("error", "retarget_queue.CheckRetargetQueueData", "retarget_queue > state 업데이트 실패")
								}
							}
							// 대기열에서 데이터 제거
							DeleteRetargetQueueData(target_idx)
						}
					} else {
						helper.Log("error", "retarget_queue.CheckRetargetQueueData", "리타겟큐 메시지 셋팅 정보가 없음")
					}
				} else {
					helper.Log("error", "retarget_queue.CheckRetargetQueueData", "상품 정보 취득 실패, 발송 실패 처리")

					// 상품정보를 못 가져왔으면 처리결과를 실패로 업데이트
					d := map[string]interface{}{
						"state": "N",
					}
					update_queue := mysql.Update("ma", tb_retarget_queue, d, "idx='"+mrow["idx"]+"'")
					if update_queue < 1 {
						helper.Log("error", "retarget_queue.CheckRetargetQueueData", "retarget_queue > state 업데이트 실패")
					}

					// 1차나 2차를 발송 실패한 경우, 다음 회차가 있는지 체크해서 있으면, 2차 시간은 현 시간 + 5초후 발송처리 & 3차를 2차로 당김
					m_send_no, _ := strconv.Atoi(mrow["send_no"])
					if m_send_no < 3 {
						sql = fmt.Sprintf("SELECT * FROM %s WHERE app_udid='%s' AND send_no > %d AND state='%s' ORDER BY send_no ASC", tb_retarget_queue, mrow["app_udid"], m_send_no, "R")
						srows, sRecord := mysql.Query("ma", sql)
						if sRecord > 0 {
							old_schedule_time := ""
							new_schedule_time := now.Add(time.Second * 5).Format("200601021504")
							for _, srow := range srows {
								s_send_no, _ := strconv.Atoi(srow["send_no"])
								if m_send_no == 1 {
									if s_send_no == 2 {
										old_schedule_time = srow["schedule_time"]
									}
									if s_send_no == 3 {
										new_schedule_time = old_schedule_time
									}
								}

								// 스케쥴타임만 업데이트
								f := map[string]interface{}{
									"schedule_time": new_schedule_time,
								}
								update_queue = mysql.Update("ma", tb_retarget_queue, f, "idx='"+srow["idx"]+"'")
								if update_queue < 1 {
									helper.Log("error", "retarget_queue.CheckRetargetQueueData", "retarget_queue > schedule_time 업데이트 실패")
								}
							}
						}
					}
				}
			} else {
				// 서비스가 유효하지 않으면 에러 기록하고 대기열에서 제거
				helper.Log("error", "retarget_queue.CheckRetargetQueueData", fmt.Sprintf("%s - 앱서비스,부가서비스 상태가 유효하지 않음", mrow["app_id"]))
				DeleteRetargetQueueData(target_idx)
			}
		}
	}
}

// 대기열에서 데이터 제거
func DeleteRetargetQueueData(idx int) {
	sql := fmt.Sprintf("DELETE FROM %s WHERE idx = '%d'", tb_retarget_queue, idx)
	_, record := mysql.Query("ma", sql)
	if record != 0 {
		helper.Log("error", "retarget_queue.DeleteRetargetQueueData", "retarget_queue 대기열 제거 실패")
		common.SendJandiMsg("retarget_queue.DeleteRetargetQueueData", "retarget_queue 대기열 제거 실패")
	}
}

/*
* 단일 메시지 전송 데이터 삽입하기
*
* @return 실행 결과 true, false
 */
func InsertOnePushMSGSendsData(push_idx int, app_id string, app_udid string) bool {
	//fmt.Println("insert 시작")
	tb_push_users := common.GetTable("push_users_", app_id)
	tb_push_msg := common.GetTable("push_msg_sends_", app_id)

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
				"reg_time":   time.Now().Unix(),
			}

			res, _ := mysql.Insert("master", tb_push_msg, data, false)
			if res < 1 {
				helper.Log("error", "retarget_queue.InsertOnePushMSGSendsData", fmt.Sprintf("메시지 전송 데이터 삽입 실패-%s", mrow))
				common.SendJandiMsg("retarget_queue.InsertOnePushMSGSendsData", fmt.Sprintf("%s 메시지 전송 데이터 삽입 실패-%d", app_id, push_idx))

				return false
			}
		}
	}
	return true
}

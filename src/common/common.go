package common

import (
	"fmt"
	"pushschedule/src/helper"
	"pushschedule/src/mysql"
)

// 메시지 전송 데이터 삽입하기
func InsertPushMSGSendsData(push_idx int, app_id string) {
	fmt.Println("insert 시작")
	push_users_table := helper.GetTable("push_users_", app_id)
	push_msg_table := helper.GetTable("push_msg_sends_", app_id)

	sql := fmt.Sprintf("SELECT * FROM %s WHERE app_id = '%s'", push_users_table, app_id)
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
				helper.Log("error", "common.InsertPushMSGSendsData", fmt.Sprintf("메시지 전송 데이터 삽입 실패-%s", mrow))
			}
		}
	}
}
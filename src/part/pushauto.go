package part

import (
	"fmt"
	"math/rand"
	"pushschedule/src/common"
	"pushschedule/src/config"
	"pushschedule/src/helper"
	"pushschedule/src/mysql"
	"strconv"
	"strings"
	"time"
)

/*
 * 자동화 푸시 데이터 체크
 */
 func CheckPushAutoData() {
	fmt.Println("자동화푸시 체크 시작")
	// KST로 timezone 설정
	now := time.Now()
	now = now.In(time.FixedZone("KST", 9*60*60))

	// 현재 홀수 주 인지 짝수 주인지 체크
	_, week := now.ISOWeek()
	evenWeek := helper.IsEvenWeek(week)
	// 요일 체크
	weekDay := int(now.Weekday())

	// hhmm 형식으로 현재 시간 변환 (기준 시간은 5분)
	now_timestamp := now.Unix()
	pushauto_time_limit := config.Get("PUSHAUTO_LIMIT")
	mins, _ := time.ParseDuration(pushauto_time_limit + "m")
	mins_limit := now.Add(-mins)
	mins_timestamp := mins_limit.Unix()
	hours, minutes, _ := mins_limit.Clock()
	currentTime := fmt.Sprintf("%d%02d", hours, minutes)

	// 자동화푸시 테이블
	const tb_push_auto_data = "BYAPPS2019_push_auto_data"

	// 자동화푸시 테이블에서 데이터 가져오기
	sql := fmt.Sprintf("SELECT * FROM %s WHERE action_on = 1", tb_push_auto_data)
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

			// 설정한 시간 내의 데이터인지 체크
			timely, _ := strconv.Atoi(mrow["timely"])
			currTime, _ := strconv.Atoi(currentTime)
			if timely != currTime {
				continue
			}

			// 상품 정보를 가져와서 만약 error가 true이면 다음으로 패스
			pdsInfo, err := GetProductData(mrow)
			if err == true {
				continue
			}

		    // 메시지에 변수 포함되어있으면 치환
			msg := ConvertProductInfo(mrow["msg"], pdsInfo.Name, strconv.Itoa(pdsInfo.Price))
			ios_msg := ConvertProductInfo(mrow["msg"], pdsInfo.Name, strconv.Itoa(pdsInfo.Price))

			// push_msg_data에 데이터 삽입
			f := map[string]interface{}{
				"app_id":        mrow["app_id"],
				"push_type":     "auto",
				"msg_type":      mrow["msg_type"],
				"server_group":  mrow["server_group"],
				"app_lang":      mrow["app_lang"],
				"os":            helper.ConvOS(mrow["os"]),
				"title":         mrow["title"],
				"notice_title":  mrow["notice_title"],
				"msg":           msg,
				"ios_msg":       ios_msg,
				"attach_img":    mrow["attach_img"],
				"link_url":      mrow["link_url"],
				"gcm_color":     mrow["gcm_color"],
				"target_option": mrow["target_option"],
				"fcm":           mrow["fcm"],
				"schedule_time": strconv.FormatInt(mins_timestamp, 10),
				"reg_time":      strconv.FormatInt(now_timestamp, 10),
			}
			if (mrow["app_os"] == "total") {
				f["send_and"] = 1
				f["send_ios"] = 1
			} else if (mrow["app_os"] == "android") {
				f["send_and"] = 1
			} else {
				f["send_ios"] = 1
			}

			res, res_idx := mysql.Insert("master", common.TB_push_msg_data, f, true)
			if res < 1 {
				helper.Log("error", "pushauto.CheckPushAutoData", fmt.Sprintf("메시지 데이터 삽입 실패-%s", mrow))
			} else {
				// push_msg_sends_ 에 데이터 삽입
				go common.InsertPushMSGSendsData(res_idx, mrow["app_id"])
			}
		}
	} else {
		helper.Log("error", "pushauto.CheckPushAutoData", "수집된 데이터가 없음")
	}
}

/*
* 상품 정보 가져오는 함수
*
* @param pushdata  
* 
* @return PDS 구조체, error 발생 여부
*/
func GetProductData(pushdata map[string]string) (common.PDS, bool) {
	data := common.PDS{}
	err := false
	if pushdata["action_type"] == "best" || pushdata["action_type"] == "product" {
		data, err = common.GetProductFromByapps(pushdata["app_id"], pushdata["action_type"], "")
	} else { // custom일때는 op=product로, 상품 code를 같이 API 호출해서 정보 가져오기
	    data, err = common.GetProductFromByapps(pushdata["app_id"], pushdata["action_type"], GetProductCode(pushdata))
	}

	if err == true {
		helper.Log("error", "pushauto.GetProductData", fmt.Sprintf("상품정보 가져오기 실패-%s", pushdata))
	}
	
	return data, err
}

/*
* #name, #price 치환
*
* @return string
*/
func ConvertProductInfo(msg string, name string, price string) string {
	if strings.Contains(msg, "#name#") {
		msg = strings.Replace(msg, "#name#", name, -1)
	}
	if strings.Contains(msg, "#price#") {
		msg = strings.Replace(msg, "#price#", price, -1)
	}
	return msg	
}

/*
* 수집할 상품 code 가져오기
*
* @return string
*/
func GetProductCode(data map[string]string) string{
    product_codes := strings.Split(data["products"], "|")
	seq, _ := strconv.Atoi(data["custom_seq"])
    if data["send_type"] == "queue" {		
        if len(product_codes) > seq + 1 {
            seq += 1
        } else {
            seq = 0
        }
		
        return product_codes[seq]
    } else if data["send_type"] == "random" {
        var i int
        seed := rand.NewSource(time.Now().UnixNano())
        random := rand.New(seed)
        for {
            i = random.Intn(len(product_codes))
            if i != seq {
                break;
            }
        }
        return product_codes[i]
    }
    return ""
}
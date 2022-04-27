package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"pushschedule/src/config"
	"pushschedule/src/helper"
	"pushschedule/src/mysql"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// 메시지 데이터 넣을 테이블
const TB_push_msg_data = "push_msg_data"

/*
* 앱 아이디 기준으로 테이블 이름 찾기
*
* @param
* 	string tb_name 기준 테이블 명
*   string app_id 앱 아이디
 */
func GetTable(tb_name string, app_id string) string {
	// a-z가 아닐 경우에는 0
	test, _ := regexp.MatchString("^[a-z]", app_id)
	if test == false {
		tb_name += "0"
	} else {
		tb_name += string(app_id[0])
	}
	return tb_name
}

/*
* 앱 상태와 서비스 기간이 유효한지 체크
*/
func IsAppValid(app_id string) bool {
	sql := fmt.Sprintf("SELECT app_process, end_time FROM BYAPPS_apps_data WHERE app_id='%s'", app_id)
	mrow, tRecord := mysql.GetRow("master", sql)
	now_timestamp := time.Now().Unix()
	
	if tRecord > 0 {
		app_process, _ := strconv.Atoi(mrow["app_process"])
		end_time, _ := strconv.ParseInt(mrow["end_time"], 10, 64)
		if app_process == 7 && end_time > now_timestamp {
			return true
		}
	}
	return false
}
/*
*  부가서비스 상태와 서비스 기간이 유효한지 체크
*/
func IsServiceValid(service string, app_id string) bool {
	time_column := ""
	id_column := ""
	tb_name := ""
	valid_process := 0
	switch service {
		case "ma":
			time_column = "end_time"
			id_column = "ma_id"
			tb_name = "BYAPPS_MA_data"
			valid_process = 3
		case "pushauto":
			time_column = "service_end"
			id_column = "app_id"
			tb_name = "BYAPPS_push_auto_data"
			valid_process = 2
	}
	
	sql := fmt.Sprintf("SELECT app_process, %s FROM %s WHERE %s='%s'", time_column, tb_name, id_column, app_id)
	mrow, tRecord := mysql.GetRow("master", sql)
	now_timestamp := time.Now().Unix()
	if tRecord > 0 {
		app_process, _ := strconv.Atoi(mrow["app_process"])
		end_time, _ := strconv.ParseInt(mrow[time_column], 10, 64)
		if app_process == valid_process && end_time > now_timestamp {
			return true
		}
	}
	return false
}

/*
* 잔디 웹훅 전송 함수
 */
func SendJandiMsg(desc string, msg string) {
	type ConnectInfo struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		ImageURL    string `json:"imageUrl,omitempty"`
	}
	type Payload struct {
		Body        string        `json:"body"`
		ConnectInfo []ConnectInfo `json:"connectInfo"`
	}

	cdata := []ConnectInfo{
		{
			Title:       "[PUSHSCHEDULE 알림]",
			Description: desc,
			ImageURL:    "",
		},
	}
	data := &Payload{
		Body:        msg,
		ConnectInfo: cdata,
	}
	payloadBytes, err := json.Marshal(data)
	body := bytes.NewReader(payloadBytes)

	URL := config.Get("JANDI_WEBHOOK_URL")
	req, err := http.NewRequest("POST", URL, body)
	req.Header.Set("Accept", "application/vnd.tosslab.jandi-v2+json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		helper.Log("error", "common.SendJandiMsg", "잔디 웹훅 전송 실패")
	}
	defer resp.Body.Close()
}

/*
* 개별 메시지 전송 데이터 삽입하기
*
* @return OS 갯수
*	 total, android, ios
 */
func InsertPushMSGSendsData(push_idx int, app_id string) (int, int, int) {
	//fmt.Println("insert 시작")
	tb_push_users := GetTable("push_users_", app_id)
	tb_push_msg := GetTable("push_msg_sends_", app_id)

	and_cnt := 0
	ios_cnt := 0

	sql := fmt.Sprintf("SELECT * FROM %s WHERE app_id = '%s'", tb_push_users, app_id)
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
				helper.Log("error", "common.InsertPushMSGSendsData", fmt.Sprintf("메시지 전송 데이터 삽입 실패 - %s", data))
			}

			if mrow["app_os"] == "android" {
				and_cnt++
			} else {
				ios_cnt++
			}
		}
	}

	return and_cnt + ios_cnt, and_cnt, ios_cnt
}

/*
* 상품정보를 받기 위한 구조체
 */
type ProductData struct {
	Result  int    `json:"result"`
	Message string `json:"message"`
	Pds     []PDS  `json:"pds"`
	Request struct {
		Op    string `json:"op"`
		AppID string `json:"app_id"`
		Code  string `json:"code"`
	} `json:"request"`
}

type PDS struct {
	AppID      string `json:"app_id"`
	State      string `json:"state"`
	Code       string    `json:"code,omitempty"`
	Name       string `json:"name"`
	Price      string    `json:"price,omitempty"`
	Thum       string `json:"thum"`
	Link       string `json:"link"`
	Linkm      string `json:"linkm"`
	Hits       string  `json:"hits,omitempty"`
	PdUtime    string  `json:"pd_utime,omitempty"`
	PdRtime    string  `json:"pd_rtime,omitempty"`
	UpdateTime string  `json:"update_time,omitempty"`
	Idx        string    `json:"idx,omitempty"`
}

/*
* API call 함수
*
* @param
* 	method : GET, POST
*   url
*   key
*
* @return
* 	PDS 구조체, error 발생 여부
 */
func CallByappsApi(method string, url string, key string) (ProductData, error) {
	request, err := http.NewRequest(method, url, nil)
	if err != nil {
		return ProductData{}, err
	}
	request.Header.Add("Authorization", key)
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return ProductData{}, err
	}
	defer response.Body.Close()

	responseBody, _ := ioutil.ReadAll(response.Body)
	var responseJson ProductData
	err = json.Unmarshal(responseBody, &responseJson)
	if err != nil {
		return ProductData{}, err
	}
	return responseJson, nil
}

/*
* API 통해서 상품 정보 가져오는 함수
*
* @param
*	app_id
* 	action_type : best, product, custom
*	code : 상품 코드
*
* @return
*	PDS 구조체, 상품 존재 여부
 */
func GetProductFromByapps(app_id string, action_type string, code string) (PDS, bool) {
	URL := ""
	if code == "" {
		URL = config.Get("PRODUCT_API") + "/index.php?op=new&app_id=" + app_id
	} else {
		URL = config.Get("PRODUCT_API") + "/index.php?op=product&app_id=" + app_id + "&code=" + code
	}

	pdata, err := CallByappsApi("GET", URL, config.Get("PRODUCT_KEY"))
	if err != nil {
		helper.Log("error", "common.GetProductFromByapps", fmt.Sprintf("BYAPPS API 서버 탐색 실패 - %s", err))
		return PDS{}, false
	}

	if pdata.Result == 0 {
		helper.Log("error", "common.GetProductFromByapps", fmt.Sprintf("상품정보 없음 %s", code))
		return PDS{}, false
	}

	// best는 hit가 가장 높은 걸로
	if action_type == "best" {
		best := PDS{}
		for _, val := range pdata.Pds {
			if val.Hits > best.Hits && val.State == "Y" {
				best = val
			}
		}
		fmt.Println("베스트", best)
		return best, true
		// product는 new에서 가장 최신으로
	} else if action_type == "product" {
		new := PDS{}
		for _, val := range pdata.Pds {
			if val.PdRtime > new.PdRtime && val.State == "Y" {
				new = val
			}
		}
		return new, true
		// custom(선택상품)일때는 code로 지정된 상품 정보
	} else {
		if len(pdata.Pds) > 0 {
			if pdata.Pds[0].State == "Y" {
				return pdata.Pds[0], true
			}
		}
		return PDS{}, false
	}
}

/*
* 수집할 상품 code 가져오기
*
* @return string
 */
func GetProductCode(data map[string]string) string {
	product_codes := strings.Split(data["products"], "|")
	seq, _ := strconv.Atoi(data["custom_seq"])
	if data["send_type"] == "queue" {
		if len(product_codes) > seq+1 {
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
				break
			}
		}
		return product_codes[i]
	}
	return ""
}

/*
* 상품 정보 가져오는 함수
*
* @param pushdata
*
* @return PDS 구조체, 상품존재여부
 */
func GetProductData(pushdata map[string]string) (PDS, bool) {
	data := PDS{}
	chk := false
	if pushdata["action_type"] == "best" || pushdata["action_type"] == "product" {
		data, chk = GetProductFromByapps(pushdata["app_id"], pushdata["action_type"], "")
	} else { // custom일때는 op=product로, 상품 code를 같이 API 호출해서 정보 가져오기
		data, chk = GetProductFromByapps(pushdata["app_id"], pushdata["action_type"], GetProductCode(pushdata))
	}

	if chk == false {
		helper.Log("error", "pushauto.GetProductData", fmt.Sprintf("상품정보 가져오기 실패-%s", pushdata))
	}

	return data, chk
}

/*
* #name, #price 치환
*
* @return string
 */
func ConvertProductInfo(msg string, data map[string]string) string {
	switch {
	case strings.Contains(msg, "#name#"):
		msg = strings.Replace(msg, "#name#", data["name"], -1)
		fallthrough
	case strings.Contains(msg, "#price#"):
		msg = strings.Replace(msg, "#price#", data["price"], -1)
		fallthrough
	case strings.Contains(msg, "#USER#"):
		msg = strings.Replace(msg, "#USER#", data["USER"], -1)
		fallthrough
	case strings.Contains(msg, "#PRODUCT#"):
		msg = strings.Replace(msg, "#PRODUCT#", data["PRODUCT"], -1)
		fallthrough
	default:
	}
	return msg
}

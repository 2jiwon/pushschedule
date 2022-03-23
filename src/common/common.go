package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"pushschedule/src/config"
	"pushschedule/src/helper"
	"pushschedule/src/mysql"
)

// 메시지 데이터 넣을 테이블
const TB_push_msg_data = "push_msg_data"

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
		Body         string        `json:"body"`
		ConnectInfo  []ConnectInfo `json:"connectInfo"`
	}

	cdata := []ConnectInfo{
		{
			Title : "스케쥴링 푸시",
			Description : desc,
			ImageURL : "",
		},
	}
	data := &Payload{
		Body : msg,
		ConnectInfo : cdata,
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

// 개별 메시지 전송 데이터 삽입하기
func InsertPushMSGSendsData(push_idx int, app_id string) {
	fmt.Println("insert 시작")
	tb_push_users := helper.GetTable("push_users_", app_id)
	tb_push_msg := helper.GetTable("push_msg_sends_", app_id)

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
			}
			
			res, _ := mysql.Insert("master", tb_push_msg, data, false)
			if res < 1 {
				helper.Log("error", "common.InsertPushMSGSendsData", fmt.Sprintf("메시지 전송 데이터 삽입 실패-%s", mrow))
			}
		}
	}
}

/*
* 상품정보를 받기 위한 구조체
*/
type ProductData struct {
	Result  int    `json:"result"`
	Message string `json:"message"`
	Pds []PDS `json:"pds"`
	Request struct {
		Op    string `json:"op"`
		AppID string `json:"app_id"`
		Code  string `json:"code"`
	} `json:"request"`
}

type PDS struct {
	AppID      string `json:"app_id"`
	State      string `json:"state"`
	Code       int    `json:"code,string,omitempty"`
	Name       string `json:"name"`
	Price      int    `json:"price,string,omitempty"`
	Thum       string `json:"thum"`
	Link       string `json:"link"`
	Linkm      string `json:"linkm"`
	Hits       int    `json:"hits,string,omitempty"`
	PdUtime    int    `json:"pd_utime,string,omitempty"`
	PdRtime    int    `json:"pd_rtime,string,omitempty"`
	UpdateTime int    `json:"update_time,string,omitempty"`
	Idx        int    `json:"idx,string,omitempty"`
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
*	PDS 구조체, error 발생 여부
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
		helper.Log("error", "common.GetProductFromByapps", "BYAPPS API 서버 탐색 실패")
        return PDS{}, true
    }
	fmt.Println("pdata: ", pdata)
    if pdata.Result == 0 {
		helper.Log("error", "common.GetProductFromByapps", fmt.Sprintf("상품정보 없음 %s", code))
		return PDS{}, true
	}

	// best는 hit가 가장 높은 걸로
	if action_type == "best" {
		best := pdata.Pds[0]
		for _, val := range pdata.Pds {
			if val.Hits > best.Hits && val.State == "Y" {
				best = val
			}
		}
		return best, false
	// product는 new에서 가장 최신으로
	} else if action_type == "product" {
		new := pdata.Pds[0]
		for _, val := range pdata.Pds {
			if val.PdRtime > new.PdRtime && val.State == "Y" {
				new = val
			}
		}
		return new, false
	// custom(선택상품)일때는 code로 지정된 상품 정보
	} else {
		if len(pdata.Pds) > 0 {
			return pdata.Pds[0], false
		}
		return PDS{}, true
	}
}

package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"pushschedule/src/config"
	"pushschedule/src/helper"
	"pushschedule/src/mysql"
	"strings"
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

// cafe24 token 정보 가져오기
func GetCafe24ApiInfo(app_id string) map[string]string {
	cafe24Api_table := "BYAPPS_cafe24_api_token"
	sql := fmt.Sprintf("SELECT * FROM %s WHERE app_id = '%s'", cafe24Api_table, app_id)
	mrow, tRecord := mysql.GetRow("master", sql)
	if tRecord > 0 {
		return mrow
	} else {
		helper.Log("error", "common.GetCafe24ApiInfo", fmt.Sprintf("카페24 API 정보 취득 실패-%s", mrow))
	}
	return map[string]string{}
}

// cafe24 api call
func CallCafe24Api(method string, url string, token string) (map[string]interface{}, error) {
	request, err := http.NewRequest(method, url, nil)
    if err != nil {
        return nil, err
    }
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
    client := &http.Client{}
    response, err := client.Do(request)
    if err != nil {
        return nil, err
    }
    defer response.Body.Close()

	responseBody, _ := ioutil.ReadAll(response.Body)
    var responseJson map[string]interface{}
    err = json.Unmarshal(responseBody, &responseJson)
    if err != nil {
        return nil, err
    }
    return responseJson, nil
}

type ProductData struct {
	Result  int    `json:"result"`
	Message string `json:"message"`
	Pds     []struct {
		AppID      string `json:"app_id"`
		State      string `json:"state"`
		Code       int    `json:"code"`
		Name       string `json:"name"`
		Price      int    `json:"price"`
		Thum       string `json:"thum"`
		Link       string `json:"link"`
		Linkm      string `json:"linkm"`
		Hits       int    `json:"hits"`
		PdUtime    int    `json:"pd_utime"`
		PdRtime    int    `json:"pd_rtime"`
		UpdateTime int    `json:"update_time"`
		Idx        int    `json:"idx"`
	} `json:"pds"`
	Request struct {
		Op    string `json:"op"`
		AppID string `json:"app_id"`
	} `json:"request"`
}

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

func GetProductFromByapps(app_id string, action_type string, code string) ProductData {
	URL := config.Get("PRODUCT_API_" + strings.ToUpper(config.Get("MODE"))) + "/index.php?op=new&app_id=" + app_id
	res, err := CallByappsApi("GET", URL, config.Get("PRODUCT_KEY"))
    if err != nil {
		helper.Log("error", "common.GetNewProductFromByapps", "BYAPPS API 서버 탐색 실패")
        return ProductData{}
    }
    return res
}

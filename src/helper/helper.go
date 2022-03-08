package helper

import (
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"time"
)

func ConvOS(t string) string {
	switch t {
	case "total":
		return "T"
	case "android":
		return "A"
	case "ios":
		return "I"
	}
	return "T"
}

/*
 * 로그남기기
 *
 * @param
 *     string tag 파일태그
 *     string title 제목
 *     string content 내용
 *
 * @return void
 */
func Log(tag string, title string, content string) {
	now := time.Now()
	os.MkdirAll("log", os.ModePerm)
	file, openErr := os.OpenFile(fmt.Sprintf("log/%s_%s.log", tag, now.Format("200601")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	if openErr != nil {
		fmt.Printf("%#v\n", openErr)
		return
	}
	defer file.Close()

	msg := fmt.Sprintf("[%s][%s] %s\n", now.Format("2006-01-02 15:04:05"), title, content)
	_, writeErr := file.Write([]byte(msg))
	fmt.Printf("%s: %s", tag, msg)
	if writeErr != nil {
		fmt.Printf("%#v\n", writeErr)
	}
}

/*
 * 홀수 주 인지 짝수 주 인지 판단
 *
 * @param
 *		int week_value 주 차수
 * @return 
 * 		boolean 짝수 주에 해당하면 true, 홀수 주에 해당하면 false
 */
func IsEvenWeek(week_value int) bool {
	if week_value%2 == 0 {
		return true
	}
	return false
}

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
* 서버그룹 랜덤으로 지정
*/
func GetRandom() int {
	max := 2
	min := 0
	rand.Seed(time.Now().UTC().UnixNano())
	n := rand.Intn(max - min) + 1
	
	return n
}
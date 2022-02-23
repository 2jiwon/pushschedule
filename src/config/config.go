package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var config map[string]string

/*
 * 설정파일 가져오기
 *
 * @param
 *     string $key 설정 파일 이름
 *
 * @return string 설정 값
 */
func Get(key string) string {
	if config == nil {
		config = ReadConfig()
	}

	return config[key]
}

/*
 * 전체 설정 가져오기
 *
 * @return map[string]string 전체 설정
 */
func ReadConfig() map[string]string {
	result := make(map[string]string)
	file, err := os.Open(".env")
	if err != nil {
		fmt.Printf("%#v\n", err)
		return result
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		content := scanner.Text()
		split := strings.Split(content, "=")
		if len(split) == 2 {
			result[split[0]] = split[1]
		}
	}
	return result
}

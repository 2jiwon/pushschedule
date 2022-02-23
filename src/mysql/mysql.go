package mysql

import (
	"database/sql"
	"fmt"
	"pushschedule/src/config"
	"pushschedule/src/helper"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	//fmt.Printf("%#v\n", "init")
}

/*
 * DB 연결
 *
 * @param
 *     string connection 연결DB 이름
 *
 * @return *sqlx.DB 연결된 DB
 */
func Conn(connection string) (*sql.DB, error) {
	defer func() { //전역 에러 처리 및 복원
		if r := recover(); r != nil {
			helper.Log("error", "mysql.Conn.defer", fmt.Sprintf("%s", r))
		}
	}()

	db, err := sql.Open(
		"mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s:3306)/%s",
			config.Get(fmt.Sprintf("DB_%s_USER", strings.ToUpper(connection))),
			config.Get(fmt.Sprintf("DB_%s_PASS", strings.ToUpper(connection))),
			config.Get(fmt.Sprintf("DB_%s_HOST", strings.ToUpper(connection))),
			config.Get(fmt.Sprintf("DB_%s_NAME", strings.ToUpper(connection))),
		),
	)
	row, _ := db.Query("SET NAMES utf8mb4")
	defer row.Close()
	if err != nil {
		helper.Log("error", "mysql.Conn", fmt.Sprintf("%s", err))
	}
	return db, err
}

/*
 * mysql query
 *
 * @param
 *     string sql 쿼리문
 */
func Query(sql string) (ret []map[string]string, totalRecord int) {
	//fmt.Printf("Query:%#v\n", sql)
	ret = []map[string]string{}
	totalRecord = 0
	defer func() { //전역 에러 처리 및 복원
		if r := recover(); r != nil {
			helper.Log("error", "mysql.Query.defer", fmt.Sprintf("%s", r))
		}
	}()
	db, err := Conn("master")
	defer db.Close()
	if err != nil {
		helper.Log("error", "mysql.Query-Conn", fmt.Sprintf("%s", err))
	} else {

		rows, err := db.Query(sql)
		defer rows.Close()

		if err != nil {
			helper.Log("error", "mysql.Query", fmt.Sprintf("%s", err))
		} else {
			defer rows.Close()
			//var app_id string
			columns, err := rows.Columns()
			if err != nil {
				return
			}
			values := make([]interface{}, len(columns))
			scan_args := make([]interface{}, len(columns))
			for i := range values {
				scan_args[i] = &values[i]
			}
			for rows.Next() {
				retRow := map[string]string{}
				rows.Scan(scan_args...)
				for i, col := range columns {
					val := values[i]
					switch val.(type) {
					case []uint8:
						retRow[col] = string(val.([]uint8))
					default:
						retRow[col] = fmt.Sprintf("%s", val) //string(val)
					}
				}
				ret = append(ret, retRow)
			}
		}
	}
	totalRecord = len(ret)
	return ret, totalRecord
}

func GetRow(sql string) (row map[string]string, rowCnt int) {
	rowCnt = 0
	result, totalRecord := Query(sql)
	if totalRecord > 0 {
		rowCnt = 1
		row = result[0]
	}
	return row, rowCnt
}

func GetOne(sql string) string {
	result, totalRecord := Query(sql)
	if totalRecord > 0 {
		row := result[0]
		for _, val := range row {
			return val
		}
	}
	return ""
}

/*
 * mysql insert
 *
 * @param
 *     string tb 테이블명
 *     map f 	 필드 내용
 *	   bool re   idx 반환 여부
 */
func Insert(tb string, f map[string]interface{}, re bool) (int, int) {
	defer func() { //전역 에러 처리 및 복원
		if r := recover(); r != nil {
			helper.Log("error", "mysql.Insert.defer", fmt.Sprintf("%s", r))
		}
	}()
	db, err := Conn("master")
	defer db.Close()
	if err != nil {
		helper.Log("error", "mysql.Insert-Conn", fmt.Sprintf("%s", err))
	} else {
		var f1 []string
		var f2 []interface{}
		for key, value := range f {
			f1 = append(f1, key+"=?")
			f2 = append(f2, value)
		}

		sql := "INSERT INTO " + tb + " SET " + strings.Join(f1, ",")
		res, err := db.Exec(sql, f2...)
		if err != nil {
			helper.Log("error", "mysql.Insert-Exec", fmt.Sprintf("%s", err))
		} else {
			n, err := res.RowsAffected()
			id, _ := res.LastInsertId()
			if err != nil {
				helper.Log("error", "mysql.Insert-Exec", fmt.Sprintf("%s", err))
			} else {
				if n > 0 {
					if re == true {
						return int(n), int(id)
					} else {
						return int(n), 0
					}
				}
			}
		}
	}
	return 0, 0
}

/*
 * mysql update
 *
 * @param
 *     string tb 테이블명
 *     map f 업데이트할 필드 내용
 *     string w where
 */
func Update(tb string, f map[string]string, w string) int {
	defer func() { //전역 에러 처리 및 복원
		if r := recover(); r != nil {
			helper.Log("error", "mysql.Update.defer", fmt.Sprintf("%s", r))
		}
	}()
	db, err := Conn("master")
	defer db.Close()
	if err != nil {
		helper.Log("error", "mysql.Update-Conn", fmt.Sprintf("%s", err))
	} else {
		var f1 []string
		var f2 []interface{}
		for key, value := range f {
			f1 = append(f1, key+"=?")
			f2 = append(f2, value)
		}
		sql := "update " + tb + " set " + strings.Join(f1, ",") + " where " + w
		res, err := db.Exec(sql, f2...)
		if err != nil {
			helper.Log("error", "mysql.Update-Exec", fmt.Sprintf("%s", err))
		} else {
			n, err := res.RowsAffected()
			if err != nil {
				helper.Log("error", "mysql.Update-Exec", fmt.Sprintf("%s", err))
			} else {
				if n > 0 {
					return int(n)
				}
			}
		}
	}
	return 0
}

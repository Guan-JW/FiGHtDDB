package util

import (
	"log"
	"strconv"

	"database/sql"
)

func TupleToByte(resp *[]byte, cols ...interface{}) {
	for _, col := range cols {
		switch col := col.(type) {
		case int:
			*resp = append(*resp, strconv.Itoa(col)...)
		case string:
			*resp = append(*resp, col...)
		default:
			log.Fatal("unimplemeted column type to byte")
		}
		*resp = append(*resp, ","...)
	}
	*resp = append(*resp, "\n"...)
}

func remove(slice []int, s int) []int {
	return append(slice[:s], slice[s+1:]...)
}

func Strval(value interface{}) string {
	var key string
	if value == nil {
		return key
	}

	switch value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = "'" + value.(string) + "'"
	case []byte:
		key = "'" + string(value.([]byte)) + "'"
	case sql.RawBytes:
		key = "'" + string(value.(sql.RawBytes)) + "'"
	case sql.NullBool:
		boolnull := value.(sql.NullBool)
		if boolnull.Valid {
			key = strconv.FormatBool(boolnull.Bool)
		} else {
			key = "NULL"
		}
	case sql.NullString:
		stringnull := value.(sql.NullString)
		if stringnull.Valid {
			key = "'" + stringnull.String + "'"
		} else {
			key = "NULL"
		}
	case sql.NullFloat64:
		float64null := value.(sql.NullFloat64)
		if float64null.Valid {
			key = strconv.FormatFloat(float64null.Float64, 'f', -1, 64)
		} else {
			key = "NULL"
		}
	case sql.NullInt32:
		int32null := value.(sql.NullInt32)
		if int32null.Valid {
			key = strconv.Itoa(int(int32null.Int32))
		} else {
			key = "NULL"
		}
	case sql.NullInt64:
		int64null := value.(sql.NullInt64)
		if int64null.Valid {
			key = strconv.FormatInt(int64null.Int64, 10)
		} else {
			key = "NULL"
		}

	default:
		// newValue, _ := json.Marshal(value)
		// key = string(newValue)
	}

	return key
}
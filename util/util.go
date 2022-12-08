package util

import (
	"log"
	"strconv"
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

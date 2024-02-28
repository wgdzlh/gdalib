package utils

import (
	"bytes"
	"encoding/hex"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

var (
	validCodes = regexp.MustCompile(`^[A-Za-z0-9,]+$`)
)

func StrToInt32(s string) int32 {
	i, _ := strconv.Atoi(s)
	return int32(i)
}

func StrToUint32(s string) uint32 {
	i, _ := strconv.Atoi(s)
	return uint32(i)
}

func StrToInt64(s string) int64 {
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}

func StrToUint64(s string) uint64 {
	i, _ := strconv.ParseUint(s, 10, 64)
	return i
}

func StrToInt(s string) int {
	if s == "" {
		return 0
	}
	i, _ := strconv.Atoi(s)
	return i
}

func StrArrToInts(ss []string) []int {
	var (
		rets = make([]int, 0, len(ss))
		i    int
		e    error
	)
	for _, id := range ss {
		i, e = strconv.Atoi(id)
		if e == nil {
			rets = append(rets, i)
		}
	}
	return rets
}

func StrToInts(s, sep string) []int {
	var (
		ids  = strings.Split(s, sep)
		rets = make([]int, 0, len(ids))
		i    int
		e    error
	)
	for _, id := range ids {
		i, e = strconv.Atoi(id)
		if e == nil {
			rets = append(rets, i)
		}
	}
	return rets
}

func Int64sToStr(ids []int64, sep byte) string {
	var ret strings.Builder
	for i, id := range ids {
		if i > 0 {
			ret.WriteByte(sep)
		}
		ret.WriteString(strconv.FormatInt(id, 10))
	}
	return ret.String()
}

func UInt64sToStr(ids []uint64, sep byte) string {
	var ret strings.Builder
	for i, id := range ids {
		if i > 0 {
			ret.WriteByte(sep)
		}
		ret.WriteString(strconv.FormatUint(id, 10))
	}
	return ret.String()
}

func B2S(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func S2B(s string) []byte {
	const MaxInt32 = 1<<31 - 1
	return (*[MaxInt32]byte)(unsafe.Pointer((*reflect.StringHeader)(
		unsafe.Pointer(&s)).Data))[: len(s)&MaxInt32 : len(s)&MaxInt32]
}

func TrimTailCommas(s string) string {
	return strings.TrimRight(s, ",")
}

func ReplaceUnderline(s string) string {
	return strings.ReplaceAll(s, "_", "-")
}

func GetNowTimeTag() string {
	const tf = "20060102150405.000"
	t := time.Now().Format(tf)
	return t[:len(tf)-4] + t[len(tf)-3:]
}

func BsToHex(s string) string {
	src := S2B(s)
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return B2S(dst)
}

func ContainsAll(group, sub []string) bool {
out:
	for _, s := range sub {
		for _, a := range group {
			if a == s {
				continue out
			}
		}
		return false
	}
	return true
}

func ContainsAny(group, sub []string) bool {
	for _, s := range sub {
		for _, a := range group {
			if a == s {
				return true
			}
		}
	}
	return false
}

// GBK 转 UTF-8
func GbkToUtf8(s []byte) (d []byte, e error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GBK.NewDecoder())
	d, e = io.ReadAll(reader)
	return
}

// UTF-8 转 GBK
func Utf8ToGbk(s []byte) (d []byte, e error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GBK.NewEncoder())
	d, e = io.ReadAll(reader)
	return
}

// GBK string 转 UTF-8
func GbkStrToUtf8(s string) (d string, e error) {
	reader := transform.NewReader(strings.NewReader(s), simplifiedchinese.GBK.NewDecoder())
	t, e := io.ReadAll(reader)
	if e != nil {
		return
	}
	d = B2S(t)
	return
}

// UTF-8 string 转 GBK
func Utf8StrToGbk(s string) (d string, e error) {
	reader := transform.NewReader(strings.NewReader(s), simplifiedchinese.GBK.NewEncoder())
	t, e := io.ReadAll(reader)
	if e != nil {
		return
	}
	d = B2S(t)
	return
}

func PurifyForUtf8(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "")
}

// anti SQL injection
func CheckSQLTextValue(s string) bool {
	return validCodes.MatchString(s)
}

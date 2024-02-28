package utils

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	FILE_EXT_SHP = ".shp"
	FILE_EXT_CPG = ".cpg"

	FILE_EXT_TXT = ".txt"

	UTF8  = "UTF8"
	UTF_8 = "UTF-8"
)

var (
	ErrNoShpInZip = errors.New("no shp in zip")
)

func GetUniqSubDir(parentPath string) (path string, err error) {
	path = filepath.Join(parentPath, uuid.NewString())
	err = os.Mkdir(path, os.ModePerm)
	return
}

func GetDateSubDir(parentPath, date string) (path string, err error) {
	path = filepath.Join(parentPath, date)
	err = os.MkdirAll(path, os.ModePerm)
	return
}

func GetFilenameWithoutExt(path string) (name string) {
	name = filepath.Base(path)
	name = strings.TrimSuffix(name, filepath.Ext(path))
	return
}

func GetShpInZip(zipFile, dstDir string) (path string, utf8 bool, err error) {
	shpFiles, err := Unzip(zipFile, dstDir)
	if err != nil {
		return
	}
	os.Remove(zipFile)
	for _, file := range shpFiles {
		if strings.HasSuffix(file, FILE_EXT_SHP) {
			path = file
			continue
		}
		if strings.HasSuffix(file, FILE_EXT_CPG) {
			enc, e := os.ReadFile(file)
			if e == nil && len(enc) > 0 {
				encStr := strings.ToUpper(string(enc))
				utf8 = encStr == UTF_8 || encStr == UTF8
			}
		}
	}
	if path == "" {
		err = ErrNoShpInZip
	}
	return
}

func GetDistrictInShpName(shp string) (district string) {
	district = strings.TrimSuffix(filepath.Base(shp), FILE_EXT_SHP)
	return
}

func GetBasicBandIdx(bandOrder string) (idx [3]string, invalid bool) {
	bands := strings.Split(bandOrder, ",")
	for i, b := range bands {
		switch b {
		case "R":
			idx[0] = strconv.Itoa(i + 1)
		case "G":
			idx[1] = strconv.Itoa(i + 1)
		case "B":
			idx[2] = strconv.Itoa(i + 1)
		}
	}
	for _, b := range idx {
		if b == "" {
			invalid = true
			break
		}
	}
	return
}

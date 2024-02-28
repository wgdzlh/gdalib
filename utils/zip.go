package utils

import (
	"archive/zip"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"unicode/utf8"
)

const (
	ZIP_EXT = ".zip"
)

var (
	ErrAbsoluteZipPath = errors.New("absolute zip path")
)

func IsZip(fp string) bool {
	return filepath.Ext(fp) == ZIP_EXT
}

// CompressZip 打包任意文件夹
// srcDir 输入的文件夹相对路径
// dest zip文件存放路径
func CompressZip(dest, srcDir string) (err error) {
	if dest == "" || srcDir == "" {
		return
	}
	if srcDir[0] == os.PathSeparator {
		return ErrAbsoluteZipPath
	}
	df, err := os.Create(dest)
	if err != nil {
		return
	}
	defer df.Close()

	w := zip.NewWriter(df)
	defer w.Close()

	walker := func(path string, info os.FileInfo, e error) error {
		log.Println("zip crawling: " + path)
		if e != nil || info.IsDir() {
			return e
		}
		file, e := os.Open(path)
		if e != nil {
			return e
		}
		defer file.Close()

		// Ensure that `path` is not absolute, it should not start with "/"
		f, e := w.Create(path)
		if e != nil {
			return e
		}
		_, e = io.Copy(f, file)
		return e
	}
	err = filepath.Walk(srcDir, walker)
	log.Println("zip created: " + dest)
	return
}

// CompressFiles 打包任意个文件
// src 输入的文件路径列表
// dest zip文件存放路径
func CompressFiles(dest string, src ...string) (err error) {
	if dest == "" || len(src) == 0 {
		return
	}
	df, err := os.Create(dest)
	if err != nil {
		return
	}
	defer df.Close()

	w := zip.NewWriter(df)
	defer w.Close()

	var (
		file *os.File
		zfw  io.Writer
	)
	for _, path := range src {
		log.Println("zip crawling: " + path)
		file, err = os.Open(path)
		if err != nil {
			return
		}
		defer file.Close()

		// Ensure that `path` is not absolute, it should not start with "/"
		zfw, err = w.Create(filepath.Base(path))
		if err != nil {
			return
		}
		if _, err = io.Copy(zfw, file); err != nil {
			return
		}
	}
	log.Println("zip created: " + dest)
	return
}

func Unzip(archive, dstDir string) (files []string, err error) {
	reader, err := zip.OpenReader(archive)
	if err != nil {
		return
	}
	defer reader.Close()

	var (
		fileReader io.ReadCloser
		fileName   string
		filePath   string
		targetFile *os.File
	)
	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue // zip里一般不会有空文件夹，有的话就跳过
		}
		fileReader, err = file.Open()
		if err != nil {
			return
		}
		defer fileReader.Close()

		fileName = file.Name
		if !utf8.ValidString(fileName) {
			if fileName, err = GbkStrToUtf8(fileName); err != nil {
				return
			}
		}
		filePath = filepath.Join(dstDir, fileName)
		if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
			return
		}

		targetFile, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			return
		}
		defer targetFile.Close()

		if _, err = io.Copy(targetFile, fileReader); err != nil {
			return
		}
		files = append(files, filePath)
	}

	return
}

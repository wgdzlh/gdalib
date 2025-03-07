package gdalib

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/wgdzlh/gdalib/log"
	"github.com/wgdzlh/gdalib/utils"

	gdal "github.com/airbusgeo/godal"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	METEO_TIF_X = 6939
	METEO_TIF_Y = 5211
)

type RasterBand struct {
	gdal.Band
	Dataset *Dataset
}

// 读取一般Tif
func (g *GdalToolbox) ParseRaster(tif string, bands int) (buf [][]byte, dtSize int, err error) {
	sds, err := gdal.Open(tif, gdal.RasterOnly())
	if err != nil {
		log.Error(g.logTag+"open tif failed", zap.Error(err))
		err = ErrInvalidTif
		return
	}
	defer sds.Close()
	tifBands := sds.Bands()
	bc := len(tifBands)
	if bc < bands {
		log.Error(g.logTag+"tif bands not enough", zap.Int("bands", bc))
		err = ErrWrongTif
		return
	}
	log.Info(g.logTag+"start read tif", zap.Int("bands", bc), zap.Int("bufBn", bands))
	buf = make([][]byte, bands)
	for i := 0; i < bands; i++ {
		band := tifBands[i]
		bandStruct := band.Structure()
		dt := bandStruct.DataType
		x := bandStruct.SizeX
		y := bandStruct.SizeY
		log.Info(g.logTag+"read tif band", zap.Int("band", i), zap.Int("dt", int(dt)), zap.Int("width", x), zap.Int("height", y))
		dtSize = dt.Size()
		buf[i] = make([]byte, x*y*dtSize)
		err = band.IO(gdal.IORead, 0, 0, buf[i], x, y)
		if err != nil {
			log.Error(g.logTag+"read tif band failed", zap.Int("band", i), zap.Error(err))
			err = ErrTifReadFailed
			return
		}
	}
	return
}

// 读取气象Tif
func (g *GdalToolbox) ParseMeteoRaster(tif string, buf []int16) (err error) {
	if len(buf) != METEO_TIF_X*METEO_TIF_Y {
		err = ErrWrongBufferSize
		return
	}
	sds, err := gdal.Open(tif, gdal.RasterOnly())
	if err != nil {
		log.Error(g.logTag+"open meteo tif failed", zap.Error(err))
		err = ErrInvalidTif
		return
	}
	defer sds.Close()
	tifBands := sds.Bands()
	if bc := len(tifBands); bc != 1 {
		log.Error(g.logTag+"meteo tif can have only one band", zap.Int("bands", bc))
		err = ErrWrongTif
		return
	}
	band := tifBands[0]
	bandStruct := band.Structure()
	dt := bandStruct.DataType
	x := bandStruct.SizeX
	y := bandStruct.SizeY
	if dt != gdal.Int16 || x != METEO_TIF_X || y != METEO_TIF_Y {
		log.Error(g.logTag+"meteo tif is malformed", zap.String("dataType", dt.String()))
		err = ErrWrongTif
		return
	}
	log.Info(g.logTag+"read meteo tif", zap.Int("dt", int(dt)), zap.Int("width", x), zap.Int("height", y))
	err = band.IO(gdal.IORead, 0, 0, buf, x, y)
	if err != nil {
		log.Error(g.logTag+"read meteo tif band failed", zap.Error(err))
		err = ErrTifReadFailed
	}
	return
}

// 获取气象Tif中的Band
func (g *GdalToolbox) GetMeteoRasterBand(tif string) (band RasterBand, err error) {
	sds, err := gdal.Open(tif, gdal.RasterOnly())
	if err != nil {
		log.Error(g.logTag+"open meteo tif failed", zap.Error(err))
		err = ErrInvalidTif
		return
	}
	tifBands := sds.Bands()
	if bc := len(tifBands); bc != 1 {
		log.Error(g.logTag+"meteo tif can have only one band", zap.Int("bands", bc))
		err = ErrWrongTif
		return
	}
	band.Band = tifBands[0]
	bandStruct := band.Band.Structure()
	dt := bandStruct.DataType
	x := bandStruct.SizeX
	y := bandStruct.SizeY
	if dt != gdal.Int16 || x != METEO_TIF_X || y != METEO_TIF_Y {
		log.Error(g.logTag+"meteo tif is malformed", zap.String("dataType", dt.String()))
		err = ErrWrongTif
		return
	}
	log.Info(g.logTag+"get meteo tif band", zap.Int("dt", int(dt)), zap.Int("width", x), zap.Int("height", y))
	return
}

func (g *GdalToolbox) ReadMeteoRasterBandOffset(band RasterBand, xOff, yOff int) (ret int16, err error) {
	if xOff >= METEO_TIF_X || yOff >= METEO_TIF_Y {
		err = ErrWrongRasterOffset
		return
	}
	buf := make([]int16, 1)
	err = band.IO(gdal.IORead, xOff, yOff, buf, 1, 1)
	if err != nil {
		log.Error(g.logTag+"read meteo tif band offset failed", zap.Error(err))
		err = ErrTifReadFailed
		return
	}
	ret = buf[0]
	return
}

func (g *GdalToolbox) CloseMeteoRasterBand(band RasterBand) {
	if band.Dataset != nil {
		band.Dataset.Close()
	}
}

// 按各自有效区WKT剪切，并按目标区域WKT镶嵌多张影像tif
// 排序靠后的tif优先显示
func (g *GdalToolbox) CropRasters(tifWkt []ImgMergeFile, extWkt, out string) (err error) {
	n_tif := len(tifWkt)
	if n_tif == 0 {
		return
	}
	ref, err := g.getSridRef(UNIVERSAL_SRID)
	if err != nil {
		return
	}
	tRef, err := g.getSridRef(OUTPUT_SRID)
	if err != nil {
		return
	}
	var (
		ext        *Geometry
		geo        *Geometry
		sds        *Dataset
		ods        *Dataset
		part       string
		parts      []string
		opts       []string
		geoJson    []byte
		tmpGeoJson = filepath.Join(g.tmpDir, fmt.Sprintf(TMP_GEOJSON, uuid.NewString()))
		tmpVrt     = out + "_tmp.vrt"
	)
	trans, err := gdal.NewTransform(ref, tRef)
	if err != nil {
		return
	}
	gc := []destroyable{trans}
	defer func() {
		for _, v := range gc {
			v.Close()
		}
		os.Remove(tmpGeoJson)
		for _, part := range parts {
			os.Remove(part)
		}
	}()
	isUniform := true
	for i, t := range tifWkt[1:] {
		if t.BandOrder != tifWkt[i].BandOrder {
			isUniform = false
			break
		}
	}
	log.Info(g.logTag+"crop and merge rasters", zap.Int("tif_cnt", n_tif), zap.Bool("uniform", isUniform), zap.String("out", out))
	if extWkt != "" {
		if ext, err = g.parseWKT(extWkt, ref); err != nil {
			return
		}
		gc = append(gc, ext)
		if err = ext.Transform(trans); err != nil {
			return
		}
	}
	hasExt := ext != nil && !ext.Empty()
	for i := n_tif - 1; i >= 0; i-- {
		t := tifWkt[i]
		if geo, err = g.parseWKB(t.Wkb, ref); err != nil {
			return
		}
		gc = append(gc, geo)
		if err = geo.Transform(trans); err != nil {
			return
		}
		if hasExt {
			if geo, err = geo.Intersection(ext); err != nil {
				return
			}
			gc = append(gc, geo)
			if ext, err = ext.Difference(geo); err != nil {
				return
			}
			gc = append(gc, ext)
		}
		gt := geo.Type()
		if (gt != gdal.GTMultiPolygon && gt != gdal.GTPolygon) || geo.Empty() {
			log.Info(g.logTag+"encounter empty cut line geo", zap.Int("idx", i), zap.String("img", t.Infile))
			continue
		}
		if geoJson, err = g.geoToGeoJSON(geo); err != nil {
			return
		}
		if err = os.WriteFile(tmpGeoJson, geoJson, os.ModePerm); err != nil {
			return
		}
		sds, err = gdal.Open(t.Infile, gdal.RasterOnly())
		if err != nil {
			return
		}
		part = out + fmt.Sprintf("_%d_part.tif", i)
		opts = []string{"-cutline", tmpGeoJson, "-crop_to_cutline", "-overwrite", "-t_srs", fmt.Sprintf("epsg:%d", OUTPUT_SRID)}
		if !isUniform && t.BandOrder != "R,G,B" { // 若通道顺序不统一，则全部输出RGB格式影像
			if bands, invalid := utils.GetBasicBandIdx(t.BandOrder); invalid {
				log.Error(g.logTag+"invalid band order to merge", zap.String("img", t.Infile), zap.String("bands", t.BandOrder))
				continue
			} else {
				opts = append(opts, []string{"-b", bands[0], "-b", bands[1], "-b", bands[2]}...)
			}
		}
		ods, err = gdal.Warp(part, []*Dataset{sds}, opts) // 剪切影像
		sds.Close()
		if err != nil {
			log.Error(g.logTag+"failed to crop raster", zap.Error(err))
			return
		}
		ods.Close()
		parts = append([]string{part}, parts...)
		// dss = append([]*Dataset{ods}, dss...)
	}
	if len(parts) == 0 {
		err = ErrEmptyTif
		return
	} else if len(parts) > 1 {
		defer os.Remove(tmpVrt)
		// 将各景影像剪切结果拼接成一个VRT
		if ods, err = gdal.BuildVRT(tmpVrt, parts, []string{"-resolution", "highest", "-overwrite"}); err != nil {
			log.Error(g.logTag+"failed to build vrt", zap.Error(err))
			return
		}
	} else {
		ods, err = gdal.Open(parts[0], gdal.RasterOnly())
		if err != nil {
			log.Error(g.logTag+"open part tif failed", zap.Error(err))
			return
		}
	}
	defer ods.Close()
	// 将VRT转为最终GTiff
	finalDs, err := ods.Translate(out, []string{"-co", "compress=lzw"})
	if err != nil {
		log.Error(g.logTag+"failed to translate vrt", zap.Error(err))
		return
	}
	finalDs.Close()
	return
}

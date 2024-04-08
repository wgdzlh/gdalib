package gdalib

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/lukeroth/gdal"
	"github.com/wgdzlh/gdalib/log"
	"github.com/wgdzlh/gdalib/utils"
	"go.uber.org/zap"
)

const (
	METEO_TIF_X = 6939
	METEO_TIF_Y = 5211
)

// 读取一般Tif
func (g *GdalToolbox) ParseRaster(tif string, buf [][]byte) (err error) {
	bands := len(buf) // 需要读取的波段数
	sds, err := gdal.Open(tif, gdal.ReadOnly)
	if err != nil {
		log.Error(g.logTag+"open tif failed", zap.Error(err))
		err = ErrInvalidTif
		return
	}
	defer sds.Close()
	bc := sds.RasterCount()
	if bc < bands {
		log.Error(g.logTag+"tif bands not enough", zap.Int("bands", bc))
		err = ErrWrongTif
		return
	}
	log.Info(g.logTag+"start read tif", zap.Int("bands", bc), zap.Int("bufBn", bands))
	for i := 1; i <= bands; i++ {
		band := sds.RasterBand(i)
		dt := band.RasterDataType()
		x := band.XSize()
		y := band.YSize()
		if dt != gdal.Byte {
			log.Error(g.logTag+"tif is malformed", zap.String("dataType", dt.Name()))
			err = ErrWrongTif
			return
		}
		log.Info(g.logTag+"read tif band", zap.Int("band", i), zap.Int("dt", int(dt)), zap.Int("width", x), zap.Int("height", y))
		err = band.IO(gdal.Read, 0, 0, x, y, buf[i], x, y, 0, 0)
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
	sds, err := gdal.Open(tif, gdal.ReadOnly)
	if err != nil {
		log.Error(g.logTag+"open meteo tif failed", zap.Error(err))
		err = ErrInvalidTif
		return
	}
	defer sds.Close()
	if bc := sds.RasterCount(); bc != 1 {
		log.Error(g.logTag+"meteo tif can have only one band", zap.Int("bands", bc))
		err = ErrWrongTif
		return
	}
	band := sds.RasterBand(1)
	dt := band.RasterDataType()
	x := band.XSize()
	y := band.YSize()
	if dt != gdal.Int16 || x != METEO_TIF_X || y != METEO_TIF_Y {
		log.Error(g.logTag+"meteo tif is malformed", zap.String("dataType", dt.Name()))
		err = ErrWrongTif
		return
	}
	log.Info(g.logTag+"read meteo tif", zap.Int("dt", int(dt)), zap.Int("width", x), zap.Int("height", y))
	err = band.IO(gdal.Read, 0, 0, x, y, buf, x, y, 0, 0)
	if err != nil {
		log.Error(g.logTag+"read meteo tif band failed", zap.Error(err))
		err = ErrTifReadFailed
	}
	return
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
		ext        gdal.Geometry
		geo        gdal.Geometry
		sds        gdal.Dataset
		ods        gdal.Dataset
		dss        []gdal.Dataset
		part       string
		parts      []string
		opts       []string
		trans      = gdal.CreateCoordinateTransform(ref, tRef)
		tmpGeoJson = filepath.Join(g.tmpDir, fmt.Sprintf(TMP_GEOJSON, uuid.NewString()))
		tmpVrt     = out + "_tmp.vrt"
		gc         = []destroyable{trans}
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
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
	hasExt := ext != emptyGeometry && !ext.IsEmpty()
	for i := n_tif - 1; i >= 0; i-- {
		t := tifWkt[i]
		if geo, err = g.parseWKT(t.Wkt, ref); err != nil {
			return
		}
		gc = append(gc, geo)
		if err = geo.Transform(trans); err != nil {
			return
		}
		if hasExt {
			geo = geo.Intersection(ext)
			gc = append(gc, geo)
			ext = ext.Difference(geo)
			gc = append(gc, ext)
		}
		gt := geo.Type()
		if (gt != gdal.GT_MultiPolygon && gt != gdal.GT_Polygon) || geo.GeometryCount() == 0 {
			log.Info(g.logTag+"encounter empty cut line geo", zap.Int("idx", i), zap.String("img", t.Infile))
			continue
		}
		if err = os.WriteFile(tmpGeoJson, utils.S2B(geo.ToJSON()), os.ModePerm); err != nil {
			return
		}
		sds, err = gdal.Open(t.Infile, gdal.ReadOnly)
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
		ods, err = gdal.Warp(part, nil, []gdal.Dataset{sds}, opts) // 剪切影像
		sds.Close()
		if err != nil {
			log.Error(g.logTag+"failed to crop raster", zap.Error(err))
			return
		}
		defer ods.Close()
		parts = append([]string{part}, parts...)
		dss = append([]gdal.Dataset{ods}, dss...)
	}
	if len(dss) == 0 {
		err = ErrEmptyTif
		return
	} else if len(dss) > 1 {
		defer os.Remove(tmpVrt)
		// 将各景影像剪切结果拼接成一个VRT
		if ods, err = gdal.BuildVRT(tmpVrt, dss, parts, []string{"-resolution", "highest", "-overwrite"}); err != nil {
			log.Error(g.logTag+"failed to build vrt", zap.Error(err))
			return
		}
		defer ods.Close()
	}
	// 将VRT转为最终GTiff
	finalDs, err := gdal.Translate(out, ods, []string{"-co", "compress=lzw"})
	if err != nil {
		log.Error(g.logTag+"failed to translate vrt", zap.Error(err))
		return
	}
	finalDs.Close()
	return
}

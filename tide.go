package gdalib

import (
	"fmt"

	"github.com/lukeroth/gdal"
	"github.com/wgdzlh/gdalib/log"
	"github.com/wgdzlh/gdalib/utils"
	"go.uber.org/zap"
)

var (
	fieldIdGbk, _ = utils.Utf8StrToGbk(SHP_FIELD_SID)
	// fieldLabelGbk, _ = utils.Utf8StrToGbk(SHP_FIELD_LABEL)
)

// 解析tide site shp文件
func (g *GdalToolbox) ParseTideSiteShp(shp string) (ret []TideSpeckle, err error) {
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 0)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	layer := ds.LayerByIndex(0)
	def := layer.Definition()
	idIdx := def.FieldIndex(SHP_FIELD_SID)
	if idIdx < 0 {
		if idIdx = def.FieldIndex(fieldIdGbk); idIdx < 0 {
			err = fmt.Errorf(ErrColumnMissingTemplate, SHP_FIELD_SID)
			return
		}
	}
	n := 128
	nf, _ := layer.FeatureCount(false)
	if nf > 0 {
		n = nf
	}
	ret = make([]TideSpeckle, 0, n)
	var (
		feature *gdal.Feature
		wkb     []byte
		idStr   string
		e       error
		gc      []destroyable
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
		}
	}()
	for {
		if feature = layer.NextFeature(); feature != nil {
			gc = append(gc, *feature)
			wkb, e = feature.Geometry().ToWKB()
			if len(wkb) < 3 || e != nil {
				log.Error(g.logTag+"err in wkb trans", zap.Int64("fid", feature.FID()), zap.Error(e))
				continue
			}
			idStr = feature.FieldAsString(idIdx)
			if idStr == "" {
				log.Error(g.logTag+"empty id str", zap.Int64("fid", feature.FID()))
				continue
			}
			// label = feature.FieldAsString(labelIdx)
			// if !utf8 {
			// 	if label, e = utils.GbkStrToUtf8(label); e != nil {
			// 		log.Error(g.logTag+"err in trans-encoding label", zap.Int64("fid", feature.FID()), zap.Error(e))
			// 		continue
			// 	}
			// }
			ret = append(ret, TideSpeckle{
				Geom: sridPrefix + utils.BsToHex(wkb)[geomPrefixLen:], // change to srid 4326
				Id:   idStr,
				// Label: label,
			})
		} else {
			return
		}
	}
}

// 解析tide span shp文件
func (g *GdalToolbox) ParseTideSpanShp(shp string) (ret []TideSpan, err error) {
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 0)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	layer := ds.LayerByIndex(0)
	def := layer.Definition()
	idIdx := def.FieldIndex(SHP_FIELD_ID)
	if idIdx < 0 {
		err = fmt.Errorf(ErrColumnMissingTemplate, SHP_FIELD_ID)
		return
	}
	ret = []TideSpan{}
	var (
		feature *gdal.Feature
		wkt     string
		idStr   string
		e       error
		gc      []destroyable
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
		}
	}()
	for {
		if feature = layer.NextFeature(); feature != nil {
			gc = append(gc, *feature)
			wkt, e = feature.Geometry().ToWKT()
			if e != nil {
				log.Error(g.logTag+"err in wkt trans", zap.Int64("fid", feature.FID()), zap.Error(e))
				continue
			}
			idStr = feature.FieldAsString(idIdx)
			if idStr == "" {
				log.Error(g.logTag+"empty id str", zap.Int64("fid", feature.FID()))
				continue
			}
			ret = append(ret, TideSpan{
				SiteId: idStr,
				Wkt:    wkt,
			})
		} else {
			return
		}
	}
}

// 解析镶嵌Shp
func (g *GdalToolbox) GetGeoFromInlayShp(shp string) (rets []InlayShpGeo, err error) {
	log.Info(g.logTag+"start parse inlay shp", zap.String("shp", shp))
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 0)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	var (
		layer = ds.LayerByIndex(0)
		trans gdal.CoordinateTransform
	)
	sRef := layer.SpatialReference()
	srid, err := g.getSrid(sRef)
	if err != nil {
		return
	}
	tifIdx := layer.Definition().FieldIndex(SHP_FIELD_TIF)
	if tifIdx < 0 {
		err = fmt.Errorf(ErrColumnMissingTemplate, SHP_FIELD_TIF)
		return
	}
	var (
		feature *gdal.Feature
		wkb     []byte
		tif     string
		geo     gdal.Geometry
		gc      []destroyable
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
		}
	}()
	needTrans := srid != UNIVERSAL_SRID
	if needTrans {
		var tRef gdal.SpatialReference
		if tRef, err = g.getSridRef(UNIVERSAL_SRID); err != nil {
			return
		}
		trans = gdal.CreateCoordinateTransform(sRef, tRef)
		gc = append(gc, trans)
	}
	for {
		if feature = layer.NextFeature(); feature != nil {
			gc = append(gc, *feature)
			geo = feature.Geometry()
			if needTrans {
				if err = geo.Transform(trans); err != nil {
					return
				}
			}
			if wkb, err = geo.ToWKB(); err != nil {
				return
			}
			if tif = feature.FieldAsString(tifIdx); tif == "" {
				err = fmt.Errorf(ErrColumnEmptyTemplate, SHP_FIELD_TIF)
				return
			}
			rets = append(rets, InlayShpGeo{
				Tif: tif,
				Geo: wkb,
			})
		} else {
			break
		}
	}
	log.Info(g.logTag+"got geo from inlay shp", zap.String("shp", shp), zap.Int("srid", srid), zap.Int("geo_num", len(rets)))
	return
}

func (g *GdalToolbox) getGeoFromScatteredShp(shp string) (mergedGeo gdal.Geometry, srid, pCnt int, err error) {
	mergedGeo = gdal.Create(gdal.GT_MultiPolygon)
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 0)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	var (
		layer = ds.LayerByIndex(0)
		trans gdal.CoordinateTransform
	)
	sRef := layer.SpatialReference()
	srid, err = g.getSrid(sRef)
	if err != nil {
		return
	}
	var (
		feature *gdal.Feature
		geo     gdal.Geometry
		gc      []destroyable
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
		}
	}()
	needTrans := srid != UNIVERSAL_SRID
	if needTrans {
		var tRef gdal.SpatialReference
		if tRef, err = g.getSridRef(UNIVERSAL_SRID); err != nil {
			return
		}
		trans = gdal.CreateCoordinateTransform(sRef, tRef)
		gc = append(gc, trans)
	}
	for {
		if feature = layer.NextFeature(); feature != nil {
			gc = append(gc, *feature)
			geo = feature.StealGeometry()
			if needTrans {
				if err = geo.Transform(trans); err != nil {
					return
				}
			}
			switch geo.Type() {
			case gdal.GT_Polygon:
				if err = mergedGeo.AddGeometryDirectly(geo); err != nil {
					return
				}
				continue
			case gdal.GT_MultiPolygon:
				for i, pn := 0, geo.GeometryCount(); i < pn; i++ {
					if err = mergedGeo.AddGeometryDirectly(geo.Geometry(0)); err != nil {
						return
					}
					if err = geo.RemoveGeometry(0, false); err != nil {
						return
					}
				}
			}
			geo.Destroy()
		} else {
			break
		}
	}
	pCnt = mergedGeo.GeometryCount()
	if pCnt == 1 {
		geo = mergedGeo.Geometry(0)
		mergedGeo.RemoveGeometry(0, false)
		mergedGeo.Destroy()
		mergedGeo = geo
	}
	return
}

func (g *GdalToolbox) GetEWktFromScatteredShp(shp string) (ret string, err error) {
	wkt, err := g.GetWktFromScatteredShp(shp)
	if err != nil {
		log.Error(g.logTag+"failed to get wkt from shp", zap.String("shp", shp), zap.Error(err))
		return
	}
	ret = "SRID=4326;" + wkt
	return
}

func (g *GdalToolbox) GetWktFromScatteredShp(shp string) (ret string, err error) {
	log.Info(g.logTag+"start shp wkt trans", zap.String("shp", shp))
	mg, srid, pCnt, err := g.getGeoFromScatteredShp(shp)
	if pCnt > 0 {
		ret, err = mg.ToWKT()
	}
	mg.Destroy()
	log.Info(g.logTag+"got wkt from shp", zap.String("shp", shp), zap.Int("srid", srid), zap.Int("cnt", pCnt), zap.Error(err))
	return
}

func (g *GdalToolbox) GetWkbFromScatteredShp(shp string) (ret []byte, err error) {
	log.Info(g.logTag+"start shp wkb trans", zap.String("shp", shp))
	mg, srid, pCnt, err := g.getGeoFromScatteredShp(shp)
	if pCnt > 0 {
		ret, err = mg.ToWKB()
	}
	mg.Destroy()
	log.Info(g.logTag+"got wkb from shp", zap.String("shp", shp), zap.Int("srid", srid), zap.Int("cnt", pCnt), zap.Error(err))
	return
}

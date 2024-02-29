package gdalib

import (
	"fmt"
	"strings"

	"github.com/wgdzlh/gdalib/log"
	"github.com/wgdzlh/gdalib/utils"

	"github.com/lukeroth/gdal"
	"go.uber.org/zap"
)

func (g *GdalToolbox) parseShp(shp string, noTrans ...bool) (ret gdal.Geometry, err error) {
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 0)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	var (
		layer    = ds.LayerByIndex(0)
		mayTrans = len(noTrans) == 0 || !noTrans[0]
		srid     int
		feature  *gdal.Feature
		e        error
		gc       []destroyable
	)
	if mayTrans {
		if srid, err = g.getSrid(layer.SpatialReference()); err != nil {
			return
		}
	}
	defer func() {
		for _, v := range gc {
			v.Destroy()
		}
	}()
	ret = gdal.Create(gdal.GT_Polygon)
	for {
		if feature = layer.NextFeature(); feature != nil {
			gc = append(gc, *feature)
			gc = append(gc, ret)
			ret = ret.Union(feature.Geometry())
		} else {
			break
		}
	}
	if mayTrans && srid != UNIVERSAL_SRID {
		var tRef gdal.SpatialReference
		if tRef, err = g.getSridRef(UNIVERSAL_SRID); err == nil {
			if err = ret.TransformTo(tRef); err != nil {
				log.Error(g.logTag+"geo transform failed", zap.Error(e))
			}
		}
		if err != nil {
			gc = append(gc, ret)
		}
	}
	return
}

// 将shp转为单个WKB（srid=4326）
func (g *GdalToolbox) GetWkbFromShp(shp string) (ret GdalGeo, err error) {
	log.Info(g.logTag+"start shp wkb trans", zap.String("shp", shp))
	geo, err := g.parseShp(shp)
	if err != nil {
		return
	}
	defer geo.Destroy()
	if !geo.IsEmpty() {
		ret, err = geo.ToWKB()
	}
	log.Info(g.logTag+"got wkb from shp", zap.String("shp", shp), zap.Bool("succeed", err == nil && len(ret) > 0))
	return
}

// 将shp转为单个WKT（srid=4326）
func (g *GdalToolbox) GetWktFromShp(shp string) (ret string, err error) {
	log.Info(g.logTag+"start shp wkt trans", zap.String("shp", shp))
	geo, err := g.parseShp(shp)
	if err != nil {
		return
	}
	defer geo.Destroy()
	if !geo.IsEmpty() {
		ret, err = geo.ToWKT()
	}
	log.Info(g.logTag+"got wkt from shp", zap.String("shp", shp), zap.Bool("succeed", err == nil && ret != ""))
	return
}

// 将shp转为GeoJSON（srid=4326）
func (g *GdalToolbox) GetGeoJSONFromShp(shp string) (ret AnyJson, err error) {
	log.Info(g.logTag+"start shp GeoJSON trans", zap.String("shp", shp))
	geo, err := g.parseShp(shp)
	if err != nil {
		return
	}
	defer geo.Destroy()
	ret = utils.S2B(geo.ToJSON())
	log.Info(g.logTag+"got GeoJSON from shp", zap.String("shp", shp), zap.Bool("succeed", !geo.IsEmpty()))
	return
}

// 从shp文件转化生成geoJson文件，可通过dstSrid指定目标srid
func (g *GdalToolbox) ShapefileToGeoJSON(shp string, dstSrid ...int) (out string, err error) {
	log.Info(g.logTag+"start geojson shp", zap.String("shp", shp))
	sds, err := gdal.OpenEx(shp, gdal.OFVector, nil, nil, nil)
	if err != nil {
		log.Error(g.logTag+"open shp error", zap.Error(err))
		return
	}
	defer sds.Close()

	tSrid := GEOJSON_SRID
	if len(dstSrid) > 0 && dstSrid[0] > 0 {
		tSrid = dstSrid[0]
	}
	prefix := strings.TrimSuffix(shp, FILE_EXT_SHP)
	out = prefix + fmt.Sprintf("_%d"+FILE_EXT_JSON, tSrid)
	dds, err := gdal.VectorTranslate(out, []gdal.Dataset{sds}, []string{"-f", "GeoJSON", "-t_srs", fmt.Sprintf("epsg:%d", tSrid)})
	if err != nil {
		log.Error(g.logTag + "VectorTranslate failed")
		return
	}
	dds.Close() // 生成转换后的json文件
	log.Info(g.logTag+"end geojson shp", zap.String("shp", shp), zap.String("out", out))
	return
}

// 转换整个shp文件的坐标系
func (g *GdalToolbox) TransformShapefile(shp string, tSrid int) (out string, err error) {
	srid, err := g.GetSridOfShapefile(shp)
	if err != nil || srid == tSrid {
		out = shp
		return
	}
	sds, err := gdal.OpenEx(shp, gdal.OFVector, nil, nil, nil)
	if err != nil {
		log.Error(g.logTag+"open shp error", zap.Error(err))
		return
	}
	defer sds.Close()
	log.Info(g.logTag+"start transform shp", zap.String("shp", shp), zap.Int("srid", tSrid))
	prefix := strings.TrimSuffix(shp, FILE_EXT_SHP)
	out = prefix + fmt.Sprintf("_%d"+FILE_EXT_SHP, tSrid)
	dds, err := gdal.VectorTranslate(out, []gdal.Dataset{sds}, []string{"-t_srs", fmt.Sprintf("epsg:%d", tSrid), "-lco", ENCODING_OPTION})
	if err != nil {
		log.Error(g.logTag + "VectorTranslate failed")
		return
	}
	dds.Close() // 生成转换后的shp文件

	if e := sds.Driver().DeleteDataset(shp); e != nil {
		log.Info(g.logTag+"delete old shp failed", zap.Error(e))
	}
	log.Info(g.logTag+"end transform shp", zap.String("shp", out))
	return
}

// 转换整个shp文件的文本编码
func (g *GdalToolbox) EncodingShapefile(shp, cpg string, rmOld bool) (out string, err error) {
	if cpg == SHAPE_ENCODING || cpg == UTF8_ENC {
		out = shp
		return
	}
	// cpg为空，或者不为UTF-8的，都当作GBK编码处理
	sds, err := gdal.OpenEx(shp, gdal.OFVector, nil, []string{OO_ENCODING}, nil)
	if err != nil {
		log.Error(g.logTag+"open shp error", zap.Error(err))
		return
	}
	defer sds.Close()
	log.Info(g.logTag+"start encoding shp", zap.String("shp", shp), zap.String("cpg", cpg))
	prefix := strings.TrimSuffix(shp, FILE_EXT_SHP)
	out = prefix + fmt.Sprintf("_%s"+FILE_EXT_SHP, cpg)
	dds, err := gdal.VectorTranslate(out, []gdal.Dataset{sds}, []string{"-lco", ENCODING_OPTION})
	if err != nil {
		log.Error(g.logTag + "VectorTranslate failed")
		return
	}
	dds.Close() // 生成转换后的shp文件

	// cmd := exec.Command("ogr2ogr", out, shp, "-oo", OO_ENCODING, "-lco", ENCODING_OPTION)
	// err = cmd.Run()

	if rmOld {
		if e := sds.Driver().DeleteDataset(shp); e != nil {
			log.Info(g.logTag+"delete old shp failed", zap.Error(e))
		}
	}
	log.Info(g.logTag+"end encoding shp", zap.String("shp", out))
	// log.Info(g.logTag+"end encoding shp", zap.String("cmd", cmd.String()), zap.String("shp", out))
	return
}

// 获取shp文件中的标签
func (g *GdalToolbox) GetLabelsFromShapefile(shp, labelField string) (labels []string, err error) {
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 0)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	layer := ds.LayerByIndex(0)
	labelIdx := layer.Definition().FieldIndex(labelField)
	if labelIdx < 0 {
		err = fmt.Errorf(ErrColumnMissingTemplate, labelField)
		return
	}
	var (
		labelSet = map[string]struct{}{}
		feature  *gdal.Feature
		label    string
		cnt      int
		gc       []destroyable
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
		}
	}()
	for {
		if feature = layer.NextFeature(); feature != nil {
			gc = append(gc, *feature)
			label = feature.FieldAsString(labelIdx)
			if label == "" {
				err = fmt.Errorf(ErrColumnEmptyTemplate, labelField)
				return
			}
			labelSet[label] = struct{}{}
			cnt++
		} else {
			break
		}
	}
	for k := range labelSet {
		labels = append(labels, k)
	}
	log.Info(g.logTag+"got labels from shp", zap.String("file", shp), zap.Any("labels", labels), zap.Int("cnt", cnt))
	return
}

// 从shp文件中解析出图斑矢量
func (g *GdalToolbox) ParseShapefile(shp, labelField string) (ret []Speckle, err error) {
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 0)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	layer := ds.LayerByIndex(0)
	def := layer.Definition()
	labelIdx := -1
	if labelField != "" {
		labelIdx = def.FieldIndex(labelField)
		if labelIdx < 0 {
			err = fmt.Errorf(ErrColumnMissingTemplate, labelField)
			return
		}
	}
	ret = make([]Speckle, 0, 128)
	var (
		feature *gdal.Feature
		geo     gdal.Geometry
		wkb     []byte
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
			geo = feature.Geometry()
			wkb, e = geo.ToWKB()
			if e != nil {
				log.Error(g.logTag+"err in wkb convert", zap.String("geom", geo.ToGML()), zap.Error(e))
				continue
			}
			sp := Speckle{
				Geom: wkb,
			}
			if labelIdx >= 0 {
				sp.ClassName = feature.FieldAsString(labelIdx)
			}
			ret = append(ret, sp)
		} else {
			return
		}
	}
}

// 更新shp文件中的标签，可通过zone shp（两个shp坐标系要一致）指定更新/截取区域
func (g *GdalToolbox) UpdateLabelInShapefile(shp, labelField, zone string, alignRet AlignedLabel) (err error) {
	needUpdate := false
	for key, pair := range alignRet {
		if key != pair[0] {
			needUpdate = true
			break
		}
	}
	if !needUpdate && zone == "" {
		return
	}
	log.Info(g.logTag+"update label with ref", zap.Any("alignRet", alignRet), zap.String("zoneShp", zone))
	mz := emptyGeometry
	if zone != "" {
		if mz, err = g.parseShp(zone, true); err != nil {
			return
		}
	}
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 1)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	layer := ds.LayerByIndex(0)
	labelIdx := layer.Definition().FieldIndex(labelField)
	if labelIdx < 0 {
		err = fmt.Errorf(ErrColumnMissingTemplate, labelField)
		return
	}
	var (
		feature *gdal.Feature
		label   string
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
			if mz != emptyGeometry && !mz.Contains(feature.Geometry()) {
				layer.Delete(feature.FID())
				continue
			}
			if !needUpdate {
				continue
			}
			label = feature.FieldAsString(labelIdx)
			feature.SetFieldString(labelIdx, alignRet[label][0])
			if e = layer.SetFeature(*feature); e != nil {
				log.Error(g.logTag+"err in set feature of layer", zap.Error(e))
			}
		} else {
			return
		}
	}
}

func (g *GdalToolbox) getShpDriver(shp string, srid int) (ds gdal.DataSource, ref gdal.SpatialReference, layer gdal.Layer, err error) {
	log.Info(g.logTag+"output shp files", zap.String("shp", shp), zap.Int("srid", srid))
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Create(shp, nil)
	if !ok {
		err = ErrGdalDriverCreate
		return
	}
	if ref, err = g.getSridRef(srid); err != nil {
		return
	}
	layer = ds.CreateLayer("", ref, gdal.GT_Unknown, []string{ENCODING_OPTION})
	return
}

func (g *GdalToolbox) initShpLayer(layer gdal.Layer, labelField string) (err error) {
	log.Info(g.logTag+"init shp layer", zap.String("labelField", labelField))
	objectLabel := gdal.CreateFieldDefinition(labelField, gdal.FT_String)
	objectLabel.SetWidth(64)
	err = layer.CreateField(objectLabel, false)
	return
}

// 将选定矢量WKB写入shp
func (g *GdalToolbox) WriteGeoToShapefile(shp string, srid int, gs ...GdalGeo) (err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	ds, _, layer, err := g.getShpDriver(shp, srid)
	if err != nil {
		return
	}
	defer ds.Destroy() // 生成shp文件 + 释放资源
	var (
		def     = layer.Definition()
		feature gdal.Feature
		geo     gdal.Geometry
		valid   int
		e       error
		gc      = make([]destroyable, len(gs))
	)
	for i, v := range gs {
		feature = def.Create()
		gc[i] = feature
		e = feature.SetFID(int64(i))
		if e != nil {
			log.Error(g.logTag+"err in set feature fid", zap.Error(e))
			continue
		}
		if geo, e = g.parseWKB(v, ref); e != nil {
			continue
		}
		e = feature.SetGeometryDirectly(geo)
		if e != nil {
			log.Error(g.logTag+"err in set geom of feature", zap.Error(e))
			continue
		}
		if e = layer.Create(feature); e != nil {
			log.Error(g.logTag+"err in create feature of layer", zap.Error(e))
			continue
		}
		valid++
	}
	for _, v := range gc {
		v.Destroy()
	}
	log.Info(g.logTag+"output geo to shapefile done", zap.String("shp", shp), zap.Int("total", len(gs)), zap.Int("valid", valid))
	return
}

// 将选定图斑矢量写入shp
func (g *GdalToolbox) WriteShapefile(shp, labelField string, srid int, speckles ...Speckle) (err error) {
	ds, ref, layer, err := g.getShpDriver(shp, srid)
	if err != nil {
		return
	}
	defer ds.Destroy() // 生成shp文件 + 释放资源
	if labelField != "" {
		if err = g.initShpLayer(layer, labelField); err != nil {
			return
		}
	}
	var (
		def = layer.Definition()
		// uidIdx = def.FieldIndex(SHP_FIELD_UID)
		labelIdx int = -1
		feature  gdal.Feature
		geo      gdal.Geometry
		cnt      int
		e        error
		gc       = make([]destroyable, len(speckles))
	)
	if labelField != "" {
		labelIdx = def.FieldIndex(labelField)
	}
	for i, vec := range speckles {
		feature = def.Create()
		gc[i] = feature
		e = feature.SetFID(int64(i))
		if e != nil {
			log.Error(g.logTag+"err in set feature fid", zap.Error(e))
			continue
		}
		if labelIdx >= 0 {
			feature.SetFieldString(labelIdx, vec.ClassName)
		}
		if geo, e = g.parseWKB(vec.Geom, ref); e != nil {
			continue
		}
		if e = feature.SetGeometryDirectly(geo); e != nil {
			log.Error(g.logTag+"err in set geom of feature", zap.Error(e))
			continue
		}
		if e = layer.Create(feature); e != nil {
			log.Error(g.logTag+"err in create feature of layer", zap.Error(e))
			continue
		}
		cnt++
	}
	for _, v := range gc {
		v.Destroy()
	}
	log.Info(g.logTag+"shp files created", zap.String("shp", shp), zap.Int("total", len(speckles)), zap.Int("valid", cnt))
	return
}

// 将选定区域矢量写入shp
func (g *GdalToolbox) WriteZoneShapefile(shp string, srid int, ucs ...Uncertainty) (err error) {
	ds, ref, layer, err := g.getShpDriver(shp, srid)
	if err != nil {
		return
	}
	defer ds.Destroy() // 生成shp文件 + 释放资源
	objectOid := gdal.CreateFieldDefinition(SHP_FIELD_OID, gdal.FT_Integer)
	if err = layer.CreateField(objectOid, false); err != nil {
		return
	}
	var (
		def     = layer.Definition()
		feature gdal.Feature
		geo     gdal.Geometry
		cnt     int
		e       error
		gc      = make([]destroyable, len(ucs))
	)
	for i, vec := range ucs {
		feature = def.Create()
		gc[i] = feature
		e = feature.SetFID(int64(i))
		if e != nil {
			log.Error(g.logTag+"err in set feature fid", zap.Error(e))
			continue
		}
		feature.SetFieldInteger(0, vec.Id)
		if geo, e = g.parseWKB(vec.Geom, ref); e != nil {
			continue
		}
		e = feature.SetGeometryDirectly(geo)
		if e != nil {
			log.Error(g.logTag+"err in set geom of feature", zap.Error(e))
			continue
		}
		if e = layer.Create(feature); e != nil {
			log.Error(g.logTag+"err in create feature of layer", zap.Error(e))
			continue
		}
		cnt++
	}
	for _, v := range gc {
		v.Destroy()
	}
	log.Info(g.logTag+"zone shp files created", zap.String("shp", shp), zap.Int("total", len(ucs)), zap.Int("valid", cnt))
	return
}

// 将图斑合并区域矢量写入shp
func (g *GdalToolbox) WriteMergedShapefile(shp string, uc Uncertainty) (err error) {
	sRef, err := g.getSridRef(GEOJSON_SRID)
	if err != nil {
		return
	}
	ucGeo, err := g.parseWKB(uc.Geom, sRef)
	if err != nil {
		return
	}
	defer ucGeo.Destroy()
	ds, tRef, layer, err := g.getShpDriver(shp, OUTPUT_SRID)
	if err != nil {
		return
	}
	defer ds.Destroy() // 生成shp文件 + 释放资源
	if err = ucGeo.TransformTo(tRef); err != nil {
		log.Error(g.logTag+"geo transform failed", zap.Error(err))
		return
	}
	var polygons []gdal.Geometry
	switch ucGeo.Type() {
	case gdal.GT_Polygon:
		polygons = []gdal.Geometry{ucGeo}
	case gdal.GT_MultiPolygon:
		gNum := ucGeo.GeometryCount()
		polygons = make([]gdal.Geometry, gNum)
		for i := range polygons {
			polygons[i] = ucGeo.Geometry(i)
		}
	default:
		err = ErrGdalWrongGeoType
		return
	}
	var (
		def     = layer.Definition()
		feature gdal.Feature
		cnt     int
		e       error
		gc      = make([]destroyable, len(polygons))
	)
	for i := range polygons {
		feature = def.Create()
		gc[i] = feature
		e = feature.SetFID(int64(i))
		if e != nil {
			log.Error(g.logTag+"err in set feature fid", zap.Error(e))
			continue
		}
		if e = feature.SetGeometry(polygons[i]); e != nil {
			log.Error(g.logTag+"err in set geom of feature", zap.Error(e))
			continue
		}
		if e = layer.Create(feature); e != nil {
			log.Error(g.logTag+"err in create feature of layer", zap.Error(e))
			continue
		}
		cnt++
	}
	for _, v := range gc {
		v.Destroy()
	}
	log.Info(g.logTag+"merged zone shp files created", zap.String("shp", shp), zap.Int("total", len(polygons)), zap.Int("valid", cnt))
	return
}

package gdalib

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/wgdzlh/gdalib/log"
	"github.com/wgdzlh/gdalib/utils"

	"github.com/lukeroth/gdal"
	"go.uber.org/zap"
)

type GdalToolbox struct {
	refMap map[int]gdal.SpatialReference
	rLock  sync.Mutex
	tmpDir string
	logTag string
}

var (
	emptyGeometry = gdal.Geometry{}
)

func NewGdalToolbox(tmpDir ...string) *GdalToolbox {
	g := &GdalToolbox{
		refMap: map[int]gdal.SpatialReference{},
		logTag: "GdalToolbox:",
	}
	if len(tmpDir) > 0 && tmpDir[0] != "" {
		g.tmpDir = tmpDir[0]
	}
	return g
}

func (g *GdalToolbox) getShpDriver(shp string, srid int) (ds gdal.DataSource, ref gdal.SpatialReference, layer gdal.Layer, err error) {
	log.Info(g.logTag+"writing shp files", zap.String("shp", shp), zap.Int("srid", srid))
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

// 将选定图斑矢量写入shp
func (g *GdalToolbox) WriteShapefile(shp, labelField string, srid int, speckles []Speckle) (err error) {
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
		gc       = make([]Destroyable, len(speckles))
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
	for _, g := range gc {
		g.Destroy()
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
		gc      = make([]Destroyable, len(ucs))
	)
	for i, vec := range ucs {
		feature = def.Create()
		gc[i] = feature
		e = feature.SetFID(int64(i))
		if e != nil {
			log.Error(g.logTag+"err in set feature fid", zap.Error(e))
			continue
		}
		feature.SetFieldInteger(0, vec.Fid)
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
	for _, g := range gc {
		g.Destroy()
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
		log.Error(g.logTag+"transform geometry failed", zap.Error(err))
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
		gc      = make([]Destroyable, len(polygons))
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
	for _, g := range gc {
		g.Destroy()
	}
	log.Info(g.logTag+"merged zone shp files created", zap.String("shp", shp), zap.Int("total", len(polygons)), zap.Int("valid", cnt))
	return
}

// 解析shp文件
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
		gc      []Destroyable
	)
	defer func() {
		for _, g := range gc {
			g.Destroy()
		}
	}()
	for {
		if feature = layer.NextFeature(); feature != nil {
			gc = append(gc, *feature)
			geo = feature.Geometry()
			wkb, e = geo.ToWKB()
			if e != nil {
				log.Error(g.logTag+"err in wkb trans", zap.String("geom", geo.ToGML()), zap.Error(e))
				continue
			}
			sp := Speckle{
				Geom: utils.B2S(wkb),
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

// 获取shp文件中的标签
func (g *GdalToolbox) GetLabelsFromShapefile(shp, labelField string) (ret []string, err error) {
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
		gc       []Destroyable
	)
	defer func() {
		for _, g := range gc {
			g.Destroy()
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
		ret = append(ret, k)
	}
	log.Info(g.logTag+"got labels from shp", zap.String("file", shp), zap.Any("labels", ret), zap.Int("cnt", cnt))
	return
}

// 更新shp文件中的标签
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
		if mz, err = g.getMergedZoneFromShp(zone); err != nil {
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
		gc      []Destroyable
	)
	defer func() {
		for _, g := range gc {
			g.Destroy()
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

func (g *GdalToolbox) SubtractZones(uc *Uncertainty, subs []Uncertainty, srid int) (err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	ucGeo := gdal.CreateFromJson(uc.Geom)
	defer ucGeo.Destroy()
	if srid != GEOJSON_SRID {
		if err = ucGeo.TransformTo(ref); err != nil {
			log.Error(g.logTag+"transform geometry failed", zap.Error(err))
			return
		}
	}
	var (
		geo gdal.Geometry
		e   error
	)
	for _, vec := range subs {
		geo, e = g.parseWKB(vec.Geom, ref)
		if e != nil {
			continue
		}
		defer geo.Destroy()
		ucGeo = ucGeo.Difference(geo)
		defer ucGeo.Destroy()
	}
	wkb, err := ucGeo.ToWKB()
	if err != nil {
		return
	}
	uc.Geom = utils.B2S(wkb)
	return
}

func (g *GdalToolbox) ShapefileToGeoJson(shp string, tSrid int) (ret AnyJson, err error) {
	log.Info(g.logTag+"start geojson shp", zap.String("shp", shp))
	sds, err := gdal.OpenEx(shp, gdal.OFVector, nil, nil, nil)
	if err != nil {
		log.Error(g.logTag+"open shp error", zap.Error(err))
		return
	}
	defer sds.Close()

	prefix := strings.TrimSuffix(shp, FILE_EXT_SHP)
	out := prefix + fmt.Sprintf("_%d"+FILE_EXT_JSON, tSrid)
	dds, err := gdal.VectorTranslate(out, []gdal.Dataset{sds}, []string{"-f", "GeoJSON", "-t_srs", fmt.Sprintf("epsg:%d", tSrid)})
	if err != nil {
		log.Error(g.logTag + "VectorTranslate failed")
		return
	}
	dds.Close() // 生成转换后的json文件

	ret, err = os.ReadFile(out)
	log.Info(g.logTag+"end geojson shp", zap.String("shp", shp))
	return
}

func (g *GdalToolbox) getMergedZoneFromShp(shp string) (ret gdal.Geometry, err error) {
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 0)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	ret = gdal.Create(gdal.GT_Polygon)
	var (
		layer   = ds.LayerByIndex(0)
		feature *gdal.Feature
		gc      []Destroyable
	)
	defer func() {
		for _, g := range gc {
			g.Destroy()
		}
	}()
	for {
		if feature = layer.NextFeature(); feature != nil {
			gc = append(gc, *feature)
			gc = append(gc, ret)
			ret = ret.Union(feature.Geometry())
		} else {
			return
		}
	}
}

func (g *GdalToolbox) GetZoneJsonFromShp(shp string) (ret AnyJson, err error) {
	// if shp, err = g.TransformShapefile(shp, GEOJSON_SRID); err != nil {
	// 	return
	// }
	log.Info(g.logTag+"start zone json trans", zap.String("shp", shp))
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
	needTrans := srid != GEOJSON_SRID
	if needTrans {
		var tRef gdal.SpatialReference
		if tRef, err = g.getSridRef(GEOJSON_SRID); err != nil {
			return
		}
		trans = gdal.CreateCoordinateTransform(sRef, tRef)
	}
	var (
		unionGeo = gdal.Create(gdal.GT_Polygon)
		feature  *gdal.Feature
		geo      gdal.Geometry
		gc       = []Destroyable{unionGeo}
	)
	defer func() {
		for _, g := range gc {
			g.Destroy()
		}
	}()
	for {
		if feature = layer.NextFeature(); feature != nil {
			gc = append(gc, *feature)
			geo = feature.Geometry()
			if needTrans {
				if err = geo.Transform(trans); err != nil {
					return
				}
			}
			unionGeo = unionGeo.Union(geo)
			gc = append(gc, unionGeo)
		} else {
			break
		}
	}
	ret = utils.S2B(unionGeo.ToJSON())
	log.Info(g.logTag+"got zone json from shp", zap.String("shp", shp), zap.Int("srid", srid))
	return
}

func (g *GdalToolbox) ProcessZoneMerge(uc *Uncertainty, dis int) (ret AnyJson, err error) {
	log.Info(g.logTag+"start process zone merge", zap.Int("ucSize", len(uc.Geom)), zap.Int("caseId", uc.Fid), zap.Int("dis", dis))
	ref, err := g.getSridRef(GEOJSON_SRID)
	if err != nil {
		return
	}
	mergedSg, err := g.parseWKB(uc.Geom, ref)
	if err != nil {
		return
	}
	defer mergedSg.Destroy()
	mergeDis := MergeBufferDistance
	if dis > 0 {
		mergeDis = float64(dis) * MergeBufferMeter
	}
	// 缓冲 + 合并
	unionGeo := g.splitAndHullBuff(mergedSg, mergeDis)
	defer unionGeo.Destroy()
	// 再次拆分 + 凸包
	ucGeo := g.splitAndHullBuff(unionGeo)
	defer ucGeo.Destroy()
	ret = utils.S2B(ucGeo.ToJSON())
	log.Info(g.logTag+"output merge json", zap.Int("caseId", uc.Fid), zap.Int("dis", dis))
	return
}

func (g *GdalToolbox) splitAndHullBuff(geo gdal.Geometry, dis ...float64) (rGeo gdal.Geometry) {
	var gc []Destroyable
	if geo.Type() == gdal.GT_Polygon {
		rGeo = geo.ConvexHull()
		gc = append(gc, rGeo)
		if len(dis) > 0 {
			rGeo = rGeo.Buffer(dis[0], MergeBufferSegs)
		}
	} else {
		rGeo = gdal.Create(gdal.GT_Polygon)
		geoCount := geo.GeometryCount()
		for i := 0; i < geoCount; i++ {
			subGeo := geo.Geometry(i)
			if subGeo.Type() != gdal.GT_Polygon {
				log.Error(g.logTag+"wrong type in geom", zap.Uint("type", uint(subGeo.Type())))
				continue
			}
			subGeo = subGeo.ConvexHull()
			gc = append(gc, subGeo)
			if len(dis) > 0 {
				subGeo = subGeo.Buffer(dis[0], MergeBufferSegs)
				gc = append(gc, subGeo)
			}
			gc = append(gc, rGeo)
			rGeo = rGeo.Union(subGeo)
		}
	}
	for _, g := range gc {
		g.Destroy()
	}
	return
}

func (g *GdalToolbox) getSridRef(srid int) (ref gdal.SpatialReference, err error) {
	g.rLock.Lock()
	defer g.rLock.Unlock()
	ref, ok := g.refMap[srid]
	if ok {
		return
	}
	ref = gdal.CreateSpatialReference("")
	if err = ref.FromEPSG(srid); err != nil { // 设定坐标系ID
		log.Error(g.logTag+"set ref srid failed", zap.Int("srid", srid), zap.Error(err))
		ref.Destroy()
		return
	}
	// 这里应设置坐标系对应的数据轴次序为固定的经度/纬度（传统GIS坐标序），而不是与CRS相关的次序。否则在转换坐标系或者转GeoJSON时，可能出现次序倒置问题
	// 目前公司前后端所有空间坐标数据都为固定的经度/纬度次序
	ref.SetAxisMappingStrategy(gdal.OAMS_TraditionalGisOrder)
	// OAMS_TRADITIONAL_GIS_ORDER means that for geographic CRS with lat/long order, the data will still be long/lat ordered. Similarly for a projected CRS with northing/easting order, the data will still be easting/northing ordered.
	// OAMS_AUTHORITY_COMPLIANT means that the data axis will be identical to the CRS axis. This is the default value when instantiating OGRSpatialReference.
	// OAMS_CUSTOM means that the data axes are customly defined with SetDataAxisToSRSAxisMapping().
	g.refMap[srid] = ref
	return
}

func (g *GdalToolbox) GetSridOfShapefile(shp string) (srid int, err error) {
	driver := gdal.OGRDriverByName(SHP_DRIVER_NAME)
	ds, ok := driver.Open(shp, 0)
	if !ok {
		err = ErrGdalDriverOpen
		return
	}
	defer ds.Destroy()
	layer := ds.LayerByIndex(0)
	return g.getSrid(layer.SpatialReference())
}

func (g *GdalToolbox) getSrid(sp gdal.SpatialReference) (srid int, err error) {
	// sp.AutoIdentifyEPSG() // 可能对不规范的shp文件需要
	wkt, _ := sp.ToWKT()
	log.Info(g.logTag+"spatial ref attrs", zap.String("attr", wkt))
	rawId, ok := sp.AttrValue("AUTHORITY", 1)
	if !ok {
		if strings.Contains(wkt, "CGCS_2000") {
			rawId = "4490"
		} else {
			err = ErrVoidSrid
			return
		}
	}
	srid, err = strconv.Atoi(rawId)
	log.Info(g.logTag+"got srid from sp", zap.String("id", rawId))
	return
}

func (g *GdalToolbox) GeoJSONToWKB(geoJson AnyJson) (ret string, err error) {
	geo := gdal.CreateFromJson(utils.B2S(geoJson))
	defer geo.Destroy()
	if geo.WKBSize() == 0 {
		err = ErrGdalWrongGeoJSON
		return
	}
	wkb, err := geo.ToWKB()
	if err != nil {
		return
	}
	ret = utils.B2S(wkb)
	return
}

func (g *GdalToolbox) GetAreaCoverage(districtGeom string, imagesGeom []string) (ratios []float32, dst AnyJson, unions, diffs []AnyJson, err error) {
	log.Info(g.logTag + "start get area coverage")
	ref, err := g.getSridRef(UNIVERSAL_SRID)
	if err != nil {
		return
	}
	district, err := g.parseWKB(districtGeom, ref)
	if err != nil {
		return
	}
	// if district.SpatialReference().EPSGTreatsAsLatLong() { // 如果经纬度顺序不对，则通过和空的多边形合并来调换（废弃，见getSridRef函数）
	// 	district = gdal.Create(gdal.GT_Polygon).Union(district)
	// }
	dst = utils.S2B(district.ToJSON())
	n := len(imagesGeom)
	ratios = make([]float32, n)
	unions = make([]AnyJson, n)
	diffs = make([]AnyJson, n)
	var (
		unionGeo     gdal.Geometry
		geo          gdal.Geometry
		ratio        float32
		interArea    float64
		districtArea = district.Area()
		gc           = []Destroyable{district}
	)
	defer func() {
		for _, g := range gc {
			g.Destroy()
		}
	}()
	for i, imgGeom := range imagesGeom {
		// unionGeo = gdal.Create(gdal.GT_Polygon)
		// for _, gs := range imgGeom {
		if unionGeo, err = g.parseWKB(imgGeom, ref); err != nil {
			return
		}
		gc = append(gc, unionGeo)
		// 	unionGeo = unionGeo.Union(subGeo)
		// }
		// 计算覆盖率
		geo = district.Intersection(unionGeo)
		interArea = geo.Area()
		gc = append(gc, geo)
		ratio = float32(interArea / districtArea)
		ratios[i] = ratio
		// 影像范围合集
		unions[i] = utils.S2B(unionGeo.ToJSON())
		if ratio < CoverageThreshold {
			// 地区与影像范围差集
			geo = district.Difference(unionGeo)
			diffs[i] = utils.S2B(geo.ToJSON())
			gc = append(gc, geo)
		}
	}
	log.Info(g.logTag+"got area coverage", zap.Any("ratios", ratios))
	return
}

func (g *GdalToolbox) GetAreaCoverageRatio(districtWkt string, imagesWkt []string) (ratio float32, err error) {
	log.Info(g.logTag + "start get coverage ratio")
	ref, err := g.getSridRef(UNIVERSAL_SRID)
	if err != nil {
		return
	}
	district, err := gdal.CreateFromWKT(districtWkt, ref)
	if err != nil {
		log.Error(g.logTag+"pares district wkt failed", zap.Error(err))
		return
	}
	var (
		unionGeo = gdal.Create(gdal.GT_Polygon)
		subGeo   gdal.Geometry
		gc       = []Destroyable{district, unionGeo}
	)
	defer func() {
		for _, g := range gc {
			g.Destroy()
		}
	}()
	for _, gs := range imagesWkt {
		if subGeo, err = gdal.CreateFromWKT(gs, ref); err != nil {
			return
		}
		unionGeo = unionGeo.Union(subGeo)
		gc = append(gc, subGeo)
		gc = append(gc, unionGeo)
	}
	// 计算覆盖率
	districtArea := district.Area()
	unionGeo = district.Intersection(unionGeo)
	interArea := unionGeo.Area()
	gc = append(gc, unionGeo)
	ratio = float32(interArea / districtArea)
	log.Info(g.logTag+"got coverage ratio", zap.Float32("ratio", ratio))
	return
}

func (g *GdalToolbox) parseAlgWKT(wkt string) (ret gdal.Geometry, err error) {
	ref, err := g.getSridRef(WKT_ALG_SRID)
	if err != nil {
		return
	}
	ret, err = gdal.CreateFromWKT(wkt, ref)
	if err != nil {
		log.Error(g.logTag+"pares wkt failed", zap.Error(err))
	}
	return
}

func (g *GdalToolbox) parseWKB(geom string, ref gdal.SpatialReference) (ret gdal.Geometry, err error) {
	ret, err = gdal.CreateFromWKB(utils.S2B(geom), ref, len(geom))
	if err != nil {
		log.Error(g.logTag+"err in wkb parsing", zap.String("geom", utils.BsToHex(geom)), zap.Error(err))
	}
	return
}

func (g *GdalToolbox) CheckWkt(wkt string) (err error) {
	ref, err := g.getSridRef(UNIVERSAL_SRID)
	if err != nil {
		return
	}
	geo, e := gdal.CreateFromWKT(wkt, ref)
	if e != nil {
		err = ErrInvalidWKT
		return
	}
	geo.Destroy()
	return
}

func (g *GdalToolbox) GetWktFromShp(shp string) (ret string, err error) {
	log.Info(g.logTag+"start shp wkt trans", zap.String("shp", shp))
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
	var (
		unionGeo = gdal.Create(gdal.GT_Polygon)
		feature  *gdal.Feature
		geo      gdal.Geometry
		gc       = []Destroyable{unionGeo}
	)
	defer func() {
		for _, g := range gc {
			g.Destroy()
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
			unionGeo = unionGeo.Union(geo)
			gc = append(gc, unionGeo)
		} else {
			break
		}
	}
	if !unionGeo.IsEmpty() {
		ret, err = unionGeo.ToWKT()
	}
	log.Info(g.logTag+"got wkt from shp", zap.String("shp", shp), zap.Int("srid", srid), zap.Bool("succeed", err == nil && ret != ""))
	return
}

// 将选定矢量写入shp
func (g *GdalToolbox) WriteGeoToShapefile(shp string, gs ...GdalGeo) (err error) {
	ref, err := g.getSridRef(UNIVERSAL_SRID)
	if err != nil {
		return
	}
	ds, _, layer, err := g.getShpDriver(shp, UNIVERSAL_SRID)
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
		gc      = make([]Destroyable, len(gs))
	)
	for i, vec := range gs {
		feature = def.Create()
		gc[i] = feature
		e = feature.SetFID(int64(i))
		if e != nil {
			log.Error(g.logTag+"err in set feature fid", zap.Error(e))
			continue
		}
		geo, e = gdal.CreateFromWKB(vec, ref, len(vec))
		if e != nil {
			log.Error(g.logTag+"err in parse wkb to geo", zap.Error(e))
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
	for _, g := range gc {
		g.Destroy()
	}
	log.Info(g.logTag+"output geo to shapefile done", zap.String("shp", shp), zap.Int("total", len(gs)), zap.Int("valid", valid))
	return
}

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
		gc         = []Destroyable{trans}
	)
	defer func() {
		for _, g := range gc {
			g.Destroy()
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
		if ext, err = gdal.CreateFromWKT(extWkt, ref); err != nil {
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
		if geo, err = gdal.CreateFromWKT(t.Wkt, ref); err != nil {
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

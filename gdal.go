package gdalib

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

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

// 由GDAL库C语言创建的内存对象，需要手动调用Destroy回收
type destroyable interface {
	Destroy()
}

var (
	emptyGeometry = gdal.Geometry{}
)

// 初始化GDAL工具箱，tmpDir为可选的临时目录路径（未提供的话为当前目录）
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

// 获取srid对应的坐标系（可复用，故无需回收）
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
	// 这里应设置坐标系对应的数据轴次序为固定的(经度,纬度)（传统GIS坐标序），而不是新标准中与CRS相关的次序。否则在转换坐标系或者转GeoJSON时，可能出现次序倒置问题
	// 目前我们处理的空间坐标数据都为固定的(经度,纬度)次序
	ref.SetAxisMappingStrategy(gdal.OAMS_TraditionalGisOrder)
	// OAMS_TRADITIONAL_GIS_ORDER means that for geographic CRS with lat/long order, the data will still be long/lat ordered. Similarly for a projected CRS with northing/easting order, the data will still be easting/northing ordered.
	// OAMS_AUTHORITY_COMPLIANT means that the data axis will be identical to the CRS axis. This is the default value when instantiating OGRSpatialReference.
	// OAMS_CUSTOM means that the data axes are customly defined with SetDataAxisToSRSAxisMapping().
	g.refMap[srid] = ref
	return
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

// 获取shp的srid
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

func (g *GdalToolbox) parseWKB(wkb GdalGeo, ref gdal.SpatialReference) (ret gdal.Geometry, err error) {
	ret, err = gdal.CreateFromWKB(wkb, ref, len(wkb))
	if err != nil {
		log.Error(g.logTag+"parse wkb failed", zap.Error(err))
	}
	return
}

func (g *GdalToolbox) parseWKT(wkt string, ref gdal.SpatialReference) (ret gdal.Geometry, err error) {
	ret, err = gdal.CreateFromWKT(wkt, ref)
	if err != nil {
		log.Error(g.logTag+"parse wkt failed", zap.Error(err))
		err = ErrInvalidWKT
	}
	return
}

// 转换WKB坐标系
func (g *GdalToolbox) TransformWkb(wkb GdalGeo, srid, tSrid int) (ret GdalGeo, err error) {
	if tSrid == srid {
		ret = wkb
		return
	}
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	tRef, err := g.getSridRef(tSrid)
	if err != nil {
		return
	}
	geo, err := g.parseWKB(wkb, ref)
	if err != nil {
		return
	}
	defer geo.Destroy()
	if err = geo.TransformTo(tRef); err != nil {
		log.Error(g.logTag+"geo transform failed", zap.Error(err))
		return
	}
	ret, err = geo.ToWKB()
	return
}

// 转换WKT坐标系
func (g *GdalToolbox) TransformWkt(wkt string, srid, tSrid int) (ret string, err error) {
	if tSrid == srid {
		ret = wkt
		return
	}
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	tRef, err := g.getSridRef(tSrid)
	if err != nil {
		return
	}
	geo, err := g.parseWKT(wkt, ref)
	if err != nil {
		return
	}
	defer geo.Destroy()
	if err = geo.TransformTo(tRef); err != nil {
		log.Error(g.logTag+"geo transform failed", zap.Error(err))
		return
	}
	ret, err = geo.ToWKT()
	return
}

// 检查WKT有效性
func (g *GdalToolbox) CheckWkt(wkt string, srid int) (err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	geo, err := g.parseWKT(wkt, ref)
	if err != nil {
		return
	}
	geo.Destroy()
	return
}

// WKT转WKB
func (g *GdalToolbox) WktToWkb(wkt string, srid int) (wkb GdalGeo, err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	geo, err := g.parseWKT(wkt, ref)
	if err != nil {
		return
	}
	wkb, err = geo.ToWKB()
	geo.Destroy()
	return
}

// WKB转WKT
func (g *GdalToolbox) WkbToWkt(wkb GdalGeo, srid int) (wkt string, err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	geo, err := g.parseWKB(wkb, ref)
	if err != nil {
		return
	}
	wkt, err = geo.ToWKT()
	geo.Destroy()
	return
}

// GeoJSON转WKB
func (g *GdalToolbox) GeoJSONToWkb(geoJson AnyJson) (ret GdalGeo, err error) {
	geo := gdal.CreateFromJson(utils.B2S(geoJson))
	defer geo.Destroy()
	if geo.WKBSize() == 0 {
		err = ErrGdalWrongGeoJSON
		return
	}
	ret, err = geo.ToWKB()
	return
}

// WKB转GeoJSON
func (g *GdalToolbox) WkbToGeoJSON(wkb GdalGeo, srid int) (ret AnyJson, err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	geo, err := g.parseWKB(wkb, ref)
	if err != nil {
		return
	}
	ret = utils.S2B(geo.ToJSON())
	geo.Destroy()
	return
}

// 合并多个WKB矢量面
func (g *GdalToolbox) Union(gs []GdalGeo, srid int) (ret GdalGeo, err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	var (
		geo      gdal.Geometry
		unionGeo = gdal.Create(gdal.GT_Polygon)
		gc       = []destroyable{unionGeo}
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
		}
	}()
	for _, a := range gs {
		if geo, err = g.parseWKB(a, ref); err != nil {
			return
		}
		gc = append(gc, geo)
		unionGeo = unionGeo.Union(geo)
		gc = append(gc, unionGeo)
	}
	ret, err = unionGeo.ToWKB()
	return
}

// 获取多个WKB矢量面公共区
func (g *GdalToolbox) Intersection(gs []GdalGeo, srid int) (ret GdalGeo, err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	var (
		geo      gdal.Geometry
		interGeo = gdal.Create(gdal.GT_Polygon)
		gc       = []destroyable{interGeo}
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
		}
	}()
	for _, a := range gs {
		if geo, err = g.parseWKB(a, ref); err != nil {
			return
		}
		gc = append(gc, geo)
		interGeo = interGeo.Intersection(geo)
		gc = append(gc, interGeo)
	}
	ret, err = interGeo.ToWKB()
	return
}

// 求两个WKB矢量面之差
func (g *GdalToolbox) Difference(gA, gB GdalGeo, srid int) (ret GdalGeo, err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	geoA, err := g.parseWKB(gA, ref)
	if err != nil {
		return
	}
	defer geoA.Destroy()
	geoB, err := g.parseWKB(gB, ref)
	if err != nil {
		return
	}
	defer geoB.Destroy()
	diffGeo := geoA.Difference(geoB)
	ret, err = diffGeo.ToWKB()
	diffGeo.Destroy()
	return
}

// 从目标区域WKB中剪除多个其他区域WKB
func (g *GdalToolbox) SubtractZones(uc *Uncertainty, subs []Uncertainty, srid int) (err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	ucGeo, err := g.parseWKB(uc.Geom, ref)
	if err != nil {
		return
	}
	var (
		geo gdal.Geometry
		e   error
		gc  = []destroyable{ucGeo}
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
		}
	}()
	for _, vec := range subs {
		if geo, e = g.parseWKB(vec.Geom, ref); e != nil {
			continue
		}
		gc = append(gc, geo)
		ucGeo = ucGeo.Difference(geo)
		gc = append(gc, ucGeo)
	}
	uc.Geom, err = ucGeo.ToWKB()
	return
}

// 获取WKT经纬度范围
func (g *GdalToolbox) GetWktSpan(wkt string, srid int) (span [4]float64, err error) {
	ref, err := g.getSridRef(srid)
	if err != nil {
		return
	}
	geo, err := g.parseWKT(wkt, ref)
	if err != nil {
		return
	}
	defer geo.Destroy()
	envelop := geo.Envelope()
	span[0] = envelop.MinX()
	span[1] = envelop.MaxX()
	span[2] = envelop.MinY()
	span[3] = envelop.MaxY()
	return
}

func PointsToWkt(lon1, lon2, lat1, lat2 float64) string {
	return fmt.Sprintf("POLYGON((%[1]f %[3]f, %[1]f %[4]f, %[2]f %[4]f, %[2]f %[3]f, %[1]f %[3]f))", lon1, lon2, lat1, lat2)
}

func SpanToWkt(span [4]float64) string {
	return PointsToWkt(span[0], span[1], span[2], span[3])
}

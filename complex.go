package gdalib

import (
	"github.com/wgdzlh/gdalib/log"
	"github.com/wgdzlh/gdalib/utils"

	"github.com/lukeroth/gdal"
	"go.uber.org/zap"
)

// 拆分、凸包+缓冲、合并目标区域WKB，输出GeoJSON
func (g *GdalToolbox) ProcessZoneMerge(uc *Uncertainty, dis int) (ret AnyJson, err error) {
	log.Info(g.logTag+"start process zone merge", zap.Int("ucSize", len(uc.Geom)), zap.Int("id", uc.Id), zap.Int("dis", dis))
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
	log.Info(g.logTag+"output merge json", zap.Int("id", uc.Id), zap.Int("dis", dis))
	return
}

func (g *GdalToolbox) splitAndHullBuff(geo gdal.Geometry, dis ...float64) (rGeo gdal.Geometry) {
	var gc []destroyable
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
	for _, v := range gc {
		v.Destroy()
	}
	return
}

// 获取多个影像范围WKB分别在目标区域中的覆盖率及目标区域、影像范围、未覆盖区域的GeoJSON
func (g *GdalToolbox) GetAreaCoverage(districtGeom GdalGeo, imagesGeom []GdalGeo) (ratios []float32, dst AnyJson, unions, diffs []AnyJson, err error) {
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
		gc           = []destroyable{district}
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
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

// 获取多个影像的集合在目标区域中的覆盖率
func (g *GdalToolbox) GetAreaCoverageRatio(districtWkt string, imagesWkt []string) (ratio float32, err error) {
	log.Info(g.logTag + "start get coverage ratio")
	ref, err := g.getSridRef(UNIVERSAL_SRID)
	if err != nil {
		return
	}
	district, err := gdal.CreateFromWKT(districtWkt, ref)
	if err != nil {
		log.Error(g.logTag+"parse district wkt failed", zap.Error(err))
		return
	}
	var (
		unionGeo = gdal.Create(gdal.GT_Polygon)
		subGeo   gdal.Geometry
		gc       = []destroyable{district, unionGeo}
	)
	defer func() {
		for _, v := range gc {
			v.Destroy()
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

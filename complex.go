package gdalib

import (
	"github.com/wgdzlh/gdalib/log"

	"go.uber.org/zap"
)

/*
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
	defer mergedSg.Close()
	mergeDis := MergeBufferDistance
	if dis > 0 {
		mergeDis = float64(dis) * MergeBufferMeter
	}
	// 缓冲 + 合并
	unionGeo, err := g.splitAndHullBuff(mergedSg, mergeDis)
	if err != nil {
		return
	}
	defer unionGeo.Close()
	// 再次拆分 + 凸包
	ucGeo, err := g.splitAndHullBuff(unionGeo)
	if err != nil {
		return
	}
	defer ucGeo.Close()
	ret, err = g.geoToGeoJSON(ucGeo)
	log.Info(g.logTag+"output merge json", zap.Int("id", uc.Id), zap.Int("dis", dis), zap.Error(err))
	return
}

func (g *GdalToolbox) splitAndHullBuff(geo *Geometry, dis ...float64) (rGeo *Geometry, err error) {
	var (
		gc     []destroyable
		subGeo *Geometry
	)
	defer func() {
		for _, v := range gc {
			v.Close()
		}
	}()
	if geo.Type() == gdal.GTPolygon {
		rGeo = geo.ConvexHull()
		gc = append(gc, rGeo)
		if len(dis) > 0 {
			if rGeo, err = rGeo.Buffer(dis[0], MergeBufferSegs); err != nil {
				return
			}
		}
	} else {
		if rGeo, err = g.GetEmptyPolygon(geo.SpatialRef()); err != nil {
			return
		}
		geoCount := geo.GeometryCount()
		for i := 0; i < geoCount; i++ {
			if subGeo, err = geo.SubGeometry(i); err != nil {
				return
			}
			if subGeo.Type() != gdal.GTPolygon {
				log.Error(g.logTag+"wrong type in geom", zap.Uint("type", uint(subGeo.Type())))
				continue
			}
			subGeo = subGeo.ConvexHull()
			gc = append(gc, subGeo)
			if len(dis) > 0 {
				if subGeo, err = subGeo.Buffer(dis[0], MergeBufferSegs); err != nil {
					return
				}
				gc = append(gc, subGeo)
			}
			gc = append(gc, rGeo)
			if rGeo, err = rGeo.Union(subGeo); err != nil {
				return
			}
		}
	}
	return
}
*/

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
	if dst, err = g.geoToGeoJSON(district); err != nil {
		return
	}
	n := len(imagesGeom)
	ratios = make([]float32, n)
	unions = make([]AnyJson, n)
	diffs = make([]AnyJson, n)
	var (
		unionGeo     *Geometry
		geo          *Geometry
		ratio        float32
		interArea    float64
		districtArea = district.Area()
		gc           = []destroyable{district}
	)
	defer func() {
		for _, v := range gc {
			v.Close()
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
		if geo, err = district.Intersection(unionGeo); err != nil {
			return
		}
		interArea = geo.Area()
		gc = append(gc, geo)
		ratio = float32(interArea / districtArea)
		ratios[i] = ratio
		// 影像范围合集
		unions[i], err = g.geoToGeoJSON(unionGeo)
		if ratio < CoverageThreshold {
			// 地区与影像范围差集
			if geo, err = district.Difference(unionGeo); err != nil {
				return
			}
			if diffs[i], err = g.geoToGeoJSON(geo); err != nil {
				return
			}
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
	district, err := g.parseWKT(districtWkt, ref)
	if err != nil {
		return
	}
	unionGeo, err := g.GetEmptyPolygon(ref)
	if err != nil {
		return
	}
	gc := []destroyable{district, unionGeo}
	defer func() {
		for _, v := range gc {
			v.Close()
		}
	}()
	var subGeo *Geometry
	for _, gs := range imagesWkt {
		if subGeo, err = g.parseWKT(gs, ref); err != nil {
			return
		}
		if unionGeo, err = unionGeo.Union(subGeo); err != nil {
			return
		}
		gc = append(gc, subGeo)
		gc = append(gc, unionGeo)
	}
	// 计算覆盖率
	districtArea := district.Area()
	if unionGeo, err = district.Intersection(unionGeo); err != nil {
		return
	}
	interArea := unionGeo.Area()
	gc = append(gc, unionGeo)
	ratio = float32(interArea / districtArea)
	log.Info(g.logTag+"got coverage ratio", zap.Float32("ratio", ratio))
	return
}

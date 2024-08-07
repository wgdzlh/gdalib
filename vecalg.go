package gdalib

import (
	"fmt"
	"math"

	"github.com/wgdzlh/gdalib/log"

	"github.com/lukeroth/gdal"
	"go.uber.org/zap"
)

const (
	CutLineBuffDist   = 0.0001
	MergeLineBuffDist = 0.002
	BuffPercent       = 0.05
	BuffQuadSegs      = 12
)

func (g *GdalToolbox) parseAlgWKT(wkt string) (ret gdal.Geometry, err error) {
	ref, err := g.getSridRef(WKT_ALG_SRID)
	if err != nil {
		return
	}
	ret, err = gdal.CreateFromWKT(wkt, ref)
	if err != nil {
		log.Error(g.logTag+"parse alg wkt failed", zap.Error(err))
		err = ErrInvalidWKT
	}
	return
}

func (g *GdalToolbox) simpGeo(geo gdal.Geometry, t float64) (wkt string, err error) {
	defer geo.Destroy()
	// t := config.C.Server.GeoSimplifyT
	if t <= 0 {
		t = SimplifyT
	}
	log.Info(g.logTag+"simplify geo", zap.Float64("tolerance", t))
	ret := geo.SimplifyPreservingTopology(t)
	defer ret.Destroy()
	area := ret.Area()
	if area <= 0 {
		return
	}
	buff := math.Sqrt(area) * BuffPercent
	ret = ret.Buffer(-buff, BuffQuadSegs) // 腐蚀
	ret = ret.Buffer(buff, BuffQuadSegs)  // 膨胀
	wkt, err = ret.ToWKT()
	return
}

func (g *GdalToolbox) muffGeo(geo gdal.Geometry) (ret gdal.Geometry, err error) {
	switch geo.Type() {
	case gdal.GT_Polygon:
		err = removeHolesInPolygon(geo)
		ret = geo.Clone()
	case gdal.GT_MultiPolygon:
		// ret = gdal.Create(gdal.GT_MultiPolygon)
		var subGeo gdal.Geometry
		gNum := geo.GeometryCount()
		for i := 0; i < gNum; i++ {
			subGeo = geo.Geometry(i)
			if err = removeHolesInPolygon(subGeo); err != nil {
				return
			}
			if gNum == 1 {
				ret = subGeo.Clone()
				return
			}
			// if err = ret.AddGeometryDirectly(subGeo); err != nil {
			// 	return
			// }
		}
		ret = geo.UnionCascaded() // avoid overlaps
	default:
		err = ErrGdalWrongGeoType
	}
	return
}

func (g *GdalToolbox) Simplify(wkt string) (out string, err error) {
	log.Info(g.logTag + "start simplify wkt")
	geo, err := g.parseAlgWKT(wkt)
	if err != nil {
		return
	}
	out, err = g.simpGeo(geo, 0)
	return
}

func (g *GdalToolbox) MuffAndSimp(wkt string, t float64) (out string, err error) {
	log.Info(g.logTag + "start muff and simp wkt")
	geo, err := g.parseAlgWKT(wkt)
	if err != nil {
		return
	}
	defer geo.Destroy()
	if geo, err = g.muffGeo(geo); err != nil {
		return
	}
	out, err = g.simpGeo(geo, t)
	return
}

func (g *GdalToolbox) parseAndCheck(wkt, line string) (geo, st gdal.Geometry, np int, err error) {
	st, err = g.parseAlgWKT(line)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			st.Destroy()
			if geo != emptyGeometry {
				geo.Destroy()
			}
		}
	}()
	if st.Type() != gdal.GT_LineString {
		err = ErrGdalWrongGeoType
		return
	}
	geo, err = g.parseAlgWKT(wkt)
	if err != nil {
		return
	}
	switch geo.Type() {
	case gdal.GT_Polygon, gdal.GT_MultiPolygon:
	default:
		err = ErrGdalWrongGeoType
		return
	}
	np = st.PointCount()
	if np < 2 {
		err = ErrNotEnoughLinePoints
	}
	return
}

func (g *GdalToolbox) Cut(wkt, line string) (out []string, err error) {
	log.Info(g.logTag + "start cut wkt")
	geo, st, _, err := g.parseAndCheck(wkt, line)
	if err != nil {
		return
	}
	defer geo.Destroy()
	defer st.Destroy()

	if !geo.Intersects(st) {
		out = []string{wkt}
		return
	}
	buffedLine := st.Buffer(CutLineBuffDist, 1)
	defer buffedLine.Destroy()

	switch geo.Type() {
	case gdal.GT_Polygon:
		geo = geo.Difference(buffedLine)
		defer geo.Destroy()

		switch geo.Type() {
		case gdal.GT_Polygon:
			out = make([]string, 1)
			out[0], err = geo.ToWKT()
		case gdal.GT_MultiPolygon:
			out = make([]string, geo.GeometryCount())
			for i := range out {
				if out[i], err = geo.Geometry(i).ToWKT(); err != nil {
					return
				}
			}
		default:
			err = fmt.Errorf("wrong geo type: %d", geo.Type())
		}
	case gdal.GT_MultiPolygon:
		excluded := gdal.Create(gdal.GT_MultiPolygon)
		defer excluded.Destroy()

		ng := geo.GeometryCount()
		var subGeo gdal.Geometry
		for i := 0; i < ng; {
			subGeo = geo.Geometry(i)
			if subGeo.Intersects(st) {
				if err = geo.RemoveGeometry(i, false); err != nil {
					return
				}
				if err = excluded.AddGeometryDirectly(subGeo); err != nil {
					return
				}
				ng--
				continue
			}
			i++
		}
		if ng == 1 {
			geo = geo.Geometry(0)
		}
		excluded = excluded.Difference(buffedLine)
		defer excluded.Destroy()

		switch excluded.Type() {
		case gdal.GT_Polygon:
			out = make([]string, 2)
			if out[1], err = excluded.ToWKT(); err != nil {
				return
			}
		case gdal.GT_MultiPolygon:
			ng = excluded.GeometryCount()
			out = make([]string, ng+1)
			for i := 0; i < ng; i++ {
				if out[i+1], err = excluded.Geometry(i).ToWKT(); err != nil {
					return
				}
			}
		default:
			err = fmt.Errorf("wrong geo type: %d", excluded.Type())
			return
		}
		if geo.IsEmpty() {
			out = out[1:]
		} else {
			out[0], err = geo.ToWKT()
		}
	}
	return
}

func (g *GdalToolbox) Reshape(wkt, line string) (out string, err error) {
	log.Info(g.logTag + "start reshape wkt")
	geo, st, np, err := g.parseAndCheck(wkt, line)
	if err != nil {
		return
	}
	defer geo.Destroy()
	defer st.Destroy()

	if !geo.Intersects(st) {
		if np == 2 {
			out = wkt
		} else {
			var subRegion gdal.Geometry
			if subRegion, err = buildPolygon(st, np); err != nil {
				return
			}
			defer subRegion.Destroy()
			geo = geo.Difference(subRegion)
			defer geo.Destroy()
			out, err = geo.ToWKT()
		}
		return
	}

	ends := st.Boundary()
	defer ends.Destroy()

	if ends.GeometryCount() != 2 {
		err = ErrWrongLineEndsCount
		return
	}
	// log.Info(g.logTag+"ends within st", zap.Bool("ret", ends.Intersects(st)))
	buffedLine := st.Buffer(CutLineBuffDist, 1)
	defer buffedLine.Destroy()

	if geo.Intersects(ends.Geometry(0)) && geo.Intersects(ends.Geometry(1)) {
		geo = geo.Union(buffedLine)
		defer geo.Destroy()
		if geo, err = g.muffGeo(geo); err != nil {
			return
		}
	} else if geo.Disjoint(ends) {
		// log.Info(g.logTag+"points count of line string", zap.Int("num", np))
		if np <= 3 {
			if geo, err = removeSmallerPolygons(geo, buffedLine); err != nil {
				return
			}
		} else {
			var trimRegion gdal.Geometry
			if trimRegion, err = buildPolygon(st, np); err != nil {
				return
			}
			defer trimRegion.Destroy()
			if trimRegion.IsSimple() {
				geo = geo.Difference(buffedLine)
				if geo.Type() == gdal.GT_MultiPolygon {
					ng := geo.GeometryCount()
					for i := 0; i < ng; {
						if trimRegion.Intersects(geo.Geometry(i)) {
							if err = geo.RemoveGeometry(i, true); err != nil {
								geo.Destroy()
								return
							}
							ng--
							continue
						}
						i++
					}
				}
			} else {
				if geo, err = removeSmallerPolygons(geo, buffedLine); err != nil {
					return
				}
			}
		}
	} else {
		err = ErrWrongPositionedLine
		return
	}
	out, err = simplifyMultiPolygon(geo)
	return
}

func (g *GdalToolbox) Reshape2(wkt, line string) (out string, err error) {
	geo, st, np, err := g.parseAndCheck(wkt, line)
	if err != nil {
		return
	}
	defer geo.Destroy()
	defer st.Destroy()

	if !geo.Intersects(st) {
		if np == 2 {
			out = wkt
		} else {
			var subRegion gdal.Geometry
			if subRegion, err = buildPolygon(st, np); err != nil {
				return
			}
			defer subRegion.Destroy()
			geo = geo.Difference(subRegion)
			defer geo.Destroy()
			out, err = geo.ToWKT()
		}
		return
	}

	ends := st.Boundary()
	defer ends.Destroy()

	isRing := ends.IsEmpty()

	if isRing || geo.Intersects(ends.Geometry(0)) && geo.Intersects(ends.Geometry(1)) {
		if geo.Contains(st) {
			if np == 2 {
				out = wkt
				return
			}
			var subRegion gdal.Geometry
			if subRegion, err = buildPolygon(st, np); err != nil {
				return
			}
			defer subRegion.Destroy()
			geo = geo.Difference(subRegion)
		} else {
			buffedLine := st.Buffer(MergeLineBuffDist, 1)
			defer buffedLine.Destroy()
			geo = geo.Union(buffedLine)
			switch geo.Type() {
			case gdal.GT_Polygon:
			case gdal.GT_MultiPolygon:
				ng := geo.GeometryCount()
				log.Info(g.logTag+"got multi polygon in line merge", zap.Int("ng", ng))
				defer geo.Destroy()
				geo = geo.Geometry(0)
			default:
				err = ErrGdalWrongGeoType
				return
			}
			if err = removeConcatHolesInPolygon(geo, buffedLine); err != nil {
				return
			}
		}
	} else if geo.Disjoint(ends) {
		if np <= 3 {
			buffedLine := st.Buffer(CutLineBuffDist, 1)
			defer buffedLine.Destroy()
			if geo, err = removeSmallerPolygons(geo, buffedLine); err != nil {
				return
			}
		} else {
			var trimRegion gdal.Geometry
			if trimRegion, err = buildPolygon(st, np); err != nil {
				return
			}
			defer trimRegion.Destroy()
			if trimRegion.IsSimple() {
				geo = geo.Difference(trimRegion)
			} else {
				buffedLine := st.Buffer(CutLineBuffDist, 1)
				defer buffedLine.Destroy()
				if geo, err = removeSmallerPolygons(geo, buffedLine); err != nil {
					return
				}
			}
		}
	} else {
		err = ErrWrongPositionedLine
		return
	}
	out, err = simplifyMultiPolygon2(geo)
	return
}

func removeSmallerPolygons(geo, line gdal.Geometry) (ret gdal.Geometry, err error) {
	var (
		subGeo  gdal.Geometry
		subGNum int
		ssGeo   gdal.Geometry
	)
	switch geo.Type() {
	case gdal.GT_Polygon:
		subGeo = geo.Difference(line)
		subGNum = subGeo.GeometryCount()
		if subGeo.Type() != gdal.GT_MultiPolygon || subGNum == 0 {
			ret = subGeo
			return
		}
		ssGeo = subGeo.Geometry(0)
		for j := 1; j < subGNum; j++ {
			if subGeo.Geometry(j).Area() > ssGeo.Area() {
				ssGeo = subGeo.Geometry(j)
			}
		}
		ret = ssGeo.Clone()
		subGeo.Destroy()
	case gdal.GT_MultiPolygon:
		ret = gdal.Create(gdal.GT_MultiPolygon)
		gNum := geo.GeometryCount()
		for i := 0; i < gNum; i++ {
			subGeo = geo.Geometry(i).Difference(line)
			switch subGeo.Type() {
			case gdal.GT_Polygon:
				if err = ret.AddGeometryDirectly(subGeo); err != nil {
					subGeo.Destroy()
					ret.Destroy()
					return
				}
			case gdal.GT_MultiPolygon:
				defer subGeo.Destroy()
				subGNum = subGeo.GeometryCount()
				if subGNum == 0 {
					continue
				}
				ssGeo = subGeo.Geometry(0)
				for j := 1; j < subGNum; j++ {
					if subGeo.Geometry(j).Area() > ssGeo.Area() {
						ssGeo = subGeo.Geometry(j)
					}
				}
				if err = ret.AddGeometryDirectly(ssGeo); err != nil {
					ret.Destroy()
					return
				}
			}
		}
	}
	return
}

// func calcPointDist(x1, y1, x2, y2 float64) float64 {
// 	return math.Abs(x2-x1) + math.Abs(y2-y1)
// }

func simplifyMultiPolygon(geo gdal.Geometry) (wkt string, err error) {
	defer geo.Destroy()
	if geo.Type() == gdal.GT_MultiPolygon {
		switch geo.GeometryCount() {
		case 0:
			geo = gdal.Create(gdal.GT_Polygon)
			defer geo.Destroy()
		case 1:
			geo = geo.Geometry(0)
		}
	}
	wkt, err = geo.ToWKT()
	return
}

func simplifyMultiPolygon2(geo gdal.Geometry) (wkt string, err error) {
	defer geo.Destroy()
	if geo.Type() == gdal.GT_MultiPolygon {
		switch geo.GeometryCount() {
		case 0:
			geo = gdal.Create(gdal.GT_Polygon)
			defer geo.Destroy()
		default:
			geo = geo.Geometry(0)
		}
	}
	wkt, err = geo.ToWKT()
	return
}

func buildPolygon(line gdal.Geometry, np int) (ret gdal.Geometry, err error) {
	var (
		x, y float64
		ring = gdal.Create(gdal.GT_LinearRing)
	)
	for i := 0; i < np; i++ {
		x, y, _ = line.Point(i)
		ring.AddPoint2D(x, y)
	}
	if !ring.IsRing() {
		x, y, _ = ring.Point(0)
		ring.AddPoint2D(x, y)
	}
	ret = gdal.Create(gdal.GT_Polygon)
	if err = ret.AddGeometryDirectly(ring); err != nil {
		ring.Destroy()
		ret.Destroy()
	}
	return
}

func removeHolesInPolygon(geo gdal.Geometry) (err error) {
	gNum := geo.GeometryCount()
	// if gNum <= 1 {
	// 	ret = geo
	// 	return
	// }
	for i := 1; i < gNum; i++ {
		if err = geo.RemoveGeometry(1, true); err != nil {
			return
		}
	}
	// rings := geo.Boundary()
	// if rings.GeometryCount() == 0 {
	// 	ret = geo
	// 	return
	// }
	// ret = gdal.Create(gdal.GT_Polygon)
	// switch rings.Type() {
	// case gdal.GT_MultiLineString:
	// 	err = ret.AddGeometryDirectly(transClosedLineToRing(rings.Geometry(0)))
	// case gdal.GT_LineString:
	// 	err = ret.AddGeometry(transClosedLineToRing(rings))
	// case gdal.GT_LinearRing:
	// 	err = ret.AddGeometryDirectly(rings)
	// default:
	// 	err = ErrGdalWrongGeoType
	// }
	return
}

func removeConcatHolesInPolygon(geo, line gdal.Geometry) (err error) {
	ng := geo.GeometryCount()
	for i := 1; i < ng; {
		if line.Intersects(geo.Geometry(i)) {
			if err = geo.RemoveGeometry(i, true); err != nil {
				geo.Destroy()
				return
			}
			ng--
			continue
		}
		i++
	}
	return
}

// func transClosedLineToRing(geo gdal.Geometry) (ret gdal.Geometry) {
// 	ret = gdal.Create(gdal.GT_LinearRing)
// 	np := geo.PointCount()
// 	var x, y float64
// 	for i := 0; i < np; i++ {
// 		x, y, _ = geo.Point(i)
// 		ret.AddPoint2D(x, y)
// 	}
// 	return
// }

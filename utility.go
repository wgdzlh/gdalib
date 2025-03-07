package gdalib

import (
	"fmt"
	"math"

	gdal "github.com/airbusgeo/godal"
)

const (
	degToRad = math.Pi / 180

	xr = 20037508.34 / 180
	yr = xr / degToRad
	tr = degToRad / 2
)

func PointsToWkt(lon1, lon2, lat1, lat2 float64) string {
	return fmt.Sprintf("POLYGON((%[1]f %[3]f, %[1]f %[4]f, %[2]f %[4]f, %[2]f %[3]f, %[1]f %[3]f))", lon1, lon2, lat1, lat2)
}

func SpanToWkt(span [4]float64) string {
	return PointsToWkt(span[0], span[1], span[2], span[3])
}

func Convert4326To3857(lon, lat float64) (lonIn3857, latIn3857 float64) {
	lonIn3857 = lon * xr
	latIn3857 = math.Log(math.Tan((90+lat)*tr)) * yr
	return
}

func Convert3857To4326(lonIn3857, latIn3857 float64) (lon, lat float64) {
	lon = lonIn3857 / xr
	lat = math.Atan(math.Pow(math.E, latIn3857/yr))/tr - 90
	return
}

func LonLatIn3857ToMeteoGridIdx(lonIn3857, latIn3857 float64) (idx int32) {
	x := int32((lonIn3857 - 8133511) / 1001.277)
	y := int32((7188255 - latIn3857) / 1001.277)
	if x >= 0 && x < METEO_TIF_X && y >= 0 && y < METEO_TIF_Y {
		idx = y*METEO_TIF_X + x
	} else {
		idx = -1
	}
	return
}

func MeteoGridIdxToLonLatIn3857(idx int32) (lonIn3857, latIn3857 float64) {
	if idx < 0 || idx >= METEO_TIF_X*METEO_TIF_Y {
		return
	}
	x := idx % METEO_TIF_X
	y := idx / METEO_TIF_X
	lonIn3857 = float64(x)*1001.277 + 8133511
	latIn3857 = 7188255 - float64(y)*1001.277
	return
}

func SpanIn4326ToMeteoGridIds(span [4]float64) (ids []int32) {
	span[0], span[2] = Convert4326To3857(span[0], span[2])
	span[1], span[3] = Convert4326To3857(span[1], span[3])

	first := LonLatIn3857ToMeteoGridIdx(span[0], span[3])
	if first < 0 {
		return
	}
	last := LonLatIn3857ToMeteoGridIdx(span[1], span[2])
	if last < 0 || last < first {
		return
	}
	minX := first % METEO_TIF_X
	maxX := last % METEO_TIF_X
	if maxX < minX {
		return
	}
	spanX := maxX - minX + 1
	jumpX := METEO_TIF_X - spanX
	var j int32 = 0
	for i := first; i <= last; {
		if j < spanX {
			ids = append(ids, i)
			i++
			j++
		} else {
			i += jumpX
			j = 0
		}
	}
	return
}

func SpanIn4326ToMeteoGridIdSpans(span [4]float64) (ids [][2]int32) {
	span[0], span[2] = Convert4326To3857(span[0], span[2])
	span[1], span[3] = Convert4326To3857(span[1], span[3])

	first := LonLatIn3857ToMeteoGridIdx(span[0], span[3])
	if first < 0 {
		return
	}
	last := LonLatIn3857ToMeteoGridIdx(span[1], span[2])
	if last < 0 || last < first {
		return
	}
	minX := first % METEO_TIF_X
	maxX := last % METEO_TIF_X
	if maxX < minX {
		return
	}
	diffX := maxX - minX
	jumpX := METEO_TIF_X - diffX
	n := 0
	for i := first; i <= last; {
		ids = append(ids, [2]int32{i, last})
		i += diffX
		ids[n][1] = i
		n++
		i += jumpX
	}
	return
}

func MergeMultiPolygons(gs ...*gdal.Geometry) (out *gdal.Geometry, err error) {
	out = &gdal.Geometry{}
	out.ForceToMultiPolygon()
	var sub *Geometry
	for _, g := range gs {
		switch g.Type() {
		case gdal.GTPolygon:
			if err = out.AddGeometry(g); err != nil {
				return
			}
			continue
		case gdal.GTMultiPolygon:
			for i, pn := 0, g.GeometryCount(); i < pn; i++ {
				if sub, err = g.SubGeometry(0); err != nil {
					return
				}
				if err = out.AddGeometry(sub); err != nil {
					return
				}
			}
		}
		g.Close()
	}
	return
}

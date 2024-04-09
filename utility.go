package gdalib

import (
	"fmt"
	"math"
)

const (
	degToRad = math.Pi / 180
)

func PointsToWkt(lon1, lon2, lat1, lat2 float64) string {
	return fmt.Sprintf("POLYGON((%[1]f %[3]f, %[1]f %[4]f, %[2]f %[4]f, %[2]f %[3]f, %[1]f %[3]f))", lon1, lon2, lat1, lat2)
}

func SpanToWkt(span [4]float64) string {
	return PointsToWkt(span[0], span[1], span[2], span[3])
}

func Convert4326To3857(lon, lat float64) (lonIn3857, latIn3857 float64) {
	const xr = 20037508.34 / 180
	const yr = xr / degToRad
	const tr = degToRad / 2

	lonIn3857 = lon * xr
	latIn3857 = math.Log(math.Tan((90+lat)*tr)) * yr
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

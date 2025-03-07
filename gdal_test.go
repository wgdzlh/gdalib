package gdalib

import (
	"strings"
	"testing"
	"time"
)

func TestReadMeteoTif(t *testing.T) {
	g := NewGdalToolbox()
	if g == nil {
		t.Fatal()
	}
	buf := make([]int16, METEO_TIF_X*METEO_TIF_Y)
	err := g.ParseMeteoRaster("/mnt/c/Users/wgdzlh/Desktop/MidForecast_2022092400/Mid_Precip_2022092400_2022092406.tiff", buf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("read tif done: head %v, tail %v", buf[:10], buf[len(buf)-10:])
}

func TestTrans(t *testing.T) {
	g := NewGdalToolbox()
	if g == nil {
		t.Fatal()
	}
	span := [4]float64{113.695688629, 115.075725846, 29.971802123, 31.360788281}
	wkt := SpanToWkt(span)
	ret, err := g.TransformWkt(wkt, 4326, 3857)
	t.Log(ret, err)
	span, err = g.GetWktSpan(ret, 3857)
	t.Log(span, err)
	t.Log(Convert4326To3857(113.695688629, 29.971802123))
	t.Log(Convert4326To3857(115.075725846, 31.360788281))
	x1, y1 := Convert4326To3857(113.695688629, 31.360788281)
	x2, y2 := Convert4326To3857(115.075725846, 29.971802123)
	t.Log(LonLatIn3857ToMeteoGridIdx(x1, y1))
	t.Log(LonLatIn3857ToMeteoGridIdx(x2, y2))
}

func TestDemo(t *testing.T) {
	extent := PointsToWkt(8133511.7542897, 15081375.1438741, 1970599.25164441, 7188255.41580702)
	t.Log(extent)
}

func TestSpan(t *testing.T) {
	idSpans := SpanIn4326ToMeteoGridIdSpans([4]float64{114.45427660701012, 114.49117701581883, 22.583111358451404, 22.613144370489223})
	t.Log(idSpans)
}

func TestEmpty(t *testing.T) {
	g := NewGdalToolbox()
	ref, _ := g.getSridRef(UNIVERSAL_SRID)
	geo, _ := g.GetEmptyMultiPolygon(ref)
	wkt, _ := geo.WKT()
	t.Log(wkt, strings.HasSuffix(wkt, "EMPTY"))
	geo, _ = g.GetEmptyPolygon(ref)
	wkt, _ = geo.WKT()
	t.Log(wkt, strings.HasSuffix(wkt, "EMPTY"))
}

func TestReshape2(t *testing.T) {
	g := NewGdalToolbox()
	if g == nil {
		t.Fatal()
	}
	out, err := g.Reshape2("POLYGON((12989076.3688183 4330113.17210957,12989482.4405311 4330113.17210957,12989482.4405311 4329714.26636821,12989076.3688183 4329714.26636821,12989076.3688183 4330113.17210957),(12989183.8583893 4329874.30639618,12989322.4005031 4329816.97862497,12989401.2261885 4329974.62999581,12989234.4544904 4330004.41065618,12989183.8583893 4329874.30639618))", "LINESTRING(12989482.4405311 4330113.17210957,12989749.372965792 4329914.913567453,12989482.4405311 4329714.26636821)")
	t.Log(err)
	t.Log(out)

	out, err = g.Reshape2("POLYGON((0 0,0 2,2 2,2 0,0 0))", "LINESTRING(0 3,3 0)")
	t.Log(err)
	t.Log(out)

	gs1, _ := g.parseAlgWKT("POLYGON((0 0,0 2,2 2,2 0,0 0))")
	gs2, _ := g.parseAlgWKT("MULTIPOLYGON(((0 0,0 2,2 2,2 0,0 0)),((0 0,0 3,3 3,3 0,0 0)),((0 0,0 3,3 3,3 0,0 0)))")
	ret, err := MergeMultiPolygons(gs1, gs2)
	out, _ = ret.WKT()
	t.Log(err, out)

	ret2, _ := ret.Union(ret)
	out, _ = ret2.WKT()
	t.Log(out)

	ret.Close()
	ret2.Close()

	time.Sleep(time.Second)
	t.Log("all done")
}

func TestTransIdxToLonLat(t *testing.T) {
	a := int32(15987040)
	lon, lat := MeteoGridIdxToLonLatIn3857(a)
	t.Logf("%f %f", lon, lat)

	lon, lat = Convert3857To4326(lon, lat)
	t.Logf("%.10f,%.10f", lon, lat)
}

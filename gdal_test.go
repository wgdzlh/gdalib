package gdalib

import "testing"

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

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

package gdalib

import "errors"

var (
	ErrGdalDriverCreate    = errors.New("gdal driver create err")
	ErrGdalDriverOpen      = errors.New("gdal driver open err")
	ErrGdalEmptyShp        = errors.New("gdal shp is empty")
	ErrVoidSrid            = errors.New("gdal shp with void srid")
	ErrGdalDriverCount     = errors.New("gdal driver count err")
	ErrGdalWrongGeoType    = errors.New("gdal wrong geo type")
	ErrGdalWrongGeoJSON    = errors.New("gdal wrong GeoJSON")
	ErrWrongPositionedLine = errors.New("wrong positioned line")
	ErrNotEnoughLinePoints = errors.New("not enough line points")
	ErrEmptyCutEdge        = errors.New("cut edge is empty")
	ErrWrongLineEndsCount  = errors.New("wrong line ends count")
	ErrInvalidWKT          = errors.New("invalid WKT")
	ErrWrongBufferSize     = errors.New("wrong buffer size")
	ErrInvalidTif          = errors.New("invalid tif")
	ErrWrongTif            = errors.New("wrong tif")
	ErrEmptyTif            = errors.New("empty tif")
	ErrTifReadFailed       = errors.New("failed to read tif band")
	ErrWrongRasterOffset   = errors.New("wrong raster offset")
)

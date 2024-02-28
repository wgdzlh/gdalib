package gdalib

const (
	FILE_EXT_SHP    = ".shp"
	FILE_EXT_CPG    = ".cpg"
	FILE_EXT_JSON   = ".json"
	SHAPE_ENCODING  = "UTF-8"
	UTF8_ENC        = "UTF8"
	ZH_ENC          = "GBK"
	SHP_DRIVER_NAME = "ESRI Shapefile"
	ENCODING_OPTION = "ENCODING=" + SHAPE_ENCODING
	OO_ENCODING     = "ENCODING=" + ZH_ENC
	UNIVERSAL_SRID  = 4326
	GEOJSON_SRID    = 4326
	OUTPUT_SRID     = 4490
	WKT_ALG_SRID    = 3857

	ErrColumnMissingTemplate = `shp文件中缺失【%s】字段`
	ErrColumnEmptyTemplate   = `shp文件图斑中【%s】字段为空`

	MergeBufferDistance = 0.005
	MergeBufferMeter    = 0.00001
	MergeBufferSegs     = 24
	CoverageThreshold   = 0.9999

	SimplifyT = 1.0

	SHP_FIELD_UID = "uid"
	SHP_FIELD_OID = "oid"

	TMP_GEOJSON = "geo_%s.json"
)

package gdalib

import "encoding/json"

type LabelCode = [2]string // [label,code]

type AlignedLabel = map[string]LabelCode

type AnyJson = json.RawMessage

type GdalGeo = []byte

// 图斑矢量
type Speckle struct {
	Geom      GdalGeo // 图斑的矢量面WKB
	ClassName string  // 标签名
}

// 区域矢量
type Uncertainty struct {
	Id   int
	Geom GdalGeo
}

type ImgMergeFile struct {
	Infile    string `json:"infile"`     // 镶嵌影像
	BandOrder string `json:"band_order"` // 波段顺序
	Wkt       string `json:"wkt"`        // 镶嵌影像有效范围
}

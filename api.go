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

type TideSpeckle struct {
	Id    string // 图斑ID
	Label string // 标签名
	Geom  string // 图斑的矢量面
}

type TideSpan struct {
	SiteId string `json:"site_id"` // 潮汐站点ID
	Wkt    string `json:"wkt"`     // 潮汐打标WKT
}

type InlayShpGeo struct {
	Tif string
	Geo GdalGeo
}

// 区域矢量
type Uncertainty struct {
	Id   int
	Geom GdalGeo
}

type ImgMergeFile struct {
	Infile    string `json:"infile"`     // 镶嵌影像
	BandOrder string `json:"band_order"` // 波段顺序
	Wkb       []byte `json:"wkb"`        // 镶嵌影像有效范围
}

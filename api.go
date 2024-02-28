package gdalib

import "encoding/json"

type Destroyable interface {
	Destroy()
}

type Speckle struct {
	Uid       string `gorm:"-"`
	Geom      string `gorm:"type:geometry;column:GEOM"`    // 图斑的矢量面
	ClassName string `gorm:"type:varchar(60);column:YBBQ"` // 标签名
	Pid       uint64 `gorm:"column:PID"`
}

type Uncertainty struct {
	Fid  int
	Geom string
}

type LabelCode = [2]string // [label,code]

type AlignedLabel = map[string]LabelCode

type AnyJson = json.RawMessage

type GdalGeo = []byte

type InlayShpGeo struct {
	Tif string
	Geo GdalGeo
}

type ImgMergeFile struct {
	Infile    string `json:"infile"`     // 镶嵌影像
	BandOrder string `json:"band_order"` // 波段顺序
	Wkt       string `json:"wkt"`        // 镶嵌影像有效范围
}

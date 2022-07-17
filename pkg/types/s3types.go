package types

type S3Object struct {
	Bucket    string
	Key       string
	VersionId string
}
type ListQuery struct {
	Version         int
	Prefix          string
	Delimiter       string
	Marker          string
	MaxKeys         int64
	KeyMarker       string // only works with `VersionIdMarker`
	VersionIdMarker string
}
type MultipartQuery struct {
	Uploads        bool
	UploadId       string
	PartNumber     int64
	MaxParts       int64
	Marker         int64
	KeyMarker      string
	UploadIdMarker string
	MaxUploads     int64
}

type S3Query struct {
	Type          S3Operation
	DstObj        S3Object
	SrcObj        S3Object
	MpQuery       MultipartQuery
	ListQuery     ListQuery
	BatchDelQuery bool
}

func (q S3Query) HasCopy() bool {
	return q.SrcObj.Bucket != "" && q.SrcObj.Key != ""
}

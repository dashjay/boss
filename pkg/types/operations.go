package types

type RequestType uint32
type S3Operation uint32

const (
	AdminBucketReq RequestType = iota
	ListBucketsReq
	ReadBucketReq
	WriteBucketReq
	InvalidReq
)
const (
	PutBucket S3Operation = 100*S3Operation(AdminBucketReq) + iota
	DeleteBucket
)
const (
	ListBuckets S3Operation = 100*S3Operation(ListBucketsReq) + iota
	HeadBucket
)
const (
	GetBucket S3Operation = 100*S3Operation(ReadBucketReq) + iota
	GetObject
	HeadObject
	GetBucketVersions
)
const (
	PutObject = 100*S3Operation(WriteBucketReq) + iota
	CopyObject
	RemoveObject
	InitMultipartUpload
	MultipartUpload
	ListMultipartUpload
	CompleteMultipartUpload
	AbortMultipartUpload
	ListBucketMultiUploads
	DeleteObjects
)

const (
	NotImplementOperation = 100*S3Operation(InvalidReq) + iota
	ErrorOperation
)

var m = map[S3Operation]string{
	NotImplementOperation: "NotImplementOperation",
	ErrorOperation:        "ErrorOperation",

	PutObject:               "PutObject",
	CopyObject:              "CopyObject",
	RemoveObject:            "RemoveObject",
	InitMultipartUpload:     "InitMultipartUpload",
	MultipartUpload:         "MultipartUpload",
	ListMultipartUpload:     "ListMultipartUpload",
	CompleteMultipartUpload: "CompleteMultipartUpload",
	AbortMultipartUpload:    "AbortMultipartUpload",
	ListBucketMultiUploads:  "ListBucketMultiUploads",
	DeleteObjects:           "DeleteObjects",

	GetBucket:         "GetBucket",
	GetObject:         "GetObject",
	HeadObject:        "HeadObject",
	GetBucketVersions: "GetBucketVersions",

	ListBuckets: "ListBuckets",
	HeadBucket:  "HeadBucket",

	PutBucket:    "PutBucket",
	DeleteBucket: "DeleteBucket",
}

func (s3 S3Operation) String() string {
	return m[s3]
}

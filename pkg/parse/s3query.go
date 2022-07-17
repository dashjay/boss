package parse

import (
	"github.com/dashjay/overlay_oss/pkg/types"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var (
	multipartRelatedOp = map[types.S3Operation]bool{
		types.InitMultipartUpload:     true,
		types.CompleteMultipartUpload: true,
		types.RemoveObject:            true,
		types.PutObject:               true,
	}
)

func IsMultipartRelatedOp(op types.S3Operation) bool {
	_, isRelated := multipartRelatedOp[op]
	return isRelated
}

const (
	Delimiter         = "delimiter"
	Prefix            = "prefix"
	Marker            = "marker"
	KeyMarker         = "key-marker"
	VersionIdMarker   = "version-id-marker"
	ListType          = "list-type"
	ListTypeV2        = "2"
	StartAfter        = "start-after"
	ContinuationToken = "continuation-token"
	PartNumber        = "part-number"
	PartNumberMarker  = "part-number-marker"
	MaxUploads        = "max-uploads"
	MaxKeys           = "max-keys"
	MaxParts          = "max-parts"
	Uploads           = "uploads"
	UploadId          = "uploadId"
	UploadIdMarker    = "upload-id-marker"
	Delete            = "delete"

	// Did not implement
	Acl        = "acl"
	Lifecycle  = "lifecycle"
	Policy     = "policy"
	Tagging    = "tagging"
	Versioning = "versioning"
)

func S3Query(r *http.Request) (q types.S3Query) {
	// path2BucketAndObject Copy from https://github.com/minio/minio/blob/master/cmd/handler-utils.go
	path2BucketAndObject := func(path string) (bucket, object string) {
		// Skip the first element if it is '/', split the rest.
		path = strings.TrimPrefix(path, "/")
		pathComponents := strings.SplitN(path, "/", 2)
		// Save the bucket and object extracted from path.
		switch len(pathComponents) {
		case 1:
			bucket = pathComponents[0]
		case 2:
			bucket = pathComponents[0]
			object = pathComponents[1]
		}
		return bucket, object
	}
	bucket, object := path2BucketAndObject(r.URL.Path)
	query := r.URL.Query()
	parseIntFromQuery := func(key string, into *int64, defalt int64) {
		v := query.Get(key)
		if v == "" {
			*into = defalt
			return
		}
		k, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			*into = defalt
			log.WithError(err).WithField(key, v).Warn("Parse failed")
			return
		}
		*into = k
	}
	inQuery := func(key string) bool {
		_, ok := query[key]
		return ok
	}

	anyInQuery := func(keys ...string) bool {
		for _, key := range keys {
			_, ok := query[key]
			if ok {
				return true
			}
		}
		return false
	}

	q.ListQuery.Version = 1
	q.ListQuery.Delimiter, q.ListQuery.Prefix, q.ListQuery.Marker, q.ListQuery.KeyMarker, q.ListQuery.VersionIdMarker =
		query.Get(Delimiter), query.Get(Prefix), query.Get(Marker), query.Get(KeyMarker), query.Get(VersionIdMarker)

	if query.Get(ListType) == ListTypeV2 {
		q.ListQuery.Version = 2

		q.ListQuery.Marker = query.Get(StartAfter)
		if token := query.Get(ContinuationToken); token != "" {
			q.ListQuery.Marker = token
		}
	}
	q.ListQuery.MaxKeys = 1000
	parseIntFromQuery(MaxKeys, &q.ListQuery.MaxKeys, 1000)

	q.MpQuery.Uploads = inQuery(Uploads)

	q.MpQuery.UploadId, q.MpQuery.KeyMarker, q.MpQuery.UploadIdMarker =
		query.Get(UploadId), query.Get(KeyMarker), query.Get(UploadIdMarker)

	parseIntFromQuery(MaxUploads, &q.MpQuery.MaxUploads, 1000)
	parseIntFromQuery(PartNumberMarker, &q.MpQuery.Marker, 1000)
	parseIntFromQuery(PartNumber, &q.MpQuery.PartNumber, 0)

	parseIntFromQuery(MaxParts, &q.MpQuery.MaxParts, 0)

	// Check for batch delete.
	q.BatchDelQuery = inQuery(Delete)

	q.DstObj.Bucket = bucket
	if anyInQuery(Acl, Lifecycle, Policy, Tagging, Versioning) {
		q.Type = types.NotImplementOperation
		return
	}
	if object == "" {
		if bucket == "" {
			q.Type = types.ListBuckets
			return
		}
		switch r.Method {
		case http.MethodGet:
			if q.MpQuery.Uploads {
				q.Type = types.ListBucketMultiUploads
				return
			}
			_, ok := query["versions"]
			if ok {
				q.Type = types.GetBucketVersions
			} else {
				q.Type = types.GetBucket
			}
			return
		case http.MethodDelete:
			q.Type = types.DeleteBucket
			return
		case http.MethodPut:
			q.Type = types.PutBucket
			return
		case http.MethodHead:
			q.Type = types.HeadBucket
			return
		case http.MethodPost:
			if q.BatchDelQuery {
				q.Type = types.DeleteObjects
				return
			}
			q.Type = types.NotImplementOperation
			return
		default:
			q.Type = types.NotImplementOperation
			return
		}
	}
	q.DstObj.Key = object
	if versionId, ok := query["versionId"]; ok {
		q.DstObj.VersionId = versionId[0]
	}
	switch r.Method {
	case http.MethodGet:
		if q.MpQuery.UploadId != "" {
			q.Type = types.ListMultipartUpload
			return
		}
		q.Type = types.GetObject
		return
	case http.MethodPut:
		if v := r.Header.Get("x-amz-copy-source"); v != "" {
			src, err := url.QueryUnescape(v)
			if err != nil {
				// Save unescaped string as is.
				log.WithError(err).Warning("QueryUnescape failed")
				src = v
			}
			q.SrcObj.Bucket, q.SrcObj.Key = path2BucketAndObject(src)
			if object == "" {
				q.Type = types.ErrorOperation
				return
			}
		}
		if q.MpQuery.UploadId != "" {
			q.Type = types.MultipartUpload
			return
		}
		q.Type = types.PutObject
		return
	case http.MethodDelete:
		if q.MpQuery.UploadId != "" {
			q.Type = types.AbortMultipartUpload
			return
		}
		q.Type = types.RemoveObject
		return
	case http.MethodHead:
		q.Type = types.HeadObject
		return
	case http.MethodPost:
		if q.MpQuery.UploadId != "" {
			q.Type = types.CompleteMultipartUpload
			return
		}
		if q.MpQuery.Uploads {
			q.Type = types.InitMultipartUpload
			return
		}
		q.Type = types.NotImplementOperation
		return
	default:
		q.Type = types.NotImplementOperation
		return
	}
}

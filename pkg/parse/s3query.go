package parse

import (
	"encoding/xml"
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

func anyInQuery(query url.Values, keys ...string) bool {
	for _, key := range keys {
		_, ok := query[key]
		if ok {
			return true
		}
	}
	return false
}

// Copy from https://github.com/minio/minio/blob/master/cmd/handler-utils.go
func path2BucketAndObject(path string) (bucket, object string) {
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
func ParseS3Query(r *http.Request) (q types.S3Query) {
	log.Debugln(r.Method, r.URL)
	log.Debugln(r.Header)
	bucket, object := path2BucketAndObject(r.URL.Path)
	query := r.URL.Query()
	q.ListQuery.Version = 1
	q.ListQuery.Delimiter = query.Get("delimiter")
	q.ListQuery.Prefix = query.Get("prefix")
	q.ListQuery.Marker = query.Get("marker")
	q.ListQuery.KeyMarker = query.Get("key-marker")
	q.ListQuery.VersionIdMarker = query.Get("version-id-marker")
	if query.Get("list-type") == "2" {
		q.ListQuery.Version = 2
		startAfter := query.Get("start-after")
		token := query.Get("continuation-token")
		if token == "" {
			q.ListQuery.Marker = startAfter
		} else {
			q.ListQuery.Marker = token
		}
	}
	q.ListQuery.MaxKeys = 1000
	if v := query.Get("max-keys"); v != "" {
		k, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.WithError(err).WithField("maxkeys", v).Warn("Parse maxkeys failed")
		} else {
			q.ListQuery.MaxKeys = int64(k)
		}
	}
	if _, ok := query["uploads"]; ok {
		q.MpQuery.Uploads = true
	}
	q.MpQuery.UploadId = query.Get("uploadId")
	q.MpQuery.KeyMarker = query.Get("key-marker")
	q.MpQuery.UploadIdMarker = query.Get("upload-id-marker")
	q.MpQuery.MaxUploads = 1000
	q.MpQuery.MaxParts = 1000
	if v := query.Get("max-uploads"); v != "" {
		k, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.WithError(err).WithField("maxuploads", v).Warn("Parse maxuploads failed")
		} else {
			q.MpQuery.MaxUploads = int64(k)
		}
	}
	if v := query.Get("part-number-marker"); v != "" {
		k, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.WithError(err).WithField("part-number-marker", v).Warn("Parse part-number-marker failed")
		} else {
			q.MpQuery.Marker = int64(k)
		}
	}
	if v := query.Get("partNumber"); v != "" {
		k, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.WithError(err).WithField("partNumber", v).Warn("Parse partNumber failed")
		} else {
			q.MpQuery.PartNumber = int(k)
		}
	}
	if v := query.Get("max-parts"); v != "" {
		k, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.WithError(err).WithField("max-parts", v).Warn("Parse max-parts failed")
		} else {
			q.MpQuery.MaxParts = int64(k)
		}
	}
	// Check for batch delete.
	if _, ok := query["delete"]; ok {
		log.Debug("delete flag detected")
		q.BatchDelQuery = true
	}
	q.DstObj.Bucket = bucket
	if anyInQuery(query, "acl", "lifecycle", "policy", "tagging", "versioning") {
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
func IsValidKey(key string) bool {
	bs, err := xml.Marshal(&key)
	if err != nil {
		log.WithError(err).WithField("key", key).Warn("Marshal key failed")
		return false
	}
	var reKey string
	err = xml.Unmarshal(bs, &reKey)
	if err != nil {
		log.WithError(err).WithField("key", key).WithField("bytes", bs).Warn("Unmarshal key failed")
		return false
	}
	return key == reKey
}

package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dashjay/overlay_oss/pkg/parse"
	"github.com/dashjay/overlay_oss/pkg/s3error"
	"github.com/dashjay/overlay_oss/pkg/types"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Bucket struct {
	gorm.Model
	BucketName string `gorm:"column=bucket_name"`
}

type Object struct {
	gorm.Model
	BucketName string `gorm:"column=bucket_name"`
	KeyPrefix  string `gorm:"column=key_prefix"`
	Data       []byte `gorm:"column=data"`
}

type S3Proxy struct {
	DB  *gorm.DB
	mux map[types.S3Operation]func(s3query types.S3Query, wr http.ResponseWriter, r *http.Request)
}

func NewS3Proxy() *S3Proxy {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	logrus.Infoln("start migrating")
	// Migrate the schema
	db.AutoMigrate(&Bucket{})
	db.AutoMigrate(&Object{})

	logrus.Infoln("migrated")
	s3proxy := S3Proxy{DB: db}
	s3proxy.mux = map[types.S3Operation]func(s3query types.S3Query, wr http.ResponseWriter, r *http.Request){
		types.PutBucket:   s3proxy.CreateBucket,
		types.PutObject:   s3proxy.PutObject,
		types.HeadObject:  s3proxy.HeadObject,
		types.GetObject:   s3proxy.GetObject,
		types.GetBucket:   s3proxy.GetBucket,
		types.ListBuckets: s3proxy.ListBuckets,
	}
	return &s3proxy
}

func (a *S3Proxy) CreateBucket(s3query types.S3Query, wr http.ResponseWriter, r *http.Request) {
	bucket := s3query.DstObj.Bucket
	_, err := a.createBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		s3error.WriteError(r, wr, err)
		return
	}
}
func (a *S3Proxy) createBucket(input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	if aws.ToString(input.Bucket) == "" {
		return nil, s3error.S3Error{OriginError: nil, Code: s3error.ErrorCodeInvalidArgument}
	}
	var b Bucket
	res := a.DB.First(&b, "bucket_name = ?", aws.ToString(input.Bucket))
	if err := res.Error; err != nil {
		if res.Error != gorm.ErrRecordNotFound {
			return nil, err
		}
		a.DB.Create(&Bucket{BucketName: aws.ToString(input.Bucket)})
		out := &s3.CreateBucketOutput{Location: aws.String("/" + aws.ToString(input.Bucket))}
		return out, nil
	}
	return nil, s3error.S3Error{
		OriginError: nil,
		Code:        s3error.ErrorCodeBucketAlreadyExists,
	}
}

func (a *S3Proxy) PutObject(s3query types.S3Query, wr http.ResponseWriter, r *http.Request) {
	tempFile, err := ioutil.TempFile("", "temp-s3-object")
	if err != nil {
		s3error.WriteError(r, wr, err)
		return
	}
	defer os.Remove(tempFile.Name())
	io.Copy(tempFile, r.Body)
	tempFile.Sync()
	tempFile.Seek(0, io.SeekStart)
	_, err = a.putObject(&s3.PutObjectInput{
		Body:          tempFile,
		Bucket:        aws.String(s3query.DstObj.Bucket),
		Key:           aws.String(s3query.DstObj.Key),
		ContentLength: r.ContentLength,
	})
	if err != nil {
		s3error.WriteError(r, wr, err)
		return
	}
}
func (a *S3Proxy) putObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	var obj Object
	res := a.DB.First(&obj, "bucket_name = ? AND key_prefix = ?", input.Bucket, input.Key)
	if res.Error != nil {
		if res.Error != gorm.ErrRecordNotFound {
			return nil, res.Error
		}
	}
	obj.BucketName, obj.KeyPrefix = aws.ToString(input.Bucket), aws.ToString(input.Key)
	obj.Data = make([]byte, input.ContentLength)
	n, err := input.Body.Read(obj.Data)
	if err != nil {
		return nil, err
	}
	if n != int(input.ContentLength) {
		return nil, s3error.S3Error{OriginError: fmt.Errorf("content length is not equal to actual body length"), Code: s3error.ErrorCodeIncompleteBody}
	}
	a.DB.Save(&obj)
	return &s3.PutObjectOutput{}, nil
}

func (a *S3Proxy) HeadObject(s3query types.S3Query, wr http.ResponseWriter, r *http.Request) {
	output, err := a.getObject(&s3.GetObjectInput{
		Bucket: aws.String(s3query.DstObj.Bucket),
		Key:    aws.String(s3query.DstObj.Key),
	})
	if err != nil {
		s3error.WriteError(r, wr, err)
		return
	}
	wr.Header().Set("Last-Modified", output.LastModified.String())
	wr.Header().Set("Content-Length", strconv.Itoa(int(output.ContentLength)))
}

func (a *S3Proxy) GetObject(s3query types.S3Query, wr http.ResponseWriter, r *http.Request) {
	output, err := a.getObject(&s3.GetObjectInput{Bucket: aws.String(s3query.DstObj.Bucket), Key: aws.String(s3query.DstObj.Key)})
	if err != nil {
		s3error.WriteError(r, wr, err)
		return
	}
	n, _ := io.Copy(wr, output.Body)
	wr.Header().Set("Content-Length", strconv.Itoa(int(n)))
	return
}
func (a *S3Proxy) getObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	var obj Object
	res := a.DB.First(&obj, "bucket_name = ? AND key_prefix = ?", aws.ToString(input.Bucket), aws.ToString(input.Key))
	if err := res.Error; err != nil {
		if err != gorm.ErrRecordNotFound {
			return nil, err
		}
		return nil, s3error.S3Error{Code: s3error.ErrorCodeNoSuchKey}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewBuffer(obj.Data)),
		ContentLength: int64(len(obj.Data)),
		LastModified:  &obj.UpdatedAt,
	}, nil
}

func (a *S3Proxy) GetBucket(s3query types.S3Query, wr http.ResponseWriter, r *http.Request) {
	out, err := a.getBucket(&s3.ListObjectsV2Input{Bucket: aws.String(s3query.DstObj.Bucket)})
	if err != nil {
		s3error.WriteError(r, wr, err)
		return
	}
	bin, err := xml.Marshal(out)
	if err != nil {
		s3error.WriteError(r, wr, err)
		return
	}
	wr.Write(wrapXMLHeader(bin))
}
func (a *S3Proxy) getBucket(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	var objects []Object
	a.DB.Find(&objects, "bucket_name = ?", aws.ToString(input.Bucket))
	out := &s3.ListObjectsV2Output{Contents: make([]s3types.Object, len(objects))}
	for i := range objects {
		out.Contents[i] = s3types.Object{
			Key:          aws.String(objects[i].KeyPrefix),
			LastModified: &objects[i].UpdatedAt,
			Size:         int64(len(objects[i].Data)),
		}
	}
	return out, nil
}

func (a *S3Proxy) ListBuckets(s3query types.S3Query, wr http.ResponseWriter, r *http.Request) {
	buckets, err := a.listBuckets(&s3.ListBucketsInput{})
	if err != nil {
		s3error.WriteError(r, wr, err)
		return
	}
	bin, err := xml.Marshal(buckets)
	if err != nil {
		s3error.WriteError(r, wr, err)
		return
	}
	logrus.Infof("out: %s\n", bin)
	wr.Write(wrapXMLHeader(bin))
}
func (a *S3Proxy) listBuckets(input *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	var buckets []Bucket
	a.DB.Find(&buckets)
	var outBuckets = make([]s3types.Bucket, 0)
	outBuckets = append(outBuckets, s3types.Bucket{
		CreationDate: aws.Time(time.Now()),
		Name:         aws.String("today"),
	})

	for i := range buckets {
		outBuckets = append(outBuckets, s3types.Bucket{
			CreationDate: aws.Time(buckets[i].CreatedAt),
			Name:         aws.String(buckets[i].BucketName),
		})
	}
	return &s3.ListBucketsOutput{
		Buckets: outBuckets,
		Owner: &s3types.Owner{
			DisplayName: aws.String("dashjay"),
			ID:          aws.String("dashjay"),
		},
	}, nil
}

func (a *S3Proxy) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	query := parse.S3Query(r)
	a.ServeMux(query.Type)(query, wr, r)
}

var _ http.Handler = (*S3Proxy)(nil)

func (a *S3Proxy) ServeMux(s3Op types.S3Operation) func(s3query types.S3Query, wr http.ResponseWriter, r *http.Request) {
	logrus.Infoln("s3Op: ", s3Op.String())
	if handler, ok := a.mux[s3Op]; ok {
		return handler
	}
	return func(s3query types.S3Query, wr http.ResponseWriter, r *http.Request) {
		s3error.WriteError(r, wr, s3error.S3Error{OriginError: nil, Code: s3error.ErrorCodeNotImplemented})
	}
}

func main() {
	http.ListenAndServe(":8000", NewS3Proxy())
}

var wrapXMLHeader = func(body []byte) []byte {
	body = append([]byte(xml.Header), body...)
	return body
}

package s3error

import (
	"encoding/xml"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"strconv"
)

var wrapXMLHeader = func(body []byte) []byte {
	body = append([]byte(xml.Header), body...)
	return body
}

type ErrorCode string

type S3Error struct {
	OriginError error
	Code        ErrorCode
}

type ResponseError struct {
	Code      string
	Message   string
	Resource  string
	RequestId string
}

type errDetail struct {
	Detail         string
	httpStatusCode int
}

func (s S3Error) Error() string {
	if s.OriginError == nil {
		return s.Detail()
	}
	return s.OriginError.Error()
}

func (s *S3Error) GetCode() ErrorCode {
	return s.Code
}

func (s S3Error) Detail() string {
	return errMap[s.Code].Detail
}
func (s S3Error) HTTPStatusCode() int {
	return errMap[s.Code].httpStatusCode
}

func IsS3Error(err error, code ErrorCode) bool {
	var s3err *S3Error
	if errors.As(err, &s3err) {
		return s3err.GetCode() == code
	}
	return false
}

func IsNoSuchKey(err error) bool {
	return IsS3Error(err, ErrorCodeNoSuchKey)
}

func IsNotFound(err error) bool {
	return IsNoSuchKey(err) || IsS3Error(err, ErrorCodeNoSuchBucket)
}

var _ error = S3Error{}

func WriteError(r *http.Request, w http.ResponseWriter, err error) {
	var s3err = new(S3Error)
	var (
		s3Code   ErrorCode
		httpCode int
	)
	if errors.As(err, s3err) {
		s3Code, httpCode = s3err.GetCode(), s3err.HTTPStatusCode()
	} else {
		s3Code, httpCode = ErrorCodeInternalError, http.StatusInternalServerError
	}
	if httpCode == http.StatusMethodNotAllowed {
		w.Header().Set("Allow", "GET, HEAD")
	}
	w.Header().Set("Content-Type", "application/xml")
	id, _ := r.Context().Value("RequestId").(string)
	body, _ := xml.Marshal(&ResponseError{
		Code:      string(s3Code),
		Message:   err.Error(),
		Resource:  "",
		RequestId: id,
	})
	if s3Code == ErrorCodeInternalError {
		logrus.WithError(err).Errorln("request error")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(body)))
	w.WriteHeader(httpCode)
	if r.Method != http.MethodHead {
		_, err := io.MultiWriter(os.Stdout, w).Write(wrapXMLHeader(body))
		if err != nil {
			logrus.WithError(err).Errorln("write body error")
		}
	}
}

var (
	ErrorCodeAccessDenied                                   ErrorCode = "AccessDenied"
	ErrorCodeAccountProblem                                 ErrorCode = "AccountProblem"
	ErrorCodeAllAccessDisabled                              ErrorCode = "AllAccessDisabled"
	ErrorCodeAmbiguousGrantByEmailAddress                   ErrorCode = "AmbiguousGrantByEmailAddress"
	ErrorCodeAuthorizationHeaderMalformed                   ErrorCode = "AuthorizationHeaderMalformed"
	ErrorCodeBadDigest                                      ErrorCode = "BadDigest"
	ErrorCodeBucketAlreadyExists                            ErrorCode = "BucketAlreadyExists"
	ErrorCodeBucketAlreadyOwnedByYou                        ErrorCode = "BucketAlreadyOwnedByYou"
	ErrorCodeBucketNotEmpty                                 ErrorCode = "BucketNotEmpty"
	ErrorCodeCredentialsNotSupported                        ErrorCode = "CredentialsNotSupported"
	ErrorCodeCrossLocationLoggingProhibited                 ErrorCode = "CrossLocationLoggingProhibited"
	ErrorCodeEntityTooSmall                                 ErrorCode = "EntityTooSmall"
	ErrorCodeEntityTooLarge                                 ErrorCode = "EntityTooLarge"
	ErrorCodeExpiredToken                                   ErrorCode = "ExpiredToken"
	ErrorCodeIllegalVersioningConfigurationException        ErrorCode = "IllegalVersioningConfigurationException"
	ErrorCodeIncompleteBody                                 ErrorCode = "IncompleteBody"
	ErrorCodeIncorrectNumberOfFilesInPostRequest            ErrorCode = "IncorrectNumberOfFilesInPostRequest"
	ErrorCodeInlineDataTooLarge                             ErrorCode = "InlineDataTooLarge"
	ErrorCodeInternalError                                  ErrorCode = "InternalError"
	ErrorCodeInvalidAccessKeyId                             ErrorCode = "InvalidAccessKeyId"
	ErrorCodeInvalidAddressingHeader                        ErrorCode = "InvalidAddressingHeader"
	ErrorCodeInvalidArgument                                ErrorCode = "InvalidArgument"
	ErrorCodeInvalidBucketName                              ErrorCode = "InvalidBucketName"
	ErrorCodeInvalidBucketState                             ErrorCode = "InvalidBucketState"
	ErrorCodeInvalidDigest                                  ErrorCode = "InvalidDigest"
	ErrorCodeInvalidEncryptionAlgorithmError                ErrorCode = "InvalidEncryptionAlgorithmError"
	ErrorCodeInvalidLocationConstraint                      ErrorCode = "InvalidLocationConstraint"
	ErrorCodeInvalidObjectState                             ErrorCode = "InvalidObjectState"
	ErrorCodeInvalidPart                                    ErrorCode = "InvalidPart"
	ErrorCodeInvalidPartOrder                               ErrorCode = "InvalidPartOrder"
	ErrorCodeInvalidPayer                                   ErrorCode = "InvalidPayer"
	ErrorCodeInvalidPolicyDocument                          ErrorCode = "InvalidPolicyDocument"
	ErrorCodeInvalidRange                                   ErrorCode = "InvalidRange"
	ErrorCodeInvalidSecurity                                ErrorCode = "InvalidSecurity"
	ErrorCodeInvalidSOAPRequest                             ErrorCode = "InvalidSOAPRequest"
	ErrorCodeInvalidStorageClass                            ErrorCode = "InvalidStorageClass"
	ErrorCodeInvalidTargetBucketForLogging                  ErrorCode = "InvalidTargetBucketForLogging"
	ErrorCodeInvalidToken                                   ErrorCode = "InvalidToken"
	ErrorCodeInvalidURI                                     ErrorCode = "InvalidURI"
	ErrorCodeKeyTooLongError                                ErrorCode = "KeyTooLongError"
	ErrorCodeMalformedACLError                              ErrorCode = "MalformedACLError"
	ErrorCodeMalformedPOSTRequest                           ErrorCode = "MalformedPOSTRequest"
	ErrorCodeMalformedXML                                   ErrorCode = "MalformedXML"
	ErrorCodeMaxMessageLengthExceeded                       ErrorCode = "MaxMessageLengthExceeded"
	ErrorCodeMaxPostPreDataLengthExceededError              ErrorCode = "MaxPostPreDataLengthExceededError"
	ErrorCodeMetadataTooLarge                               ErrorCode = "MetadataTooLarge"
	ErrorCodeMethodNotAllowed                               ErrorCode = "MethodNotAllowed"
	ErrorCodeMissingAttachment                              ErrorCode = "MissingAttachment"
	ErrorCodeMissingContentLength                           ErrorCode = "MissingContentLength"
	ErrorCodeMissingRequestBodyError                        ErrorCode = "MissingRequestBodyError"
	ErrorCodeMissingSecurityElement                         ErrorCode = "MissingSecurityElement"
	ErrorCodeMissingSecurityHeader                          ErrorCode = "MissingSecurityHeader"
	ErrorCodeNoLoggingStatusForKey                          ErrorCode = "NoLoggingStatusForKey"
	ErrorCodeNoSuchBucket                                   ErrorCode = "NoSuchBucket"
	ErrorCodeNoSuchBucketPolicy                             ErrorCode = "NoSuchBucketPolicy"
	ErrorCodeNoSuchKey                                      ErrorCode = "NoSuchKey"
	ErrorCodeNoSuchLifecycleConfiguration                   ErrorCode = "NoSuchLifecycleConfiguration"
	ErrorCodeNoSuchUpload                                   ErrorCode = "NoSuchUpload"
	ErrorCodeNoSuchVersion                                  ErrorCode = "NoSuchVersion"
	ErrorCodeNotImplemented                                 ErrorCode = "NotImplemented"
	ErrorCodeNotSignedUp                                    ErrorCode = "NotSignedUp"
	ErrorCodeOperationAborted                               ErrorCode = "OperationAborted"
	ErrorCodePermanentRedirect                              ErrorCode = "PermanentRedirect"
	ErrorCodePreconditionFailed                             ErrorCode = "PreconditionFailed"
	ErrorCodeRedirect                                       ErrorCode = "Redirect"
	ErrorCodeRestoreAlreadyInProgress                       ErrorCode = "RestoreAlreadyInProgress"
	ErrorCodeRequestIsNotMultiPartContent                   ErrorCode = "RequestIsNotMultiPartContent"
	ErrorCodeRequestTimeout                                 ErrorCode = "RequestTimeout"
	ErrorCodeRequestTimeTooSkewed                           ErrorCode = "RequestTimeTooSkewed"
	ErrorCodeRequestTorrentOfBucketError                    ErrorCode = "RequestTorrentOfBucketError"
	ErrorCodeServerSideEncryptionConfigurationNotFoundError ErrorCode = "ServerSideEncryptionConfigurationNotFoundError"
	ErrorCodeServiceUnavailable                             ErrorCode = "ServiceUnavailable"
	ErrorCodeSignatureDoesNotMatch                          ErrorCode = "SignatureDoesNotMatch"
	ErrorCodeSlowDown                                       ErrorCode = "SlowDown"
	ErrorCodeTemporaryRedirect                              ErrorCode = "TemporaryRedirect"
	ErrorCodeTokenRefreshRequired                           ErrorCode = "TokenRefreshRequired"
	ErrorCodeTooManyBuckets                                 ErrorCode = "TooManyBuckets"
	ErrorCodeUnexpectedContent                              ErrorCode = "UnexpectedContent"
	ErrorCodeUnresolvableGrantByEmailAddress                ErrorCode = "UnresolvableGrantByEmailAddress"
	ErrorCodeUserKeyMustBeSpecified                         ErrorCode = "UserKeyMustBeSpecified"
	ErrorCodeObjectAlreadyExists                            ErrorCode = "ObjectAlreadyExists"
)
var errMap = map[ErrorCode]errDetail{
	ErrorCodeAccessDenied: {
		"Access Denied",
		403,
	},
	ErrorCodeAccountProblem: {
		"There is a problem with your AWS account that prevents the operation from completing successfully. Please contact AWS Support for further assistance, see Contact Us.",
		403,
	},
	ErrorCodeAllAccessDisabled: {
		"All access to this Amazon S3 resource has been disabled. Please contact AWS Support for further assistance, see Contact Us.",
		403,
	},
	ErrorCodeAmbiguousGrantByEmailAddress: {
		"The email address you provided is associated with more than one account.",
		400,
	},
	ErrorCodeAuthorizationHeaderMalformed: {
		"The authorization header you provided is invalid.",
		400,
	},
	ErrorCodeBadDigest: {
		"The Content-MD5 you specified did not match what we received.",
		400,
	},
	ErrorCodeBucketAlreadyExists: {
		"The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
		409,
	},
	ErrorCodeBucketAlreadyOwnedByYou: {
		"The bucket you tried to create already exists, and you own it. Amazon S3 returns this error in all AWS Regions except us-east-1 (N. Virginia). For legacy compatibility, if you re-create an existing bucket that you already own in us-east-1, Amazon S3 returns 200 OK and resets the bucket access control lists (ACLs).",
		0,
	},
	ErrorCodeBucketNotEmpty: {
		"The bucket you tried to delete is not empty.",
		409,
	},
	ErrorCodeCredentialsNotSupported: {
		"This request does not support credentials.",
		400,
	},
	ErrorCodeCrossLocationLoggingProhibited: {
		"Cross-location logging not allowed. Buckets in one geographic location cannot log information to a bucket in another location.",
		403,
	},
	ErrorCodeEntityTooSmall: {
		"Your proposed upload is smaller than the minimum allowed object size.",
		400,
	},
	ErrorCodeEntityTooLarge: {
		"Your proposed upload exceeds the maximum allowed object size.",
		400,
	},
	ErrorCodeExpiredToken: {
		"The provided token has expired.",
		400,
	},
	ErrorCodeIllegalVersioningConfigurationException: {
		"Indicates that the versioning configuration specified in the request is invalid.",
		400,
	},
	ErrorCodeIncompleteBody: {
		"You did not provide the number of bytes specified by the Content-Length HTTP header",
		400,
	},
	ErrorCodeIncorrectNumberOfFilesInPostRequest: {
		"POST requires exactly one file upload per request.",
		400,
	},
	ErrorCodeInlineDataTooLarge: {
		"Inline data exceeds the maximum allowed size.",
		400,
	},
	ErrorCodeInternalError: {
		"We encountered an internal error. Please try again.",
		500,
	},
	ErrorCodeInvalidAccessKeyId: {
		"The AWS access key ID you provided does not exist in our records.",
		403,
	},
	ErrorCodeInvalidAddressingHeader: {
		"You must specify the Anonymous role.",
		0,
	},
	ErrorCodeInvalidArgument: {
		"Invalid Argument",
		400,
	},
	ErrorCodeInvalidBucketName: {
		"The specified bucket is not valid.",
		400,
	},
	ErrorCodeInvalidBucketState: {
		"The request is not valid with the current state of the bucket.",
		409,
	},
	ErrorCodeInvalidDigest: {
		"The Content-MD5 you specified is not valid.",
		400,
	},
	ErrorCodeInvalidEncryptionAlgorithmError: {
		"The encryption request you specified is not valid. The valid value is AES256.",
		400,
	},
	ErrorCodeInvalidLocationConstraint: {
		"The specified location constraint is not valid. For more information about Regions, see How to Select a Region for Your Buckets.",
		400,
	},
	ErrorCodeInvalidObjectState: {
		"The operation is not valid for the current state of the object.",
		403,
	},
	ErrorCodeInvalidPart: {
		"One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.",
		400,
	},
	ErrorCodeInvalidPartOrder: {
		"The list of parts was not in ascending order. Parts list must be specified in order by part number.",
		400,
	},
	ErrorCodeInvalidPayer: {
		"All access to this object has been disabled. Please contact AWS Support for further assistance, see Contact Us.",
		403,
	},
	ErrorCodeInvalidPolicyDocument: {
		"The content of the form does not meet the conditions specified in the policy document.",
		400,
	},
	ErrorCodeInvalidRange: {
		"The requested range cannot be satisfied.",
		416,
	},
	ErrorCodeInvalidSecurity: {
		"The provided security credentials are not valid.",
		403,
	},
	ErrorCodeInvalidSOAPRequest: {
		"The SOAP request body is invalid.",
		400,
	},
	ErrorCodeInvalidStorageClass: {
		"The storage class you specified is not valid.",
		400,
	},
	ErrorCodeInvalidTargetBucketForLogging: {
		"The target bucket for logging does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group.",
		400,
	},
	ErrorCodeInvalidToken: {
		"The provided token is malformed or otherwise invalid.",
		400,
	},
	ErrorCodeInvalidURI: {
		"Couldn't parse the specified URI.",
		400,
	},
	ErrorCodeKeyTooLongError: {
		"Your key is too long.",
		400,
	},
	ErrorCodeMalformedACLError: {
		"The XML you provided was not well-formed or did not validate against our published schema.",
		400,
	},
	ErrorCodeMalformedPOSTRequest: {
		"The body of your POST request is not well-formed multipart/form-data.",
		400,
	},
	ErrorCodeMalformedXML: {
		"This happens when the user sends malformed XML (XML that doesn't conform to the published XSD) for the configuration. The error message is, \"The XML you provided was not well-formed or did not validate against our published schema.\"",
		400,
	},
	ErrorCodeMaxMessageLengthExceeded: {
		"Your request was too big.",
		400,
	},
	ErrorCodeMaxPostPreDataLengthExceededError: {
		"Your POST request fields preceding the upload file were too large.",
		400,
	},
	ErrorCodeMetadataTooLarge: {
		"Your metadata headers exceed the maximum allowed metadata size.",
		400,
	},
	ErrorCodeMethodNotAllowed: {
		"The specified method is not allowed against this resource.",
		405,
	},
	ErrorCodeMissingAttachment: {
		"A SOAP attachment was expected, but none were found.",
		0,
	},
	ErrorCodeMissingContentLength: {
		"You must provide the Content-Length HTTP header.",
		411,
	},
	ErrorCodeMissingRequestBodyError: {
		"This happens when the user sends an empty XML document as a request. The error message is, \"Request body is empty.\"",
		400,
	},
	ErrorCodeMissingSecurityElement: {
		"The SOAP 1.1 request is missing a security element.",
		400,
	},
	ErrorCodeMissingSecurityHeader: {
		"Your request is missing a required header.",
		400,
	},
	ErrorCodeNoLoggingStatusForKey: {
		"There is no such thing as a logging status subresource for a key.",
		400,
	},
	ErrorCodeNoSuchBucket: {
		"The specified bucket does not exist.",
		404,
	},
	ErrorCodeNoSuchBucketPolicy: {
		"The specified bucket does not have a bucket policy.",
		404,
	},
	ErrorCodeNoSuchKey: {
		"The specified key does not exist.",
		404,
	},
	ErrorCodeNoSuchLifecycleConfiguration: {
		"The lifecycle configuration does not exist.",
		404,
	},
	ErrorCodeNoSuchUpload: {
		"The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.",
		404,
	},
	ErrorCodeNoSuchVersion: {
		"Indicates that the version ID specified in the request does not match an existing version.",
		404,
	},
	ErrorCodeNotImplemented: {
		"A header you provided implies functionality that is not implemented.",
		501,
	},
	ErrorCodeNotSignedUp: {
		"Your account is not signed up for the Amazon S3 service. You must sign up before you can use Amazon S3. You can sign up at the following URL: https://aws.amazon.com/s3",
		403,
	},
	ErrorCodeOperationAborted: {
		"A conflicting conditional operation is currently in progress against this resource. Try again.",
		409,
	},
	ErrorCodePermanentRedirect: {
		"The bucket you are attempting to access must be addressed using the specified endpoint. Send all future requests to this endpoint.",
		301,
	},
	ErrorCodePreconditionFailed: {
		"At least one of the preconditions you specified did not hold.",
		412,
	},
	ErrorCodeRedirect: {
		"Temporary redirect.",
		307,
	},
	ErrorCodeRestoreAlreadyInProgress: {
		"Object restore is already in progress.",
		409,
	},
	ErrorCodeRequestIsNotMultiPartContent: {
		"Bucket POST must be of the enclosure-type multipart/form-data.",
		400,
	},
	ErrorCodeRequestTimeout: {
		"Your socket connection to the server was not read from or written to within the timeout period.",
		400,
	},
	ErrorCodeRequestTimeTooSkewed: {
		"The difference between the request time and the server's time is too large.",
		403,
	},
	ErrorCodeRequestTorrentOfBucketError: {
		"Requesting the torrent file of a bucket is not permitted.",
		400,
	},
	ErrorCodeServerSideEncryptionConfigurationNotFoundError: {
		"The server side encryption configuration was not found.",
		400,
	},
	ErrorCodeServiceUnavailable: {
		"Reduce your request rate.",
		503,
	},
	ErrorCodeSignatureDoesNotMatch: {
		"The request signature we calculated does not match the signature you provided. Check your AWS secret access key and signing method. For more information, see REST Authentication and SOAP Authentication for details.",
		403,
	},
	ErrorCodeSlowDown: {
		"Reduce your request rate.",
		503,
	},
	ErrorCodeTemporaryRedirect: {
		"You are being redirected to the bucket while DNS updates.",
		307,
	},
	ErrorCodeTokenRefreshRequired: {
		"The provided token must be refreshed.",
		400,
	},
	ErrorCodeTooManyBuckets: {
		"You have attempted to create more buckets than allowed.",
		400,
	},
	ErrorCodeUnexpectedContent: {
		"This request does not support content.",
		400,
	},
	ErrorCodeUnresolvableGrantByEmailAddress: {
		"The email address you provided does not match any account on record.",
		400,
	},
	ErrorCodeUserKeyMustBeSpecified: {
		"The bucket POST must contain the specified field name. If it is specified, check the order of the fields.",
		400,
	},
	ErrorCodeObjectAlreadyExists: {
		"When the `forbidden` header was set, PUT should be denied if the object already exists",
		409,
	},
}

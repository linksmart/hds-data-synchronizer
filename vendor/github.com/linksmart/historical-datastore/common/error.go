package common

import (
	"encoding/json"
	"log"
	"net/http"

	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc/codes"
)

type Error interface {
	error
	HttpStatus() int
	GrpcStatus() codes.Code
	Title() string
}

// Not Found
type InternalError struct{ S string }

func (e *InternalError) Error() string { return e.S }

func (e *InternalError) HttpStatus() int { return http.StatusInternalServerError }

func (e *InternalError) GrpcStatus() codes.Code { return codes.Unknown }

func (e *InternalError) Title() string { return http.StatusText(http.StatusInternalServerError) }

// Not Found
type NotFoundError struct{ S string }

func (e *NotFoundError) Error() string { return e.S }

func (e *NotFoundError) HttpStatus() int { return http.StatusNotFound }

func (e *NotFoundError) GrpcStatus() codes.Code { return codes.NotFound }

func (e *NotFoundError) Title() string { return http.StatusText(http.StatusNotFound) }

// Conflict (non-unique id, assignment to read-only data)
type ConflictError struct{ S string }

func (e *ConflictError) Error() string { return e.S }

func (e *ConflictError) HttpStatus() int { return http.StatusConflict }

func (e *ConflictError) GrpcStatus() codes.Code { return codes.AlreadyExists }

func (e *ConflictError) Title() string { return http.StatusText(http.StatusConflict) }

// Bad Request
type BadRequestError struct{ S string }

func (e *BadRequestError) Error() string { return e.S }

func (e *BadRequestError) HttpStatus() int { return http.StatusBadRequest }

func (e *BadRequestError) GrpcStatus() codes.Code { return codes.InvalidArgument }

func (e *BadRequestError) Title() string { return http.StatusText(http.StatusBadRequest) }

// Deadline Exceeded Error
type UnsupportedMediaTypeError struct{ S string }

func (e *UnsupportedMediaTypeError) Error() string { return e.S }

func (e *UnsupportedMediaTypeError) HttpStatus() int { return http.StatusUnsupportedMediaType }

func (e *UnsupportedMediaTypeError) GrpcStatus() codes.Code { return codes.InvalidArgument }

func (e *UnsupportedMediaTypeError) Title() string {
	return http.StatusText(http.StatusUnsupportedMediaType)
}

// HttpErrorResponse writes error to HTTP ResponseWriter
func HttpErrorResponse(err Error, w http.ResponseWriter) {
	// Problem Details for HTTP APIs (RFC 7807)
	type Error struct {
		// Type A URI reference [RFC3986] that identifies the problem type.  This specification encourages that, when
		//      dereferenced, it provide human-readable documentation for the problem type (e.g., using HTML).  When
		//      this member is not present, its value is assumed to be "about:blank".
		Type string `json:"type,omitempty"`

		// Title - A short, human-readable summary of the problem type. It SHOULD NOT change from occurrence to occurrence of the
		//      problem, except for purposes of localization (e.g., using proactive content negotiation; see RFC7231], Section 3.4.
		Title string `json:"title"`

		// Status - The HTTP status code (RFC7231, Section 6) generated by the origin server for this occurrence of the problem.
		Status int `json:"status"`

		// Detail - A human-readable explanation specific to this occurrence of the problem.
		Detail string `json:"detail"`

		// "Instance" - A URI reference that identifies the specific occurrence of the problem.  It may or may not yield further information if dereferenced.
		Instance string `json:"instance,omitempty"`
	}

	status := err.HttpStatus()
	e := &Error{
		Title:  err.Title(),
		Status: status,
		Detail: err.Error(),
	}
	if status >= 500 {
		// Append the uuid
		// A suffix "/problem" is added, that might not exist as  this has to be URI, can be a relative URI, does not have to yield further information if dereferenced.
		e.Instance = "/problem/" + uuid.NewV4().String()

		log.Printf("ERROR: %s: %s\n", e.Instance, err)
	}
	b, _ := json.Marshal(e)
	w.Header().Set("Content-Type", "application/problem+json;version="+APIVersion)
	w.WriteHeader(status)
	w.Write(b)
}
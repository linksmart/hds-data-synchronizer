package utils

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/linksmart/go-sec/auth/obtainer"
)

// Constructs and submits an HTTP request and returns the response
func HTTPRequest(method string, url string, headers map[string][]string, body io.Reader,
	ticket *obtainer.Client) (*http.Response, error) {

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	// Set headers
	for key, val := range headers {
		req.Header.Set(key, strings.Join(val, ";"))
	}

	// Do authenticated request if ticket client is provided
	if ticket != nil {
		return HTTPDoAuth(req, ticket)
	}

	// No auth
	return http.DefaultClient.Do(req)
}

// Send an HTTP request with X-Auth-Token entity-header.
//	Ticket is renewed once in case of failure.
func HTTPDoAuth(req *http.Request, ticket *obtainer.Client) (*http.Response, error) {
	X_Auth_Token, err := ticket.Obtain()
	if err != nil {
		return nil, err
	}

	// Set auth header and send the request
	//req.Header.Set("X-Auth-Token", X_Auth_Token)
	req.Header.Set("Authorization", "Bearer "+X_Auth_Token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("HTTPDoAuth() Unexpected empty HTTP response.")
	}

	if res.StatusCode == http.StatusUnauthorized {
		// Get a new ticket and retry again
		log.Println("HTTPDoAuth() Invalid authentication ticket.")
		X_Auth_Token, err = ticket.Renew()
		if err != nil {
			return nil, err
		}
		log.Println("HTTPDoAuth() Ticket was renewed.")

		// Reset the header and try again
		//req.Header.Set("X-Auth-Token", X_Auth_Token)
		req.Header.Set("Authorization", "Bearer "+X_Auth_Token)
		return http.DefaultClient.Do(req)
	}

	return res, nil
}

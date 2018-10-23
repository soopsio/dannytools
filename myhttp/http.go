package myhttp

import (
	"bytes"
	"dannytools/ehand"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"time"
)

// timeout millisecond
func RequestGet(url string, timeout uint32) ([]byte, error) {

	var result []byte

	tout := time.Duration(time.Duration(timeout) * time.Millisecond)
	client := http.Client{Timeout: tout}

	resp, err := client.Get(url)
	if resp != nil {
		if resp.Body != nil {
			defer resp.Body.Close()
		}

	}
	if err != nil {
		if resp != nil {
			resp.Close = true
			if resp.Body != nil {
				result, _ = ioutil.ReadAll(resp.Body)
			}
		}
		return result, err
	}

	result, err = ioutil.ReadAll(resp.Body)
	resp.Close = true
	if err != nil {
		return result, err
	}
	if resp.StatusCode != http.StatusOK {
		return []byte{}, fmt.Errorf("request fail: errorcode: %d, errormsg:%s", resp.StatusCode, string(result))
	}
	//fmt.Printf("request result : %s\n", result)
	return result, nil

}

// timeout millisecond
// return result, error, errmsg
func RequestPostJson(url string, timeout uint32, body []byte, header map[string]string) ([]byte, error, string) {
	var result []byte
	tout := time.Duration(time.Duration(timeout) * time.Millisecond)
	client := http.Client{Timeout: tout}
	bd := bytes.NewBuffer(body)
	req, err := http.NewRequest("POST", url, bd)
	if err != nil {
		return result, err, err.Error()
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if resp != nil {
		if resp.Body != nil {
			defer resp.Body.Close()
		}

	}

	if err != nil {
		if resp != nil {
			resp.Close = true
			if resp.Body != nil {
				result, _ = ioutil.ReadAll(resp.Body)
			}
		}

		return result, err, err.Error()
	}
	result, err = ioutil.ReadAll(resp.Body)
	resp.Close = true
	if err != nil {
		return result, err, err.Error()
	}
	if resp.StatusCode != http.StatusOK {
		if len(result) > 0 {
			return result, fmt.Errorf("request fail: errorcode: %d, errormsg:%s", resp.StatusCode, string(result)), string(result)
		} else {
			return nil, fmt.Errorf("request fail: errorcode: %d", resp.StatusCode), ""
		}
	}
	return result, nil, ""
}

func JoinUrlPaths(urlStr, relativePath string) (string, error) {
	base, err := url.Parse(urlStr)
	if err != nil {
		return "", ehand.WithStackError(err)
	}

	rel, err := url.Parse(relativePath)
	if err != nil {
		return "", ehand.WithStackError(err)
	}
	return base.ResolveReference(rel).String(), nil
}

func BuildUrl(base string, params map[string]string) string {
	if len(params) == 0 {
		return base
	}
	p := ""
	for k, v := range params {
		if p == "" {
			p = fmt.Sprintf("%s=%s", k, url.QueryEscape(v))
		} else {
			p = fmt.Sprintf("%s&%s=%s", p, k, url.QueryEscape(v))
		}
	}
	//p = url.QueryEscape(p)
	return fmt.Sprintf("%s?%s", base, p)
}

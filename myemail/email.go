package myemail

import (
	"crypto/tls"
	"dannytools/myhttp"
	//"fmt"
	"strings"

	"github.com/go-gomail/gomail"
)

type EmailInfo struct {
	Host     string
	Port     int
	UserName string
	Password string
	From     string
	To       []string
}
type EmailBody struct {
	Body        string
	ContentType string
}

type EmailCC struct {
	Addr string
	Name string
}

type EmailContent struct {
	//	/To []string
	//CC       []EmailCC
	Subject  string
	Body     EmailBody
	Attaches []string
}

func (this EmailInfo) SendEmail(detail EmailContent) error {
	d := gomail.NewDialer(this.Host, this.Port, this.UserName, this.Password)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	m := gomail.NewMessage()
	m.SetHeader("From", this.From)
	m.SetHeader("To", this.To...)
	m.SetHeader("Subject", detail.Subject)
	m.SetBody(detail.Body.ContentType, detail.Body.Body)
	for _, f := range detail.Attaches {
		m.Attach(f)
	}
	return d.DialAndSend(m)
}

func (this EmailInfo) SendEmailUrlGet(url string, detail EmailContent, tout uint32) ([]byte, error) {
	params := map[string]string{"emails": strings.Join(this.To, ","), "subject": detail.Subject, "message": detail.Body.Body}
	myurl := myhttp.BuildUrl(url, params)
	//fmt.Println(myurl)
	return myhttp.RequestGet(myurl, tout)
}

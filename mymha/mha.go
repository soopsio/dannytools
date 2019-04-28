package mymha

import (
	"dannytools/constvar"
	"dannytools/ehand"
	"dannytools/logging"
	"dannytools/myemail"
	"dannytools/myhttp"
	"dannytools/netip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/toolkits/file"
)

const (
	ReportLevelNo        int    = 0
	ReportLevelEmail     int    = 1
	ReportLevelWebchat   int    = 2
	ReportLevelSms       int    = 3
	ReportLevelPhoneCall int    = 4
	CmdStop              string = "stop"
	CmdStopSsh           string = "stopssh"
	CmdStart             string = "start"
	CmdStatus            string = "status"
	AlarmChannelPhone    string = "phone"
	AlarmChannelSms      string = "sms"
	AlarmChannelWechat   string = "wx"
	AlarmChannelEmail    string = "mail"
)

var (
	//xx.xx.xx.xx_3309: MySQL Master failover xx.xx.xx.xx(xx.xx.xx.xx:3309)
	//xx.xx.xx.xx_3307: MySQL Master failover xx.xx.xx.xx(xx.xx.xx.xx:3307) to xx.xx.xx.yy(xx.xx.xx.yy:3307) succeeded
	MhaSubjMyAddrReg          *regexp.Regexp = regexp.MustCompile(`\((.+?):(\d+)\)`)
	MhaSubjMyAddrAndResultReg *regexp.Regexp = regexp.MustCompile(`\((.+?):(\d+)\)\s+(\w+)`)
)

type FailoverSummary struct {
	OldMasterIp   string
	OldMasterPort string
	NewMasterIp   string
	NewMasterPort string
	IfSucceed     bool
}

func (this *FailoverSummary) ParseMhaSubject(s string) {

	if s != "" {
		arr := strings.Split(s, "to")

		tmpArr := MhaSubjMyAddrReg.FindStringSubmatch(arr[0])
		if len(tmpArr) >= 3 {
			this.OldMasterIp = tmpArr[1]
			this.OldMasterPort = tmpArr[2]
		}

		if len(arr) >= 2 {
			tmpArr = MhaSubjMyAddrAndResultReg.FindStringSubmatch(arr[1])
			if len(tmpArr) >= 4 {
				this.NewMasterIp = tmpArr[1]
				this.NewMasterPort = tmpArr[2]
				if tmpArr[3] == "succeeded" {
					this.IfSucceed = true
				}
			}
		}
	}
}

type AlarmMsg struct {
	//Level    string
	Channels string
	Users    []string
	Subject  string
	Content  string
}

func (this *AlarmMsg) AddTimeToContent() {
	this.Content = fmt.Sprintf("time: %s\n%s", time.Now().Format(constvar.DATETIME_FORMAT), this.Content)
}

func (this *AlarmMsg) UrlString() string {
	return fmt.Sprintf("users=%s&subject=%s&content=%s&channels=%s",
		strings.Join(this.Users, ","), this.Subject, this.Content, this.Channels)

}

func (this *AlarmMsg) JsonBytes() ([]byte, error) {
	return json.Marshal(this)
}

type AlarmAddrs struct {
	EmailAddrs   []string
	SmsPhones    []string
	CallPhones   []string
	WebchatAddrs []string
	//curl -d "users=roc&subject=MHA failover &content=MHA failover suceessfully &channels=wx"   http://xxx
	//curl -d "users=roc&subject=MHA failover &content=MHA failover suceessfully &channels=wx"  http://xx
	AlarmUrl          string
	UrlRequestTimeout uint32 //millisecond
	IfJson            bool   // if true, http body= AlarmMsg.JsonBytes(), else http body=AlarmMsg.UrlString()

	// reportLevel:
	//	0: not send
	//	1: email
	//	2: webchat
	//	3: sms
	//	4: phonecall
	ReportLevel int

	EmailHost     string
	EmailPort     int
	EmailUser     string
	EmailPassword string
	EmailFrom     string
}

func (this *AlarmAddrs) SetDefaultNotOverwrite() {
	if this.UrlRequestTimeout <= 0 {
		this.UrlRequestTimeout = 2000
	}
	if this.ReportLevel <= 0 {
		this.ReportLevel = ReportLevelEmail
	}
}

func (this *AlarmAddrs) CheckOk() bool {
	if len(this.EmailAddrs) == 0 && len(this.SmsPhones) == 0 && len(this.CallPhones) == 0 && len(this.WebchatAddrs) == 0 {
		return false
	}
	if this.AlarmUrl == "" && (this.EmailHost == "" || this.EmailPort <= 0 || this.EmailUser == "" || this.EmailPassword == "") {
		return false
	}
	return true
}

func (this *AlarmAddrs) SendReportNoLog(subj, content string) error {
	var (
		msgsArr     []*AlarmMsg
		result      []byte
		err         error
		errStr      string
		bodyMsgByte []byte
		bodyMsgStr  string
	)
	if this.ReportLevel <= ReportLevelNo {
		return errors.Errorf("ReportLevel is %d, no alarm is sent", this.ReportLevel)
	}
	if this.AlarmUrl != "" {

		if this.ReportLevel >= ReportLevelPhoneCall && len(this.CallPhones) > 0 {

			msgsArr = append(msgsArr, &AlarmMsg{
				Channels: AlarmChannelPhone,
				Users:    this.CallPhones,
				Subject:  subj})
		}
		if this.ReportLevel >= ReportLevelSms && len(this.SmsPhones) > 0 {
			msgsArr = append(msgsArr, &AlarmMsg{
				Channels: AlarmChannelSms,
				Users:    this.SmsPhones,
				Subject:  subj})
		}

		if this.ReportLevel >= ReportLevelWebchat && len(this.WebchatAddrs) > 0 {
			msgsArr = append(msgsArr, &AlarmMsg{
				Channels: AlarmChannelWechat,
				Users:    this.WebchatAddrs,
				Subject:  subj,
				Content:  content})
		}

		if this.ReportLevel >= ReportLevelEmail && len(this.EmailAddrs) > 0 {
			msgsArr = append(msgsArr, &AlarmMsg{
				Channels: AlarmChannelEmail,
				Users:    this.EmailAddrs,
				Subject:  subj,
				Content:  content})
		}

		for i := range msgsArr {
			msgsArr[i].AddTimeToContent()
			if this.IfJson {
				bodyMsgByte, err = msgsArr[i].JsonBytes()
				if err != nil {
					return errors.Annotatef(err, "error to marshal alarm msg to json")
				}
				result, err, errStr = myhttp.RequestPostJson(this.AlarmUrl, this.UrlRequestTimeout, bodyMsgByte, map[string]string{})
				bodyMsgStr = string(bodyMsgByte)
			} else {
				bodyMsgStr = msgsArr[i].UrlString()
				result, err, errStr = myhttp.RequestPostJson(this.AlarmUrl, this.UrlRequestTimeout, []byte(bodyMsgStr), map[string]string{})
			}

			if err != nil {
				return errors.Annotatef(err, "error to send %s alarm: %s\nresponse: %s\ncontent: %s", msgsArr[i].Channels, errStr, string(result), bodyMsgStr)

			}
		}
	} else {

		einfo := myemail.EmailInfo{Host: this.EmailHost, Port: this.EmailPort, UserName: this.EmailUser,
			Password: this.EmailPassword,
			From:     this.EmailFrom, To: this.EmailAddrs}
		eContent := myemail.EmailContent{Subject: subj,
			Body: myemail.EmailBody{Body: content, ContentType: "text/plain"}}
		err = einfo.SendEmail(eContent)
		if err != nil {
			return errors.Annotatef(err, "error to send email")
		}
	}
	return nil
}

func (this *AlarmAddrs) Parse(adrFile string) error {
	jbytes, err := file.ToBytes(adrFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(jbytes, this)
	if err != nil {
		return err
	}
	if this.UrlRequestTimeout == 0 {
		this.UrlRequestTimeout = 5
	}
	return nil
}

func (this *AlarmAddrs) SendReports(myLogger *logging.MyLog, subj, content string) {
	var (
		msgsArr     []*AlarmMsg
		result      []byte
		err         error
		errStr      string
		bodyMsgByte []byte
		bodyMsgStr  string
	)
	if this.AlarmUrl != "" {

		if this.ReportLevel >= ReportLevelPhoneCall && len(this.CallPhones) > 0 {

			msgsArr = append(msgsArr, &AlarmMsg{
				Channels: AlarmChannelPhone,
				Users:    this.CallPhones,
				Subject:  subj})
		}
		if this.ReportLevel >= ReportLevelSms && len(this.SmsPhones) > 0 {
			msgsArr = append(msgsArr, &AlarmMsg{
				Channels: AlarmChannelSms,
				Users:    this.SmsPhones,
				Subject:  subj})
		}

		if this.ReportLevel >= ReportLevelWebchat && len(this.WebchatAddrs) > 0 {
			msgsArr = append(msgsArr, &AlarmMsg{
				Channels: AlarmChannelWechat,
				Users:    this.WebchatAddrs,
				Subject:  subj,
				Content:  content})
		}

		if this.ReportLevel >= ReportLevelEmail && len(this.EmailAddrs) > 0 {
			msgsArr = append(msgsArr, &AlarmMsg{
				Channels: AlarmChannelEmail,
				Users:    this.EmailAddrs,
				Subject:  subj,
				Content:  content})
		}

		for i := range msgsArr {
			msgsArr[i].AddTimeToContent()
			if this.IfJson {
				bodyMsgByte, err = msgsArr[i].JsonBytes()
				if err != nil {
					myLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "error to marshal alarm msg to json", logging.ERROR, ehand.ERR_JSON_MARSHAL)
					continue
				}
				result, err, errStr = myhttp.RequestPostJson(this.AlarmUrl, this.UrlRequestTimeout, bodyMsgByte, map[string]string{})
				bodyMsgStr = string(bodyMsgByte)
			} else {
				bodyMsgStr = msgsArr[i].UrlString()
				result, err, errStr = myhttp.RequestPostJson(this.AlarmUrl, this.UrlRequestTimeout, []byte(bodyMsgStr), map[string]string{})
			}

			if err != nil {
				myLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, fmt.Sprintf("error to send %s alarm: %s\nresponse: %s\ncontent: %s",
					msgsArr[i].Channels, errStr, string(result), bodyMsgStr),
					logging.ERROR, ehand.ERR_HTTP_GET)
			} else {
				myLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("successfully send %s alarm \nresponse: %s",
					msgsArr[i].Channels, string(result)), logging.INFO)
			}
		}
	} else {
		myLogger.WriteToLogByFieldsNormalOnlyMsg("AlarmUrl is empty, only send email", logging.WARNING)
		einfo := myemail.EmailInfo{Host: this.EmailHost, Port: this.EmailPort, UserName: this.EmailUser,
			Password: this.EmailPassword,
			From:     this.EmailFrom, To: this.EmailAddrs}
		eContent := myemail.EmailContent{Subject: subj,
			Body: myemail.EmailBody{Body: content, ContentType: "text/plain"}}
		err = einfo.SendEmail(eContent)
		if err != nil {
			myLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "error to send email", logging.ERROR, ehand.ERR_ERROR)
		} else {
			myLogger.WriteToLogByFieldsNormalOnlyMsg("successfully send email", logging.INFO)
		}
	}
}

func (this *AlarmAddrs) GenAndSendReports(myLogger *logging.MyLog, vip string, lanip string, port int, subject string, body string) {

	if lanip != "" {
		subject = fmt.Sprintf("%s %s", subject, lanip)
		body = fmt.Sprintf("ip: %s\n%s", lanip, body)
	} else if vip != "" {
		subject = fmt.Sprintf("%s %s", subject, vip)
		body = fmt.Sprintf("vip: %s\n%s", vip, body)
	}
	if port != 0 {
		subject = fmt.Sprintf("%s %d", subject, port)
		body = fmt.Sprintf("port: %d\n%s", port, body)
	}

	this.SendReports(myLogger, subject, body)

}

//file named as ip_port_vip
//get the latest modified one
func GetVipFromFileName(vipDir string, masterIp string, masterPort int, ifGetLatest bool) (string, error) {

	var (
		name      string
		matchName string
		mtime     time.Time
		prefix    string = fmt.Sprintf("%s_%d_", masterIp, masterPort)
	)
	fs, err := ioutil.ReadDir(vipDir)
	if err != nil {
		return "", err
	}

	for _, f := range fs {
		if f.IsDir() {
			continue
		}
		name = f.Name()
		if name == "." || name == ".." {
			continue
		}
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if matchName == "" {
			matchName = name
			if !ifGetLatest {
				break
			}
			mtime = f.ModTime()
			continue
		}
		if f.ModTime().After(mtime) {
			matchName = name
			mtime = f.ModTime()
		}

	}
	if matchName == "" {
		return "", fmt.Errorf("vip file %s_%d_vip not found in %s", masterIp, masterPort, vipDir)
	}
	arr := strings.Split(matchName, "_")
	if len(arr) != 3 {
		return "", fmt.Errorf("vip file %s is not named as ip_port_vip", filepath.Join(vipDir, matchName))
	}
	vip := arr[2]
	if !netip.CheckValidIpv4(vip) {
		return "", fmt.Errorf("vip %s (%s) is invalid an ipv4 ip", vip, filepath.Join(vipDir, matchName))
	}
	return vip, nil

}

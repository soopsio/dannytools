package myssh

import (
	"fmt"
	"os/user"
	"path/filepath"
	"time"

	"github.com/toolkits/file"
	"golang.org/x/crypto/ssh"
)

// tout: millisecond
func SshConnectByKeyUser(user string, host string, port int, tout int) (*ssh.Client, error) {
	var (
		addr string = fmt.Sprintf("%s:%d", host, port)
		err  error
	)
	cfg, err := GetSshConfigUser(user, tout)
	if err != nil {
		return nil, err
	}
	return ssh.Dial("tcp", addr, cfg)
}

//return: stdout&stderr, error
func SshExecuteCmd(client *ssh.Client, cmd string) (string, error) {
	var (
		session *ssh.Session
		err     error
		result  []byte
	)
	session, err = client.NewSession()
	if session != nil {
		defer session.Close()
	}
	if err != nil {
		return "", err
	}
	result, err = session.CombinedOutput(cmd)

	return string(result), err

}

func GetSshConfigUser(user string, tout int) (*ssh.ClientConfig, error) {
	var (
		err   error
		cfg   *ssh.ClientConfig
		kFile string
	)
	kFile, err = GetUserKeyFile(user)
	if err != nil {
		return cfg, err
	}
	return GetSshConfig(user, tout, kFile)

}

// tout: millisecond
func GetSshConfig(user string, tout int, keyFile string) (*ssh.ClientConfig, error) {
	var (
		err     error
		cfg     *ssh.ClientConfig
		hostKey ssh.PublicKey
	)
	sig, err := GetSignerFromKey(keyFile)
	if err != nil {
		return cfg, err
	}
	return &ssh.ClientConfig{User: user,
		Timeout: time.Duration(tout) * time.Millisecond,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(sig)},
		HostKeyCallback: ssh.FixedHostKey(hostKey),
	}, nil
}

func GetUserKeyFile(userName string) (string, error) {
	u, err := user.Lookup(userName)
	if err != nil {
		return "", err
	}
	keyFile := filepath.Join(u.HomeDir, ".ssh", "id_rsa")
	if file.IsFile(keyFile) {
		return keyFile, nil
	} else {
		return "", fmt.Errorf("%s not exists", keyFile)
	}
}

func GetSignerFromKey(keyFile string) (ssh.Signer, error) {
	kBytes, err := file.ToBytes(keyFile)
	if err != nil {
		return nil, err
	}
	return ssh.ParsePrivateKey(kBytes)
}

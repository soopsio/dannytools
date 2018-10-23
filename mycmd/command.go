package mycmd

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"dannytools/ehand"
)

/* timeout: millisecond
return: stdout, stderr, error
*/
func ExecCmdTimeOutBytes(timeout uint32, name string, args ...string) ([]byte, []byte, error) {

	var (
		out    bytes.Buffer
		errout bytes.Buffer
		err    error
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = &out
	cmd.Stderr = &errout

	err = cmd.Run()
	if err != nil {
		return out.Bytes(), errout.Bytes(), ehand.WithStackError(err)
	} else {
		return out.Bytes(), errout.Bytes(), nil
	}

}

/* timeout: millisecond
return: stdout, stderr, error
*/
func ExecCmdTimeOutString(timeout uint32, name string, args ...string) (string, string, error) {

	var (
		out    bytes.Buffer
		errout bytes.Buffer
		err    error
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = &out
	cmd.Stderr = &errout

	err = cmd.Run()

	if err != nil {
		return strings.TrimSpace(out.String()), strings.TrimSpace(errout.String()), ehand.WithStackError(err)
	} else {
		return strings.TrimSpace(out.String()), strings.TrimSpace(errout.String()), nil
	}

}

/* timeout: millisecond
return: stdout, stderr, error
*/
func ExecCmdTimeOutStringNoStack(timeout uint32, name string, args ...string) (string, string, error) {

	var (
		out    bytes.Buffer
		errout bytes.Buffer
		err    error
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = &out
	cmd.Stderr = &errout

	err = cmd.Run()

	if err != nil {
		return strings.TrimSpace(out.String()), strings.TrimSpace(errout.String()), err
	} else {
		return strings.TrimSpace(out.String()), strings.TrimSpace(errout.String()), nil
	}

}

func ExecCmdTimeOutStringSplit(timeout uint32, cmd string) (string, string, error) {
	arr := strings.Fields(cmd)
	args := strings.Join(arr[1:], " ")
	return ExecCmdTimeOutString(timeout, arr[0], args)
}

func ExecCmdTimeOutStringBashNoStack(timeout uint32, cmd string) (string, string, error) {
	return ExecCmdTimeOutStringNoStack(timeout, "bash", "-c", cmd)
}

func ExecCmdTimeOutStringBash(timeout uint32, cmd string) (string, string, error) {
	return ExecCmdTimeOutString(timeout, "bash", "-c", cmd)
}

func GetPidOfProcess(cmd string) (int32, error) {
	stdOut, stdErr, err := ExecCmdTimeOutStringBash(2000, cmd)
	if err != nil {
		return 0, fmt.Errorf("%s %s %s", err, stdErr, stdOut)
	}
	stdOut = strings.TrimSpace(stdOut)
	if stdOut == "" {
		return 0, ehand.WithStackError(fmt.Errorf("the cmd of get pid return empty: %s", cmd))
	}
	tmpInt, err := strconv.Atoi(stdOut)
	if err != nil {
		return 0, ehand.WithStackError(fmt.Errorf("fail to convert string %s to integer: %s", stdOut, err))
	}
	if tmpInt == 0 {
		return 0, ehand.WithStackError(fmt.Errorf("processid is %d", tmpInt))
	}
	return int32(tmpInt), nil
}

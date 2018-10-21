package mypciecard

import (
	"dannytools/mycmd"
	"fmt"
	"strings"
)

const (
	C_PCIE_TYPE_NONE    uint8 = 0
	C_PCIE_TYPE_FUSION  uint8 = 1
	C_PCIE_TYPE_SHANNO  uint8 = 2
	C_PCIE_TYPE_UNKNOWN uint8 = 254
	C_PCIE_TYPE_ERROR   uint8 = 255

	C_PCIE_HEALTHY   uint8 = 1
	C_PCIE_UNHEALTHY uint8 = 0

	C_No_Cmd string = "cmdNotFound"
)

func RunPcieCardStatusCmd(tout uint32, cmd string) (string, string) {
	msgOut, msgErr, err := mycmd.ExecCmdTimeOutStringBash(tout, cmd)
	if msgErr != "" && strings.Contains(msgErr, "command not found") {
		return "", C_No_Cmd
	}
	outStr := fmt.Sprintf("error: %s\n\tstderr: %s\n\tstdout: %s\n\tcommand: %s", err, msgErr, msgOut, cmd)
	if err != nil || msgErr != "" {
		return "", outStr
	} else {
		return msgOut, ""
	}
}

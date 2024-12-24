package greengage

import (
	"os/exec"

	"github.com/GreengageDB/greengage/gp/utils"
)

const (
	gpstart = "gpstart"
)

type GpStart struct {
	DataDirectory string `flag:"-d"`
	Verbose       bool   `flag:"-v"`
}

func (cmd *GpStart) BuildExecCommand(gpHome string) *exec.Cmd {
	utility := utils.GetGpUtilityPath(gpHome, gpstart)
	args := append([]string{"-a"}, utils.GenerateArgs(cmd)...)

	return utils.System.ExecCommand(utility, args...)
}

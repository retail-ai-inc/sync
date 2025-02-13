package test

import (
	"github.com/retail-ai-inc/sync/pkg/utils"
	"testing"
)

func testTC13Utils(t *testing.T) {

	now := utils.GetCurrentTime()
	t.Logf("Utils.GetCurrentTime => %s", now)

	utils.UnzipDistFile("", "")

}

// Command mint prints a bearer token for the API, signed with the
// SYNC_TOKEN_SECRET in the environment. It exists because the API takes a
// Bearer header and has no cookie session, so a shell script needs a token the
// same way the web UI holds one.
package main

import (
	"fmt"
	"os"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: mint <username> <access>")
		os.Exit(2)
	}
	fmt.Print(domain.GenerateUserToken(os.Args[1], os.Args[2]))
}

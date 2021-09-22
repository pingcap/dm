package conn

import (
	"fmt"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/server"
)

// NewMemoryMysqlServer New in memory mysql server.
func NewMemoryMysqlServer(host, user, password string, port int) (*server.Server, error) {
	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", host, port),
		Auth:     auth.NewNativeSingle(user, password, auth.AllPermissions),
	}
	return server.NewDefaultServer(config, sqle.NewDefault())
}

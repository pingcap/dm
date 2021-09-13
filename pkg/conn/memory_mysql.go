package conn

import (
	"fmt"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/server"
)

// NewMemoryMysqlServer New in memory mysql server.
func NewMemoryMysqlServer(host, user, password string, port int) *server.Server {
	driver := sqle.NewDefault()

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", host, port),
		Auth:     auth.NewNativeSingle(user, password, auth.AllPermissions),
	}

	s, err := server.NewDefaultServer(config, driver)
	if err != nil {
		panic(err)
	}
	return s
}

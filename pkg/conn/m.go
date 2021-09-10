package conn

import (
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/server"
)

func InitInMemoryMysqlDBIn3306() *server.Server {
	driver := sqle.NewDefault()

	config := server.Config{
		Protocol: "tcp",
		Address:  "127.0.0.1:3306",
		Auth:     auth.NewNativeSingle("root", "123456", auth.AllPermissions),
	}

	s, err := server.NewDefaultServer(config, driver)
	if err != nil {
		panic(err)
	}
	return s
}

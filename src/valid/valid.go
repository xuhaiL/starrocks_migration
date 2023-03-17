package valid

import (
	"os"
	"starrocks_migration/src/utls"
)

func ConnectionValid() {
	utls.InitConnAll()

	err := utls.SourceConn.Ping()
	if err != nil {
		os.Exit(-1022)
	}
	err = utls.TargetConn.Ping()
	if err != nil {
		os.Exit(-1022)
	}
	utls.Log("INFO", "Connection valid success")
}

/*func Close() {
	utls.CloseConn(SourceConn)
	utls.CloseConn(TargetConn)
	utls.Log("INFO", "Connection close success")
}*/

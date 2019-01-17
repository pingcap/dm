package master

import (
	"net"
	"net/http"

	"github.com/ngaut/log"
	"github.com/soheilhy/cmux"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/utils"
)

type statusHandler struct {
}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	text := utils.GetRawInfo()
	_, err := w.Write([]byte(text))
	if err != nil && !common.IsErrNetClosing(err) {
		log.Errorf("[server] write status response error %s", err.Error())
	}
}

// InitStatus initializes the HTTP status server
func InitStatus(lis net.Listener) {
	mux := http.NewServeMux()
	mux.Handle("/status", &statusHandler{})
	httpS := &http.Server{
		Handler: mux,
	}
	err := httpS.Serve(lis)
	if err != nil && !common.IsErrNetClosing(err) && err != cmux.ErrListenerClosed {
		log.Errorf("[server] status server return with error %s", err.Error())
	}
}

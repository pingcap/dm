package openapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestNewSwaggerDocUI(t *testing.T) {
	req, _ := http.NewRequest("GET", "/api/v1/docs", nil)
	req = req.WithContext(context.TODO()) // make lint happy

	// 200
	mw := NewSwaggerDocUI(NewSwaggerConfig("/api/v1/docs", "/api/v1/docs/dm.json", ""), []byte{})
	e := echo.New()
	e.Pre(mw)
	handler := e.Server.Handler
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != 200 {
		t.Errorf("Wrong swagger resp code: %v, want: 200 ", recorder.Code)
	}
	if recorder.Header().Get("Content-Type") != "text/html; charset=UTF-8" {
		t.Errorf("Wrong response content type: %v", recorder.Header().Get("Content-Type"))
	}
	respString := recorder.Body.String()
	if !strings.Contains(respString, "<title>API documentation</title>") {
		t.Errorf("Wrong response bytes")
	}
}

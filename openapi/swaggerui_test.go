package openapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/pingcap/check"
)

var _ = check.Suite(&swaggerUISuite{})

type swaggerUISuite struct{}

func TestNewSwaggerDocUI(t *testing.T) {
	check.TestingT(t)
}

func (t *swaggerUISuite) TestNewSwaggerDocUI(c *check.C) {
	req, _ := http.NewRequest("GET", "/api/v1/docs", nil)
	req = req.WithContext(context.TODO()) // make lint happy

	mw := NewSwaggerDocUI(NewSwaggerConfig("/api/v1/docs", "/api/v1/docs/dm.json", ""), []byte{})
	e := echo.New()
	e.Pre(mw)
	handler := e.Server.Handler
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	c.Assert(recorder.Code, check.Equals, http.StatusOK)
	c.Assert(recorder.Header().Get("Content-Type"), check.Equals, "text/html; charset=UTF-8")
	respString := recorder.Body.String()
	c.Assert(strings.Contains(respString, "<title>API documentation</title>"), check.Equals, true)
}

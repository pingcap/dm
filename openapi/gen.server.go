// Package openapi provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen version v1.8.2 DO NOT EDIT.
package openapi

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/deepmap/oapi-codegen/pkg/runtime"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/labstack/echo/v4"
)

// ServerInterface represents all server handlers.
type ServerInterface interface {
	// get doc json
	// (GET /api/v1/dm.json)
	GetDocJSON(ctx echo.Context) error
	// get doc html
	// (GET /api/v1/docs)
	GetDocHTML(ctx echo.Context) error
	// get data source list
	// (GET /api/v1/sources)
	DMAPIGetSourceList(ctx echo.Context) error
	// create new data source
	// (POST /api/v1/sources)
	DMAPICreateSource(ctx echo.Context) error
	// delete a data source
	// (DELETE /api/v1/sources/{source-name})
	DMAPIDeleteSource(ctx echo.Context, sourceName string) error
	// enable relay log function for the data source
	// (PATCH /api/v1/sources/{source-name}/start-relay)
	DMAPIStartRelay(ctx echo.Context, sourceName string) error
	// get the current status of the data source
	// (GET /api/v1/sources/{source-name}/status)
	DMAPIGetSourceStatus(ctx echo.Context, sourceName string) error
	// disable relay log function for the data source
	// (PATCH /api/v1/sources/{source-name}/stop-relay)
	DMAPIStopRelay(ctx echo.Context, sourceName string) error
	// get task list
	// (GET /api/v1/tasks)
	DMAPIGetTaskList(ctx echo.Context) error
	// create and start task
	// (POST /api/v1/tasks)
	DMAPIStartTask(ctx echo.Context) error
	// delete and stop task
	// (DELETE /api/v1/tasks/{task-name})
	DMAPIDeleteTask(ctx echo.Context, taskName string) error
	// get task status
	// (GET /api/v1/tasks/{task-name}/status)
	DMAPIGetTaskStatus(ctx echo.Context, taskName string) error
}

// ServerInterfaceWrapper converts echo contexts to parameters.
type ServerInterfaceWrapper struct {
	Handler ServerInterface
}

// GetDocJSON converts echo context to params.
func (w *ServerInterfaceWrapper) GetDocJSON(ctx echo.Context) error {
	var err error

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.GetDocJSON(ctx)
	return err
}

// GetDocHTML converts echo context to params.
func (w *ServerInterfaceWrapper) GetDocHTML(ctx echo.Context) error {
	var err error

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.GetDocHTML(ctx)
	return err
}

// DMAPIGetSourceList converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPIGetSourceList(ctx echo.Context) error {
	var err error

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPIGetSourceList(ctx)
	return err
}

// DMAPICreateSource converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPICreateSource(ctx echo.Context) error {
	var err error

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPICreateSource(ctx)
	return err
}

// DMAPIDeleteSource converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPIDeleteSource(ctx echo.Context) error {
	var err error
	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameterWithLocation("simple", false, "source-name", runtime.ParamLocationPath, ctx.Param("source-name"), &sourceName)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Invalid format for parameter source-name: %s", err))
	}

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPIDeleteSource(ctx, sourceName)
	return err
}

// DMAPIStartRelay converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPIStartRelay(ctx echo.Context) error {
	var err error
	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameterWithLocation("simple", false, "source-name", runtime.ParamLocationPath, ctx.Param("source-name"), &sourceName)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Invalid format for parameter source-name: %s", err))
	}

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPIStartRelay(ctx, sourceName)
	return err
}

// DMAPIGetSourceStatus converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPIGetSourceStatus(ctx echo.Context) error {
	var err error
	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameterWithLocation("simple", false, "source-name", runtime.ParamLocationPath, ctx.Param("source-name"), &sourceName)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Invalid format for parameter source-name: %s", err))
	}

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPIGetSourceStatus(ctx, sourceName)
	return err
}

// DMAPIStopRelay converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPIStopRelay(ctx echo.Context) error {
	var err error
	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameterWithLocation("simple", false, "source-name", runtime.ParamLocationPath, ctx.Param("source-name"), &sourceName)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Invalid format for parameter source-name: %s", err))
	}

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPIStopRelay(ctx, sourceName)
	return err
}

// DMAPIGetTaskList converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPIGetTaskList(ctx echo.Context) error {
	var err error

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPIGetTaskList(ctx)
	return err
}

// DMAPIStartTask converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPIStartTask(ctx echo.Context) error {
	var err error

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPIStartTask(ctx)
	return err
}

// DMAPIDeleteTask converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPIDeleteTask(ctx echo.Context) error {
	var err error
	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameterWithLocation("simple", false, "task-name", runtime.ParamLocationPath, ctx.Param("task-name"), &taskName)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Invalid format for parameter task-name: %s", err))
	}

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPIDeleteTask(ctx, taskName)
	return err
}

// DMAPIGetTaskStatus converts echo context to params.
func (w *ServerInterfaceWrapper) DMAPIGetTaskStatus(ctx echo.Context) error {
	var err error
	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameterWithLocation("simple", false, "task-name", runtime.ParamLocationPath, ctx.Param("task-name"), &taskName)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Invalid format for parameter task-name: %s", err))
	}

	// Invoke the callback with all the unmarshalled arguments
	err = w.Handler.DMAPIGetTaskStatus(ctx, taskName)
	return err
}

// This is a simple interface which specifies echo.Route addition functions which
// are present on both echo.Echo and echo.Group, since we want to allow using
// either of them for path registration
type EchoRouter interface {
	CONNECT(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	DELETE(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	GET(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	HEAD(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	OPTIONS(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	PATCH(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	POST(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	PUT(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	TRACE(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
}

// RegisterHandlers adds each server route to the EchoRouter.
func RegisterHandlers(router EchoRouter, si ServerInterface) {
	RegisterHandlersWithBaseURL(router, si, "")
}

// Registers handlers, and prepends BaseURL to the paths, so that the paths
// can be served under a prefix.
func RegisterHandlersWithBaseURL(router EchoRouter, si ServerInterface, baseURL string) {
	wrapper := ServerInterfaceWrapper{
		Handler: si,
	}

	router.GET(baseURL+"/api/v1/dm.json", wrapper.GetDocJSON)
	router.GET(baseURL+"/api/v1/docs", wrapper.GetDocHTML)
	router.GET(baseURL+"/api/v1/sources", wrapper.DMAPIGetSourceList)
	router.POST(baseURL+"/api/v1/sources", wrapper.DMAPICreateSource)
	router.DELETE(baseURL+"/api/v1/sources/:source-name", wrapper.DMAPIDeleteSource)
	router.PATCH(baseURL+"/api/v1/sources/:source-name/start-relay", wrapper.DMAPIStartRelay)
	router.GET(baseURL+"/api/v1/sources/:source-name/status", wrapper.DMAPIGetSourceStatus)
	router.PATCH(baseURL+"/api/v1/sources/:source-name/stop-relay", wrapper.DMAPIStopRelay)
	router.GET(baseURL+"/api/v1/tasks", wrapper.DMAPIGetTaskList)
	router.POST(baseURL+"/api/v1/tasks", wrapper.DMAPIStartTask)
	router.DELETE(baseURL+"/api/v1/tasks/:task-name", wrapper.DMAPIDeleteTask)
	router.GET(baseURL+"/api/v1/tasks/:task-name/status", wrapper.DMAPIGetTaskStatus)
}

// Base64 encoded, gzipped, json marshaled Swagger object
var swaggerSpec = []string{

	"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",
	"b4em+DMZgfEnu+vdfwMAAP//RCmZx05GAAA=",
}

// GetSwagger returns the content of the embedded swagger specification file
// or error if failed to decode
func decodeSpec() ([]byte, error) {
	zipped, err := base64.StdEncoding.DecodeString(strings.Join(swaggerSpec, ""))
	if err != nil {
		return nil, fmt.Errorf("error base64 decoding spec: %s", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(zipped))
	if err != nil {
		return nil, fmt.Errorf("error decompressing spec: %s", err)
	}
	var buf bytes.Buffer
	_, err = buf.ReadFrom(zr)
	if err != nil {
		return nil, fmt.Errorf("error decompressing spec: %s", err)
	}

	return buf.Bytes(), nil
}

var rawSpec = decodeSpecCached()

// a naive cached of a decoded swagger spec
func decodeSpecCached() func() ([]byte, error) {
	data, err := decodeSpec()
	return func() ([]byte, error) {
		return data, err
	}
}

// Constructs a synthetic filesystem for resolving external references when loading openapi specifications.
func PathToRawSpec(pathToFile string) map[string]func() ([]byte, error) {
	res := make(map[string]func() ([]byte, error))
	if len(pathToFile) > 0 {
		res[pathToFile] = rawSpec
	}

	return res
}

// GetSwagger returns the Swagger specification corresponding to the generated code
// in this file. The external references of Swagger specification are resolved.
// The logic of resolving external references is tightly connected to "import-mapping" feature.
// Externally referenced files must be embedded in the corresponding golang packages.
// Urls can be supported but this task was out of the scope.
func GetSwagger() (swagger *openapi3.T, err error) {
	resolvePath := PathToRawSpec("")

	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true
	loader.ReadFromURIFunc = func(loader *openapi3.Loader, url *url.URL) ([]byte, error) {
		pathToFile := url.String()
		pathToFile = path.Clean(pathToFile)
		getSpec, ok := resolvePath[pathToFile]
		if !ok {
			err1 := fmt.Errorf("path not found: %s", pathToFile)
			return nil, err1
		}
		return getSpec()
	}
	var specData []byte
	specData, err = rawSpec()
	if err != nil {
		return
	}
	swagger, err = loader.LoadFromData(specData)
	if err != nil {
		return
	}
	return
}

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

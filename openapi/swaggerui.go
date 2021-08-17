package openapi

import (
	"bytes"
	"html/template"

	"github.com/labstack/echo/v4"
)

const (
	defaultSwaggerHost = "https://petstore3.swagger.io"
	swaggerUITemplate  = `
	<!-- HTML for static distribution bundle build -->
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>API documentation</title>
    <link rel="stylesheet" type="text/css" href="{{ .SwaggerHost }}/swagger-ui.css" >
    <link rel="icon" type="image/png" href="{{ .SwaggerHost }}/favicon-32x32.png" sizes="32x32" />
    <link rel="icon" type="image/png" href="{{ .SwaggerHost }}/favicon-16x16.png" sizes="16x16" />
    <style>
      html
      {
        box-sizing: border-box;
        overflow: -moz-scrollbars-vertical;
        overflow-y: scroll;
      }
      *,
      *:before,
      *:after
      {
        box-sizing: inherit;
      }
      body
      {
        margin:0;
        background: #fafafa;
      }
    </style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="{{ .SwaggerHost }}/swagger-ui-bundle.js"> </script>
    <script src="{{ .SwaggerHost }}/swagger-ui-standalone-preset.js"> </script>
    <script>
    window.onload = function() {
      // Begin Swagger UI call region
      const ui = SwaggerUIBundle({
        "dom_id": "#swagger-ui",
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout",
        url: "{{ .SpecJSONPath }}",
      })
      // End Swagger UI call region
      window.ui = ui
    }
  </script>
  </body>
</html>`
)

// SwaggerConfig configures the SwaggerDoc middlewares.
type SwaggerConfig struct {
	// DocPrefix the url to find the doc
	DocPath string
	// SpecJsonPath the url to find the spec
	SpecJSONPath string
	// SwaggerHost for the js that generates the swagger ui site, defaults to: http://petstore3.swagger.io/
	SwaggerHost string
}

// NewSwaggerConfig return swaggerConfig.
func NewSwaggerConfig(docPath, specJSONPath, swaggerHost string) *SwaggerConfig {
	if swaggerHost == "" {
		swaggerHost = defaultSwaggerHost
	}
	return &SwaggerConfig{
		DocPath:      docPath,
		SpecJSONPath: specJSONPath,
		SwaggerHost:  swaggerHost,
	}
}

// NewSwaggerDocUI creates a echo middleware to serve a documentation site for a swagger spec.
// This allows for altering the spec before starting the http listener.
func NewSwaggerDocUI(config *SwaggerConfig, swaggerJSON []byte) echo.MiddlewareFunc {
	// swagger html
	tmpl := template.Must(template.New("swaggerdoc").Parse(swaggerUITemplate))
	buf := bytes.NewBuffer(nil)
	_ = tmpl.Execute(buf, config)
	uiHTML := buf.Bytes()
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			path := c.Request().URL.Path
			switch {
			case path == config.DocPath:
				return c.HTML(200, string(uiHTML))
			case path == config.SpecJSONPath:
				return c.JSONBlob(200, swaggerJSON)
			}
			return next(c)
		}
	}
}

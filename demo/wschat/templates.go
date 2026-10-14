package wschat

import (
	_ "embed"
	"html/template"
	"strings"
)

//go:embed templates/index.gohtml
var indexHTML string

//go:embed templates/messages.gohtml
var messagesHTML string

//go:embed templates/channels.gohtml
var channelsHTML string

//go:embed templates/status.gohtml
var statusHTML string

var templateFuncMap = template.FuncMap{
	"join": strings.Join,
}

var (
	indexTemplate    = template.Must(template.New("index").Funcs(templateFuncMap).Parse(indexHTML))
	messagesTemplate = template.Must(indexTemplate.New("messagesPartial").Parse(messagesHTML))
	channelsTemplate = template.Must(indexTemplate.New("channelsPartial").Parse(channelsHTML))
	statusTemplate   = template.Must(indexTemplate.New("statusPartial").Parse(statusHTML))
)

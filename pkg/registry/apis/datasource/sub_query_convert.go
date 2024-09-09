package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	data "github.com/grafana/grafana-plugin-sdk-go/experimental/apis/data/v0alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"

	query "github.com/grafana/grafana/pkg/apis/query/v0alpha1"
	"github.com/grafana/grafana/pkg/web"
)

type subQueryConvertREST struct {
	builder *DataSourceAPIBuilder
}

var (
	_ rest.Storage         = (*subQueryREST)(nil)
	_ rest.Connecter       = (*subQueryREST)(nil)
	_ rest.StorageMetadata = (*subQueryREST)(nil)
)

func (r *subQueryConvertREST) New() runtime.Object {
	// This is added as the "ResponseType" regarless what ProducesObject() says :)
	return &query.QueryDataResponse{}
}

func (r *subQueryConvertREST) Destroy() {}

func (r *subQueryConvertREST) ProducesMIMETypes(verb string) []string {
	return []string{"application/json"}
}

func (r *subQueryConvertREST) ProducesObject(verb string) interface{} {
	return &query.QueryDataResponse{}
}

func (r *subQueryConvertREST) ConnectMethods() []string {
	return []string{"POST"}
}

func (r *subQueryConvertREST) NewConnectOptions() (runtime.Object, bool, string) {
	return nil, false, "" // true means you can use the trailing path as a variable
}

func (r *subQueryConvertREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	pluginCtx, err := r.builder.getPluginContext(ctx, name)
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		dqr := data.QueryDataRequest{}
		err := web.Bind(req, &dqr)
		if err != nil {
			responder.Error(err)
			return
		}

		queries, dsRef, err := data.ToDataSourceQueries(dqr)
		if err != nil {
			responder.Error(err)
			return
		}
		if dsRef != nil && dsRef.UID != name {
			responder.Error(fmt.Errorf("expected query body datasource and request to match"))
			return
		}

		ctx = backend.WithGrafanaConfig(ctx, pluginCtx.GrafanaConfig)
		ctx = contextualMiddlewares(ctx)
		pluginCtx, err := r.builder.getPluginContext(ctx, name)
		if err != nil {
			responder.Error(err)
			return
		}
		convertRequest := &backend.ConversionRequest{
			PluginContext: pluginCtx,
			TargetVersion: backend.GroupVersion{
				Group:   "query.grafana.app",
				Version: "v0alpha1",
			},
			Objects: make([]backend.RawObject, 0, len(queries)),
		}

		for _, query := range queries {
			raw, err := json.Marshal(query)
			if err != nil {
				responder.Error(fmt.Errorf("marshal: %w", err))
				return
			}
			convertRequest.Objects = append(convertRequest.Objects, backend.RawObject{
				Raw:         raw,
				ContentType: "application/json",
			})
		}

		cli, ok := r.builder.client.(PluginClientWithConversion)
		if !ok {
			responder.Error(fmt.Errorf("datasource does not support conversion"))
			return
		}

		convertResponse, err := cli.ConvertObjects(ctx, convertRequest)
		if err != nil {
			// TODO: Use convertResponse.Result to return an error?
			responder.Error(err)
			return
		}

		// TODO: Define object
		r := &query.QueryDataRequest{}
		for _, obj := range convertResponse.Objects {
			if obj.ContentType != "application/json" {
				responder.Error(fmt.Errorf("unsupported content type %s", obj.ContentType))
				return
			}
			query := &data.DataQuery{}
			err := json.Unmarshal(obj.Raw, query)
			if err != nil {
				responder.Error(fmt.Errorf("unmarshal: %w", err))
				return
			}
			r.Queries = append(r.Queries, *query)
		}
		responder.Object(http.StatusOK, r)
	}), nil
}
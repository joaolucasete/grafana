package clientmiddleware

import (
	"context"

	"github.com/grafana/grafana-plugin-sdk-go/backend"

	"github.com/grafana/grafana/pkg/plugins"
)

var _ = plugins.Client(&baseMiddleware{})

// The base middleware simply passes the request down the chain
// This allows middleware to avoid implementing all the noop functions
type baseMiddleware struct {
	next plugins.Client
}

func (m *baseMiddleware) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	return m.next.QueryData(ctx, req)
}

func (m *baseMiddleware) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	return m.next.CallResource(ctx, req, sender)
}

func (m *baseMiddleware) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	return m.next.CheckHealth(ctx, req)
}

func (m *baseMiddleware) CollectMetrics(ctx context.Context, req *backend.CollectMetricsRequest) (*backend.CollectMetricsResult, error) {
	return m.next.CollectMetrics(ctx, req)
}

func (m *baseMiddleware) SubscribeStream(ctx context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	return m.next.SubscribeStream(ctx, req)
}

func (m *baseMiddleware) PublishStream(ctx context.Context, req *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	return m.next.PublishStream(ctx, req)
}

func (m *baseMiddleware) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	return m.next.RunStream(ctx, req, sender)
}

func (m *baseMiddleware) CreateInstanceSettings(ctx context.Context, req *backend.CreateInstanceSettingsRequest) (*backend.InstanceSettingsResponse, error) {
	return m.next.CreateInstanceSettings(ctx, req)
}

func (m *baseMiddleware) UpdateInstanceSettings(ctx context.Context, req *backend.UpdateInstanceSettingsRequest) (*backend.InstanceSettingsResponse, error) {
	return m.next.UpdateInstanceSettings(ctx, req)
}

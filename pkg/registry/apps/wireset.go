package appregistry

import (
	"github.com/google/wire"

	"github.com/grafana/grafana/pkg/registry/apps/playlist"
	"github.com/grafana/grafana/pkg/services/apiserver/builder/runner"
)

var WireSet = wire.NewSet(
	ProvideRegistryServiceSink,
	wire.Bind(new(runner.RunnerInitializer), new(*Service)),

	// apps
	playlist.RegisterApp,
)

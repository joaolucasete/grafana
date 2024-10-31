package appregistry

import (
	"context"

	"github.com/grafana/grafana/pkg/registry"
	"github.com/grafana/grafana/pkg/registry/apps/playlist"
	"github.com/grafana/grafana/pkg/services/apiserver/builder/runner"
)

var (
	_ registry.BackgroundService = (*Service)(nil)
)

type Service struct {
	runner *runner.APIGroupRunner
}

// ProvideRegistryServiceSink is an entry point for each service that will force initialization
func ProvideRegistryServiceSink(
	playlistAppProvider *playlist.PlaylistAppProvider,
) *Service {
	return &Service{
		runner: runner.NewAPIGroupRunner(playlistAppProvider),
	}
}

func (s *Service) Init(cfg runner.Config) error {
	if err := s.runner.Init(cfg); err != nil {
		return err
	}
	builders := s.runner.GetBuilders()
	for _, b := range builders {
		cfg.APIRegistrar.RegisterAPI(b)
	}
	return nil
}

func (s *Service) Run(ctx context.Context) error {
	return s.runner.Run(ctx)
}

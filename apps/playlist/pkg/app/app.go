package app

import (
	"context"
	"fmt"

	"github.com/grafana/grafana-app-sdk/app"
	"github.com/grafana/grafana-app-sdk/logging"
	"github.com/grafana/grafana-app-sdk/simple"

	playlistv0alpha1 "github.com/grafana/grafana/apps/playlist/pkg/apis/playlist/v0alpha1"
	"github.com/grafana/grafana/apps/playlist/pkg/watchers"
)

func New(cfg app.Config) (app.App, error) {
	playlistWatcher, err := watchers.NewPlaylistWatcher()
	if err != nil {
		return nil, fmt.Errorf("unable to create PlaylistWatcher: %w", err)
	}

	config := simple.AppConfig{
		Name:       "playlist",
		KubeConfig: cfg.KubeConfig,
		InformerConfig: simple.AppInformerConfig{
			ErrorHandler: func(ctx context.Context, err error) {
				logging.FromContext(ctx).With("error", err).Error("Informer processing error")
			},
		},
		ManagedKinds: []simple.AppManagedKind{
			{
				Kind:    playlistv0alpha1.PlaylistKind(),
				Watcher: playlistWatcher,
			},
		},
	}

	a, err := simple.NewApp(config)
	if err != nil {
		return nil, err
	}

	err = a.ValidateManifest(cfg.ManifestData)
	if err != nil {
		return nil, err
	}

	return a, nil
}

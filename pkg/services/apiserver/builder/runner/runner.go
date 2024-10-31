package runner

import (
	"context"
	"fmt"

	"github.com/grafana/grafana-app-sdk/app"
	"github.com/grafana/grafana-app-sdk/resource"
	"github.com/grafana/grafana/pkg/services/apiserver/builder"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

type Config struct {
	RestConfig   rest.Config
	APIRegistrar builder.APIRegistrar
}

type RunnerInitializer interface {
	Init(Config) error
}

func NewAPIGroupRunner(providers ...app.Provider) *APIGroupRunner {
	return &APIGroupRunner{
		providers: providers,
	}
}

type APIGroupRunner struct {
	providers []app.Provider
	builders  []builder.APIGroupBuilder
	apps      []app.App
}

func (r *APIGroupRunner) Run(ctx context.Context) error {
	runner := app.NewMultiRunner()
	for _, a := range r.apps {
		runner.AddRunnable(a.Runner())
	}
	return runner.Run(ctx)
}

func (r *APIGroupRunner) Init(cfg Config) error {
	builders := []builder.APIGroupBuilder{}
	apps := []app.App{}
	for _, provider := range r.providers {
		manifest := provider.Manifest()
		// only support embedded manifests for the builder runner
		if manifest.Location.Type != app.ManifestLocationEmbedded {
			return fmt.Errorf("unsupported manifest location type: %s", manifest.Location.Type)
		}
		config := app.Config{
			KubeConfig:     cfg.RestConfig,
			ManifestData:   *manifest.ManifestData,
			SpecificConfig: provider.SpecificConfig(),
		}

		app, err := provider.NewApp(config)
		if err != nil {
			return err
		}
		apps = append(apps, app)

		groups := map[schema.GroupVersion][]resource.Kind{}
		for _, kind := range app.ManagedKinds() {
			groups[kind.GroupVersionKind().GroupVersion()] = append(groups[kind.GroupVersionKind().GroupVersion()], kind)
		}
		for gv, kinds := range groups {
			builders = append(builders, &AppBuilder{
				app:   app,
				gv:    gv,
				kinds: kinds,
			})
		}
	}
	r.builders = builders
	return nil
}

func (r *APIGroupRunner) GetBuilders() []builder.APIGroupBuilder {
	return r.builders
}

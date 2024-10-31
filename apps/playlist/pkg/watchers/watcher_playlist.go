package watchers

import (
	"context"
	"fmt"

	"github.com/grafana/grafana-app-sdk/logging"
	"github.com/grafana/grafana-app-sdk/operator"
	"github.com/grafana/grafana-app-sdk/resource"
	"go.opentelemetry.io/otel"

	playlist "github.com/grafana/grafana/apps/playlist/pkg/apis/playlist/v0alpha1"
)

var _ operator.ResourceWatcher = &PlaylistWatcher{}

type PlaylistWatcher struct{}

func NewPlaylistWatcher() (*PlaylistWatcher, error) {
	return &PlaylistWatcher{}, nil
}

// Add handles add events for playlist.Playlist resources.
func (s *PlaylistWatcher) Add(ctx context.Context, rObj resource.Object) error {
	ctx, span := otel.GetTracerProvider().Tracer("watcher").Start(ctx, "watcher-add")
	defer span.End()
	object, ok := rObj.(*playlist.Playlist)
	if !ok {
		return fmt.Errorf("provided object is not of type *playlist.Playlist (name=%s, namespace=%s, kind=%s)",
			rObj.GetStaticMetadata().Name, rObj.GetStaticMetadata().Namespace, rObj.GetStaticMetadata().Kind)
	}

	// TODO
	logging.FromContext(ctx).Debug("Added resource", "name", object.GetStaticMetadata().Identifier().Name)
	return nil
}

// Update handles update events for playlist.Playlist resources.
func (s *PlaylistWatcher) Update(ctx context.Context, rOld resource.Object, rNew resource.Object) error {
	ctx, span := otel.GetTracerProvider().Tracer("watcher").Start(ctx, "watcher-update")
	defer span.End()
	oldObject, ok := rOld.(*playlist.Playlist)
	if !ok {
		return fmt.Errorf("provided object is not of type *playlist.Playlist (name=%s, namespace=%s, kind=%s)",
			rOld.GetStaticMetadata().Name, rOld.GetStaticMetadata().Namespace, rOld.GetStaticMetadata().Kind)
	}

	_, ok = rNew.(*playlist.Playlist)
	if !ok {
		return fmt.Errorf("provided object is not of type *playlist.Playlist (name=%s, namespace=%s, kind=%s)",
			rNew.GetStaticMetadata().Name, rNew.GetStaticMetadata().Namespace, rNew.GetStaticMetadata().Kind)
	}

	// TODO
	logging.FromContext(ctx).Debug("Updated resource", "name", oldObject.GetStaticMetadata().Identifier().Name)
	return nil
}

// Delete handles delete events for playlist.Playlist resources.
func (s *PlaylistWatcher) Delete(ctx context.Context, rObj resource.Object) error {
	ctx, span := otel.GetTracerProvider().Tracer("watcher").Start(ctx, "watcher-delete")
	defer span.End()
	object, ok := rObj.(*playlist.Playlist)
	if !ok {
		return fmt.Errorf("provided object is not of type *playlist.Playlist (name=%s, namespace=%s, kind=%s)",
			rObj.GetStaticMetadata().Name, rObj.GetStaticMetadata().Namespace, rObj.GetStaticMetadata().Kind)
	}

	// TODO
	logging.FromContext(ctx).Debug("Deleted resource", "name", object.GetStaticMetadata().Identifier().Name)
	return nil
}

// Sync is not a standard resource.Watcher function, but is used when wrapping this watcher in an operator.OpinionatedWatcher.
// It handles resources which MAY have been updated during an outage period where the watcher was not able to consume events.
func (s *PlaylistWatcher) Sync(ctx context.Context, rObj resource.Object) error {
	ctx, span := otel.GetTracerProvider().Tracer("watcher").Start(ctx, "watcher-sync")
	defer span.End()
	object, ok := rObj.(*playlist.Playlist)
	if !ok {
		return fmt.Errorf("provided object is not of type *playlist.Playlist (name=%s, namespace=%s, kind=%s)",
			rObj.GetStaticMetadata().Name, rObj.GetStaticMetadata().Namespace, rObj.GetStaticMetadata().Kind)
	}

	// TODO
	logging.FromContext(ctx).Debug("Possible resource update", "name", object.GetStaticMetadata().Identifier().Name)
	return nil
}

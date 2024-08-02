package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"path"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
	genericapiserver "k8s.io/apiserver/pkg/server"
	clientrest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/grafana/grafana/pkg/api/routing"
	"github.com/grafana/grafana/pkg/apimachinery/identity"
	grafanaresponsewriter "github.com/grafana/grafana/pkg/apiserver/endpoints/responsewriter"
	"github.com/grafana/grafana/pkg/infra/db"
	"github.com/grafana/grafana/pkg/infra/kvstore"
	"github.com/grafana/grafana/pkg/infra/metrics"
	"github.com/grafana/grafana/pkg/infra/serverlock"
	"github.com/grafana/grafana/pkg/infra/tracing"
	"github.com/grafana/grafana/pkg/middleware"
	"github.com/grafana/grafana/pkg/modules"
	"github.com/grafana/grafana/pkg/registry"
	"github.com/grafana/grafana/pkg/services/apiserver/aggregator"
	"github.com/grafana/grafana/pkg/services/apiserver/auth/authenticator"
	"github.com/grafana/grafana/pkg/services/apiserver/auth/authorizer"
	"github.com/grafana/grafana/pkg/services/apiserver/builder"
	"github.com/grafana/grafana/pkg/services/apiserver/endpoints/request"
	grafanaapiserveroptions "github.com/grafana/grafana/pkg/services/apiserver/options"
	"github.com/grafana/grafana/pkg/services/apiserver/utils"
	contextmodel "github.com/grafana/grafana/pkg/services/contexthandler/model"
	"github.com/grafana/grafana/pkg/services/featuremgmt"
	"github.com/grafana/grafana/pkg/services/org"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/storage/unified/apistore"
	"github.com/grafana/grafana/pkg/storage/unified/resource"
	"github.com/grafana/grafana/pkg/storage/unified/sql"
)

var (
	_ Service                    = (*service)(nil)
	_ RestConfigProvider         = (*service)(nil)
	_ registry.BackgroundService = (*service)(nil)
	_ registry.CanBeDisabled     = (*service)(nil)

	Scheme = runtime.NewScheme()
	Codecs = serializer.NewCodecFactory(Scheme)

	unversionedVersion = schema.GroupVersion{Group: "", Version: "v1"}
	unversionedTypes   = []runtime.Object{
		&metav1.Status{},
		&metav1.WatchEvent{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	}
)

func init() {
	// we need to add the options to empty v1
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Group: "", Version: "v1"})
	Scheme.AddUnversionedTypes(unversionedVersion, unversionedTypes...)
}

type Service interface {
	services.NamedService
	registry.BackgroundService
	registry.CanBeDisabled
}

type RestConfigProvider interface {
	GetRestConfig() *clientrest.Config
}

type DirectRestConfigProvider interface {
	// GetDirectRestConfig returns a k8s client configuration that will use the same
	// logged logged in user as the current request context.  This is useful when
	// creating clients that map legacy API handlers to k8s backed services
	GetDirectRestConfig(c *contextmodel.ReqContext) *clientrest.Config

	// This can be used to rewrite incoming requests to path now supported under /apis
	DirectlyServeHTTP(w http.ResponseWriter, r *http.Request)
}

type service struct {
	*services.BasicService

	options    *grafanaapiserveroptions.Options
	restConfig *clientrest.Config

	cfg      *setting.Cfg
	features featuremgmt.FeatureToggles

	startedCh chan struct{}
	stopCh    chan struct{}
	stoppedCh chan error

	db       db.DB
	rr       routing.RouteRegister
	handler  http.Handler
	builders []builder.APIGroupBuilder

	tracing *tracing.TracingService
	metrics prometheus.Registerer

	authorizer        *authorizer.GrafanaAuthorizer
	serverLockService builder.ServerLockService
	kvStore           kvstore.KVStore
}

func ProvideService(
	cfg *setting.Cfg,
	features featuremgmt.FeatureToggles,
	rr routing.RouteRegister,
	orgService org.Service,
	tracing *tracing.TracingService,
	serverLockService *serverlock.ServerLockService,
	db db.DB,
	kvStore kvstore.KVStore,
) (*service, error) {
	s := &service{
		cfg:        cfg,
		features:   features,
		rr:         rr,
		startedCh:  make(chan struct{}),
		stopCh:     make(chan struct{}),
		builders:   []builder.APIGroupBuilder{},
		authorizer: authorizer.NewGrafanaAuthorizer(cfg, orgService),
		tracing:    tracing,
		db:         db, // For Unified storage
		metrics:    metrics.ProvideRegisterer(),
		kvStore:    kvStore,
	}

	// This will be used when running as a dskit service
	s.BasicService = services.NewBasicService(s.start, s.running, nil).WithName(modules.GrafanaAPIServer)

	// TODO: this is very hacky
	// We need to register the routes in ProvideService to make sure
	// the routes are registered before the Grafana HTTP server starts.
	proxyHandler := func(k8sRoute routing.RouteRegister) {
		handler := func(c *contextmodel.ReqContext) {
			<-s.startedCh
			if s.handler == nil {
				c.Resp.WriteHeader(404)
				_, _ = c.Resp.Write([]byte("Not found"))
				return
			}

			req := c.Req
			if req.URL.Path == "" {
				req.URL.Path = "/"
			}

			if c.SignedInUser != nil {
				ctx := identity.WithRequester(req.Context(), c.SignedInUser)
				req = req.WithContext(ctx)
			}

			resp := responsewriter.WrapForHTTP1Or2(c.Resp)
			s.handler.ServeHTTP(resp, req)
		}
		k8sRoute.Any("/", middleware.ReqSignedIn, handler)
		k8sRoute.Any("/*", middleware.ReqSignedIn, handler)
	}

	s.rr.Group("/apis", proxyHandler)
	s.rr.Group("/livez", proxyHandler)
	s.rr.Group("/readyz", proxyHandler)
	s.rr.Group("/healthz", proxyHandler)
	s.rr.Group("/openapi", proxyHandler)
	s.rr.Group("/version", proxyHandler)

	return s, nil
}

func (s *service) GetRestConfig() *clientrest.Config {
	return s.restConfig
}

func (s *service) IsDisabled() bool {
	return false
}

// Run is an adapter for the BackgroundService interface.
func (s *service) Run(ctx context.Context) error {
	if err := s.start(ctx); err != nil {
		return err
	}
	return s.running(ctx)
}

func (s *service) RegisterAPI(b builder.APIGroupBuilder) {
	s.builders = append(s.builders, b)
}

// nolint:gocyclo
func (s *service) start(ctx context.Context) error {
	defer close(s.startedCh)

	// Get the list of groups the server will support
	builders := s.builders

	groupVersions := make([]schema.GroupVersion, 0, len(builders))
	// Install schemas
	initialSize := len(aggregator.APIVersionPriorities)
	for i, b := range builders {
		groupVersions = append(groupVersions, b.GetGroupVersion())
		if err := b.InstallSchema(Scheme); err != nil {
			return err
		}

		if s.features.IsEnabledGlobally(featuremgmt.FlagKubernetesAggregator) {
			// set the priority for the group+version
			aggregator.APIVersionPriorities[b.GetGroupVersion()] = aggregator.Priority{Group: 15000, Version: int32(i + initialSize)}
		}

		auth := b.GetAuthorizer()
		if auth != nil {
			s.authorizer.Register(b.GetGroupVersion(), auth)
		}
	}

	o := grafanaapiserveroptions.NewOptions(Codecs.LegacyCodec(groupVersions...))
	err := applyGrafanaConfig(s.cfg, s.features, o)
	if err != nil {
		return err
	}

	if errs := o.Validate(); len(errs) != 0 {
		// TODO: handle multiple errors
		return errs[0]
	}

	serverConfig := genericapiserver.NewRecommendedConfig(Codecs)
	if err := o.ApplyTo(serverConfig); err != nil {
		return err
	}
	serverConfig.Authorization.Authorizer = s.authorizer
	serverConfig.Authentication.Authenticator = authenticator.NewAuthenticator(serverConfig.Authentication.Authenticator)
	serverConfig.TracerProvider = s.tracing.GetTracerProvider()

	// setup loopback transport for the aggregator server
	transport := &roundTripperFunc{ready: make(chan struct{})}
	serverConfig.LoopbackClientConfig.Transport = transport
	serverConfig.LoopbackClientConfig.TLSClientConfig = clientrest.TLSClientConfig{}

	switch o.StorageOptions.StorageType {
	case grafanaapiserveroptions.StorageTypeEtcd:
		if err := o.RecommendedOptions.Etcd.Validate(); len(err) > 0 {
			return err[0]
		}
		if err := o.RecommendedOptions.Etcd.ApplyTo(&serverConfig.Config); err != nil {
			return err
		}

	case grafanaapiserveroptions.StorageTypeUnified:
		if !s.features.IsEnabledGlobally(featuremgmt.FlagUnifiedStorage) {
			return fmt.Errorf("unified storage requires the unifiedStorage feature flag")
		}

		server, err := sql.ProvideResourceServer(s.db, s.cfg, s.features, s.tracing)
		if err != nil {
			return err
		}
		client := resource.NewLocalResourceStoreClient(server)
		serverConfig.Config.RESTOptionsGetter = apistore.NewRESTOptionsGetterForClient(client,
			o.RecommendedOptions.Etcd.StorageConfig)

	case grafanaapiserveroptions.StorageTypeUnifiedGrpc:
		if !s.features.IsEnabledGlobally(featuremgmt.FlagUnifiedStorage) {
			return fmt.Errorf("unified storage requires the unifiedStorage feature flag")
		}
		// Create a connection to the gRPC server
		conn, err := grpc.NewClient(o.StorageOptions.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}

		// Create a client instance
		// TODO(drclau): differentiate based on grpc_mode (e.g. on-prem vs. cloud)
		client, err := resource.NewResourceStoreClientGRPC(conn)
		if err != nil {
			return err
		}
		serverConfig.Config.RESTOptionsGetter = apistore.NewRESTOptionsGetterForClient(client, o.RecommendedOptions.Etcd.StorageConfig)

	case grafanaapiserveroptions.StorageTypeLegacy:
		fallthrough
	case grafanaapiserveroptions.StorageTypeFile:
		restOptionsGetter, err := apistore.NewRESTOptionsGetterForFile(o.StorageOptions.DataPath, o.RecommendedOptions.Etcd.StorageConfig)
		if err != nil {
			return err
		}
		serverConfig.RESTOptionsGetter = restOptionsGetter
	}

	// Add OpenAPI specs for each group+version
	err = builder.SetupConfig(
		Scheme,
		serverConfig,
		builders,
		s.cfg.BuildStamp,
		s.cfg.BuildVersion,
		s.cfg.BuildCommit,
		s.cfg.BuildBranch,
	)
	if err != nil {
		return err
	}

	// Create the server
	server, err := serverConfig.Complete().New("grafana-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return err
	}

	// Install the API group+version
	err = builder.InstallAPIs(Scheme, Codecs, server, serverConfig.RESTOptionsGetter, builders, o.StorageOptions,
		// Required for the dual writer initialization
		s.metrics, kvstore.WithNamespace(s.kvStore, 0, "storage.dualwriting"), s.serverLockService,
	)
	if err != nil {
		return err
	}

	// stash the options for later use
	s.options = o

	var runningServer *genericapiserver.GenericAPIServer
	if s.features.IsEnabledGlobally(featuremgmt.FlagKubernetesAggregator) {
		runningServer, err = s.startAggregator(transport, serverConfig, server, s.metrics)
		if err != nil {
			return err
		}
	} else {
		runningServer, err = s.startCoreServer(transport, serverConfig, server)
		if err != nil {
			return err
		}
	}

	// only write kubeconfig in dev mode
	if o.ExtraOptions.DevMode {
		if err := ensureKubeConfig(runningServer.LoopbackClientConfig, o.StorageOptions.DataPath); err != nil {
			return err
		}
	}

	// used by the proxy wrapper registered in ProvideService
	s.handler = runningServer.Handler
	// used by local clients to make requests to the server
	s.restConfig = runningServer.LoopbackClientConfig

	return nil
}

func (s *service) startCoreServer(
	transport *roundTripperFunc,
	serverConfig *genericapiserver.RecommendedConfig,
	server *genericapiserver.GenericAPIServer,
) (*genericapiserver.GenericAPIServer, error) {
	// setup the loopback transport and signal that it's ready.
	// ignore the lint error because the response is passed directly to the client,
	// so the client will be responsible for closing the response body.
	// nolint:bodyclose
	transport.fn = grafanaresponsewriter.WrapHandler(server.Handler)
	close(transport.ready)

	prepared := server.PrepareRun()
	go func() {
		s.stoppedCh <- prepared.Run(s.stopCh)
	}()

	return server, nil
}

func (s *service) startAggregator(
	transport *roundTripperFunc,
	serverConfig *genericapiserver.RecommendedConfig,
	server *genericapiserver.GenericAPIServer,
	reg prometheus.Registerer,
) (*genericapiserver.GenericAPIServer, error) {
	namespaceMapper := request.GetNamespaceMapper(s.cfg)

	aggregatorConfig, err := aggregator.CreateAggregatorConfig(s.options, *serverConfig, namespaceMapper(1))
	if err != nil {
		return nil, err
	}

	aggregatorServer, err := aggregator.CreateAggregatorServer(aggregatorConfig, server, reg)
	if err != nil {
		return nil, err
	}

	// setup the loopback transport for the aggregator server and signal that it's ready
	// ignore the lint error because the response is passed directly to the client,
	// so the client will be responsible for closing the response body.
	// nolint:bodyclose
	transport.fn = grafanaresponsewriter.WrapHandler(aggregatorServer.GenericAPIServer.Handler)
	close(transport.ready)

	prepared, err := aggregatorServer.PrepareRun()
	if err != nil {
		return nil, err
	}

	go func() {
		s.stoppedCh <- prepared.Run(s.stopCh)
	}()

	return aggregatorServer.GenericAPIServer, nil
}

func (s *service) GetDirectRestConfig(c *contextmodel.ReqContext) *clientrest.Config {
	return &clientrest.Config{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				<-s.startedCh
				ctx := identity.WithRequester(req.Context(), c.SignedInUser)
				wrapped := grafanaresponsewriter.WrapHandler(s.handler)
				return wrapped(req.WithContext(ctx))
			},
		},
	}
}

func (s *service) DirectlyServeHTTP(w http.ResponseWriter, r *http.Request) {
	<-s.startedCh
	s.handler.ServeHTTP(w, r)
}

func (s *service) running(ctx context.Context) error {
	select {
	case err := <-s.stoppedCh:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		close(s.stopCh)
	}
	return nil
}

func ensureKubeConfig(restConfig *clientrest.Config, dir string) error {
	return clientcmd.WriteToFile(
		utils.FormatKubeConfig(restConfig),
		path.Join(dir, "grafana.kubeconfig"),
	)
}

type roundTripperFunc struct {
	ready chan struct{}
	fn    func(req *http.Request) (*http.Response, error)
}

func (f *roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	if f.fn == nil {
		<-f.ready
	}
	return f.fn(req)
}

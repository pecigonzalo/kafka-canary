package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/pecigonzalo/kafka-canary/pkg/api"
	"github.com/pecigonzalo/kafka-canary/pkg/canary"
	"github.com/pecigonzalo/kafka-canary/pkg/client"
	"github.com/pecigonzalo/kafka-canary/pkg/services"
	"github.com/pecigonzalo/kafka-canary/pkg/signals"
	"github.com/pecigonzalo/kafka-canary/pkg/workers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	version              = "development"
	metrics_namespace    = "kafka_canary"
	clientCreationFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "client_creation_error_total",
		Namespace: metrics_namespace,
		Help:      "Total number of errors while creating Kafka client",
	}, nil)
)

func main() {
	fs := pflag.NewFlagSet("default", pflag.ContinueOnError)
	fs.String("host", "", "Host to bind service to")
	fs.Int("port", 9898, "HTTP port to bind service to")
	fs.String("level", "info", "log level debug, info, warn, error, fatal or panic")
	versionFlag := fs.BoolP("version", "v", false, "get version number")

	// bind flags and environment variables
	viper.BindPFlags(fs)

	// parse flags
	err := fs.Parse(os.Args[1:])
	switch {
	case err == pflag.ErrHelp:
		os.Exit(0)
	case err != nil:
		fmt.Fprintf(os.Stderr, "Error: %s\n\n", err.Error())
		fs.PrintDefaults()
		os.Exit(2)
	case *versionFlag:
		fmt.Println(version)
		os.Exit(0)
	}

	// setup logger
	level, err := zerolog.ParseLevel(viper.GetString("level"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n\n", err.Error())
		os.Exit(2)
	}
	zerolog.SetGlobalLevel(level)
	// TODO: Change to conditional
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().
		Timestamp().
		Str("service", "kafka-canary").
		Logger()

	// validate port
	if _, err := strconv.Atoi(viper.GetString("port")); err != nil {
		port, _ := fs.GetInt("port")
		viper.Set("port", strconv.Itoa(port))
	}

	// load HTTP server config
	var srvCfg api.Config
	if err := viper.Unmarshal(&srvCfg); err != nil {
		logger.Panic().Err(err).Msg("Config unmarshal failed")
	}

	// start HTTP server
	logger.Info().
		Str("version", version).
		Msg("Starting Kafka Canary")
	srv, _ := api.NewServer(&srvCfg, &logger)
	httpServer, healthy, ready := srv.ListenAndServe()

	// setup the canary services
	canaryConfig := canary.Config{
		Topic:    "__kafka_canary.3",
		ClientID: "kafka-canary-client",
		// ProducerLatencyBuckets:      []float64{2, 5, 10, 20, 50, 100, 200, 400},
		ProducerLatencyBuckets: []float64{100, 500, 1000, 1500, 2000, 4000, 8000},
		// EndToEndLatencyBuckets:      []float64{5, 10, 20, 50, 100, 200, 400, 800},
		EndToEndLatencyBuckets:      []float64{100, 500, 1000, 2000, 8000, 10000, 12000, 15000},
		ConsumerGroupID:             "kafka-canary-group",
		ReconcileInterval:           5 * time.Second,
		StatusCheckInterval:         30 * time.Second,
		BootstrapBackoffMaxAttempts: 10,
		BootstrapBackoffScale:       5 * time.Second,
	}
	connectorConfig := client.ConnectorConfig{
		BrokerAddrs: []string{
			"b-1.streamingplatformpr.ieik6g.c8.kafka.eu-central-1.amazonaws.com:9098",
			"b-2.streamingplatformpr.ieik6g.c8.kafka.eu-central-1.amazonaws.com:9098",
			"b-4.streamingplatformpr.ieik6g.c8.kafka.eu-central-1.amazonaws.com:9098",
		},
		TLS:  client.TLSConfig{Enabled: true},
		SASL: client.SASLConfig{Enabled: true, Mechanism: client.SASLMechanismAWSMSKIAM},
	}

	topicService := services.NewTopicService(canaryConfig, connectorConfig, &logger)
	producerService := services.NewProducerService(canaryConfig, connectorConfig, &logger)
	consumerService := services.NewConsumerService(canaryConfig, connectorConfig, &logger)
	connectionService := services.NewConnectionService(canaryConfig, connectorConfig)
	statusService := services.NewStatusServiceService(canaryConfig, &logger)

	// start canary manager
	canaryManager := workers.NewCanaryManager(canaryConfig, topicService, producerService, consumerService, connectionService, statusService, &logger)
	canaryManager.Start()

	// graceful shutdown
	stopCh := signals.SetupSignalHandler()
	serverShutdownTimeout := 5 * time.Second
	sd, _ := signals.NewShutdown(serverShutdownTimeout, &logger)
	sd.Graceful(stopCh, httpServer, canaryManager, healthy, ready)
}

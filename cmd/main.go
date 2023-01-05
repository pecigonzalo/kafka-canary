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
	logger := zerolog.New(os.Stdout).With().
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
		ReconcileInterval:           30 * time.Second,
		StatusCheckInterval:         30 * time.Second,
		BootstrapBackoffMaxAttempts: 10,
		BootstrapBackoffScale:       5 * time.Second,
	}
	clientConfig := client.Config{}

	producerClient, err := client.NewClient(canaryConfig, clientConfig)
	if err != nil {
		logger.Fatal().Err(err).Msg("Error creating producer client")
	}
	consumerClient, err := client.NewClient(canaryConfig, clientConfig)
	if err != nil {
		logger.Fatal().Err(err).Msg("Error creating consumer client")
	}

	topicService := services.NewTopicService(canaryConfig, clientConfig)
	producerService := services.NewProducerService(canaryConfig, producerClient)
	consumerService := services.NewConsumerService(canaryConfig, consumerClient)
	connectionService := services.NewConnectionService(canaryConfig, clientConfig)
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

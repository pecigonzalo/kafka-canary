package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pecigonzalo/kafka-canary/internal/api"
	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
	"github.com/pecigonzalo/kafka-canary/internal/services"
	"github.com/pecigonzalo/kafka-canary/internal/signals"
	"github.com/pecigonzalo/kafka-canary/internal/workers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	version              = "development"
	metricsNamespace     = "kafka_canary"
	clientCreationFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "client_creation_error_total",
		Namespace: metricsNamespace,
		Help:      "Total number of errors while creating Kafka client",
	}, nil)
)

type Config struct {
	Host    string        `mapstructure:"host"`
	Port    int           `mapstructure:"port"`
	Level   string        `mapstructure:"level"`
	Brokers []string      `mapstructure:"brokers"`
	Canary  canary.Config `mapstructure:"canary"`
	Output  string        `mapstructure:"output"`
}

func main() {
	fs := pflag.NewFlagSet("default", pflag.ContinueOnError)
	fs.String("host", "", "Host to bind service to")
	fs.Int("port", 9898, "HTTP port to bind service to")
	fs.StringSlice("brokers", []string{}, "Kafka broker address")
	fs.String("output", "json", "Output target [console, json]")
	fs.String("level", "info", "Log level [debug, info, warn, error, fatal, panic]")
	fs.String("canary.topic", "__kafka_canary", "Name of the topic used by the canary")
	fs.String("canary.client-id", "kafka-canary", "Id of the producer used by the canary")
	fs.String("canary.consumer-group-id", "kafka-canary-group", "Id of the consumer group used by the canary")
	versionFlag := fs.BoolP("version", "v", false, "get version number")

	// Bind flags and environment variables
	viper.SetEnvPrefix("KAFKA_CANARY")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.BindPFlags(fs)
	viper.AutomaticEnv()

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

	// Load config
	var config Config
	if err = viper.Unmarshal(&config); err != nil {
		fmt.Fprintf(os.Stderr, "Config unmarshal failed: %s\n\n", err.Error())
		os.Exit(2)
	}
	// TODO: Make me flags
	// Configure the canary services
	// canaryConfig.ProducerLatencyBuckets =       []float64{2, 5, 10, 20, 50, 100, 200, 400}
	config.Canary.ProducerLatencyBuckets = []float64{100, 500, 1000, 1500, 2000, 4000, 8000}
	// canaryConfig.EndToEndLatencyBuckets =       []float64{5, 10, 20, 50, 100, 200, 400, 800}
	config.Canary.EndToEndLatencyBuckets = []float64{100, 500, 1000, 2000, 8000, 10000, 12000, 15000}
	config.Canary.ReconcileInterval = 5 * time.Second
	config.Canary.StatusCheckInterval = 30 * time.Second
	config.Canary.BootstrapBackoffMaxAttempts = 10
	config.Canary.BootstrapBackoffScale = 5 * time.Second

	// Setup logger
	level, err := zerolog.ParseLevel(config.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n\n", err.Error())
		os.Exit(2)
	}
	zerolog.SetGlobalLevel(level)

	var logger zerolog.Logger
	if config.Output == "console" {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		logger = zerolog.New(os.Stderr)
	}
	logger = logger.With().
		Timestamp().
		Str("version", version).
		Str("service", "kafka-canary").
		Logger()

	// Start HTTP server
	logger.Info().
		Str("config", fmt.Sprintf("%+v", config)).
		Str("config", fmt.Sprintf("%+v", config)).
		Msg("Starting Kafka Canary")
	srvCfg := api.Config{
		Host:    config.Host,
		Port:    strconv.Itoa(config.Port),
		Service: "kafka-canary",
	}
	srv, _ := api.NewServer(&srvCfg, &logger)
	httpServer, healthy, ready := srv.ListenAndServe()

	connectorConfig := client.ConnectorConfig{
		BrokerAddrs: config.Brokers,
		TLS:         client.TLSConfig{Enabled: true},
		SASL:        client.SASLConfig{Enabled: true, Mechanism: client.SASLMechanismAWSMSKIAM},
	}

	topicService := services.NewTopicService(config.Canary, connectorConfig, &logger)
	producerService := services.NewProducerService(config.Canary, connectorConfig, &logger)
	consumerService := services.NewConsumerService(config.Canary, connectorConfig, &logger)
	connectionService := services.NewConnectionService(config.Canary, connectorConfig)
	statusService := services.NewStatusServiceService(config.Canary, &logger)

	// start canary manager
	canaryManager := workers.NewCanaryManager(config.Canary, topicService, producerService, consumerService, connectionService, statusService, &logger)
	canaryManager.Start()

	// graceful shutdown
	stopCh := signals.SetupSignalHandler()
	serverShutdownTimeout := 5 * time.Second
	sd, _ := signals.NewShutdown(serverShutdownTimeout, &logger)
	sd.Graceful(stopCh, httpServer, canaryManager, healthy, ready)
}

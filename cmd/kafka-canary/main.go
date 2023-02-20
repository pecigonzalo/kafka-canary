package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/pecigonzalo/kafka-canary/internal/api"
	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
	"github.com/pecigonzalo/kafka-canary/internal/services"
	"github.com/pecigonzalo/kafka-canary/internal/signals"
	"github.com/pecigonzalo/kafka-canary/internal/workers"
)

var (
	version = "development"
)

type Config struct {
	Level           string                 `mapstructure:"level"`
	Output          string                 `mapstructure:"output"`
	ShutdownTimeout time.Duration          `mapstructure:"shutdown-timeout"`
	Canary          canary.Config          `mapstructure:"canary"`
	API             api.Config             `mapstructure:"api"`
	Kafka           client.ConnectorConfig `mapstructure:"kafka"`
}

func main() {
	loadConfigFile()
	setupEnvVariables()

	fs := setupFlags()

	versionFlag := fs.BoolP("version", "v", false, "get version number")

	parseFlags(fs, versionFlag)

	config := loadConfig()

	logger := setupLogger(config)

	// Print config
	logger.Debug().
		Str("config", fmt.Sprintf("%+v", config)).
		Msg("Configuration")

	// Start context
	ctx := context.Background()

	// Start HTTP server
	logger.Info().
		Msg("Starting Kafka Canary")

	srvCfg := &api.Config{
		Host:    config.API.Host,
		Port:    config.API.Port,
		Service: config.API.Service,
	}
	srv, err := api.NewServer(srvCfg, &logger)
	if err != nil {
		logger.Fatal().
			Err(err).
			Msg("Error starting API service")
	}
	httpServer, healthy, ready := srv.ListenAndServe()

	connectorConfig := client.ConnectorConfig{
		BrokerAddrs: config.Kafka.BrokerAddrs,
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
	canaryManager.Start(ctx)

	// graceful shutdown
	stopCh := signals.SetupSignalHandler()
	serverShutdownTimeout := config.ShutdownTimeout
	sd, _ := signals.NewShutdown(serverShutdownTimeout, &logger)
	sd.Graceful(stopCh, httpServer, canaryManager, healthy, ready)
}

func setupFlags() *pflag.FlagSet {
	fs := pflag.NewFlagSet("default", pflag.ContinueOnError)
	fs.String("host", "", "Host to bind service to")
	fs.Int("port", 9898, "HTTP port to bind service to")
	fs.StringSlice("brokers", []string{}, "Kafka broker address")
	fs.String("output", "json", "Output target [console, json]")
	fs.String("level", "info", "Log level [debug, info, warn, error, fatal, panic]")
	fs.String("canary.topic", "__kafka_canary", "Name of the topic used by the canary")
	fs.String("canary.client-id", "kafka-canary", "Id of the producer used by the canary")
	fs.String("canary.consumer-group-id", "kafka-canary-group", "Id of the consumer group used by the canary")
	fs.StringSlice(
		"canary.producer-latency-buckets",
		[]string{"100", "500", "1000", "1500", "2000", "4000", "8000"},
		"Producer latency buckets",
	)
	fs.StringSlice(
		"canary.endtoend-latency-buckets",
		[]string{"100", "500", "1000", "2000", "8000", "10000", "12000", "15000"},
		"e2e latency buckets",
	)
	fs.Duration("canary.reconcile-interval", 5*time.Second, "Reconcile interval")
	fs.Duration("canary.status-check-interval", 30*time.Second, "Status check interval")
	fs.Int("canary.bootstrap-backoff-max-attempts", 10, "Bootstrap backoff max attempts")
	fs.Duration("canary.bootstrap-backoff-scale", 5*time.Second, "Bootstrap backoff scale")

	err := viper.BindPFlags(fs)
	if err != nil {
		exitError(err, 2, "Failed to bind flags to viper")
	}

	return fs
}

func loadConfigFile() {
	viper.SetConfigName("kafka-canary")
	viper.AddConfigPath("/etc/kafka-canary/")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if err != err.(viper.ConfigFileNotFoundError) {
			exitError(err, 2, "Load config failed")
		}
	}
}

func setupEnvVariables() {
	viper.SetEnvPrefix("KAFKA_CANARY")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}

func parseFlags(fs *pflag.FlagSet, versionFlag *bool) {
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

	fs.VisitAll(func(f *pflag.Flag) {
		configName := f.Name
		if !f.Changed && viper.IsSet(configName) {
			val := viper.Get(configName)
			err = fs.Set(f.Name, fmt.Sprintf("%v", val))
			if err != nil {
				exitError(err, 2, "Set flag error")
			}
		}
	})
}

func setupLogger(config Config) zerolog.Logger {
	level, err := zerolog.ParseLevel(config.Level)
	if err != nil {
		exitError(err, 2, "Error")
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

	return logger
}

func loadConfig() Config {
	var config Config

	if err := viper.Unmarshal(&config); err != nil {
		exitError(err, 2, "Config unmarshal failed")
	}

	return config
}

func exitError(err error, code int, msg string) {
	fmt.Fprintf(os.Stderr, "%s: %s\n\n", msg, err.Error())
	os.Exit(code)
}

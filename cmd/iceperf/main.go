// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nimbleape/iceperf-agent/client"
	"github.com/nimbleape/iceperf-agent/config"
	"github.com/nimbleape/iceperf-agent/util"
	"github.com/nimbleape/iceperf-agent/version"
	"github.com/pion/stun/v2"
	"github.com/pion/webrtc/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/rs/xid"

	// slogloki "github.com/samber/slog-loki/v3"

	slogmulti "github.com/samber/slog-multi"
	"github.com/urfave/cli/v2"

	// "github.com/grafana/loki-client-go/loki"
	loki "github.com/magnetde/slog-loki"
)

func main() {
	app := &cli.App{
		Name:        "ICEPerf",
		Usage:       "ICE Servers performance tests",
		Version:     version.Version,
		Description: "Run ICE Servers performance tests and report results",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "ICEPerf yaml config file",
			},
		},
		Action: runService,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}

func runService(ctx *cli.Context) error {
	config, err := getConfig(ctx)
	if err != nil {
		fmt.Println("Error loading config")
		return err
	}

	testRunId := xid.New()

	lvl := new(slog.LevelVar)
	lvl.Set(slog.LevelInfo)

	// Configure the logger

	var logg *slog.Logger

	if config.Logging.Loki.Enabled {

		// config, _ := loki.NewDefaultConfig(config.Logging.Loki.URL)
		// // config.TenantID = "xyz"
		// client, _ := loki.New(config)

		lokiHandler := loki.NewHandler(config.Logging.Loki.URL, loki.WithLabelsEnabled(loki.LabelAll...))

		logg = slog.New(
			slogmulti.Fanout(
				slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
					Level: slog.LevelInfo,
				}),
				lokiHandler,
			),
		).With("app", "iceperftest")

		// stop loki client and purge buffers
		defer lokiHandler.Close()

		// opts := lokirus.NewLokiHookOptions().
		// 	// Grafana doesn't have a "panic" level, but it does have a "critical" level
		// 	// https://grafana.com/docs/grafana/latest/explore/logs-integration/
		// 	WithLevelMap(lokirus.LevelMap{log.PanicLevel: "critical"}).
		// 	WithFormatter(&logrus.JSONFormatter{}).
		// 	WithStaticLabels(lokirus.Labels{
		// 		"app": "iceperftest",
		// 	})

		// if config.Logging.Loki.UseBasicAuth {
		// 	opts.WithBasicAuth(config.Logging.Loki.Username, config.Logging.Loki.Password)
		// }

		// if config.Logging.Loki.UseHeadersAuth {
		// 	httpClient := &http.Client{Transport: &transport{underlyingTransport: http.DefaultTransport, authHeaders: config.Logging.Loki.AuthHeaders}}

		// 	opts.WithHttpClient(httpClient)
		// }

		// hook := lokirus.NewLokiHookWithOpts(
		// 	config.Logging.Loki.URL,
		// 	opts,
		// 	log.InfoLevel,
		// 	log.WarnLevel,
		// 	log.ErrorLevel,
		// 	log.FatalLevel)

		// logg.AddHook(hook)

		// lokiHookConfig := &lokihook.Config{
		// 	// the loki api url
		// 	URL: config.Logging.Loki.URL,
		// 	// (optional, default: severity) the label's key to distinguish log's level, it will be added to Labels map
		// 	LevelName: "level",
		// 	// the labels which will be sent to loki, contains the {levelname: level}
		// 	Labels: map[string]string{
		// 		"app": "iceperftest",
		// 	},
		// }
		// hook, err := lokihook.NewHook(lokiHookConfig)
		// if err != nil {
		// 	log.Error(err)
		// } else {
		// 	log.AddHook(hook)
		// }

		// hook := loki.NewHook(config.Logging.Loki.URL, loki.WithLabel("app", "iceperftest"), loki.WithFormatter(&logrus.JSONFormatter{}), loki.WithLevel(log.InfoLevel))
		// defer hook.Close()

		// log.AddHook(hook)
	} else {
		handlerOpts := &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}
		logg = slog.New(slog.NewTextHandler(os.Stderr, handlerOpts))
	}
	// slog.SetDefault(logg)

	// logg.SetFormatter(&log.JSONFormatter{PrettyPrint: true})

	logger := logg.With("testRunId", testRunId)

	// TODO we will make a new client for each ICE Server URL from each provider
	// get ICE servers and loop them
	ICEServers, err := client.GetIceServers(config)
	if err != nil {
		logger.Error("Error getting ICE servers")
		//this should be a fatal
	}

	config.Registry = prometheus.NewRegistry()
	pusher := push.New("http://pushgateway:9091", "db_backup").Gatherer(config.Registry) // FIXME url and job

	for provider, iss := range ICEServers {
		providerLogger := logger.With("Provider", provider)

		providerLogger.Info("Provider Starting")

		for _, is := range iss {

			iceServerInfo, err := stun.ParseURI(is.URLs[0])

			if err != nil {
				return err
			}

			runId := xid.New()

			iceServerLogger := providerLogger.With("iceServerTestRunId", runId,
				"schemeAndProtocol", iceServerInfo.Scheme.String()+"-"+iceServerInfo.Proto.String(),
			)

			iceServerLogger.Info("Starting New Client", "iceServerHost", iceServerInfo.Host,
				"iceServerProtocol", iceServerInfo.Proto.String(),
				"iceServerPort", iceServerInfo.Port,
				"iceServerScheme", iceServerInfo.Scheme.String(),
			)
			config.Logger = iceServerLogger

			config.WebRTCConfig.ICEServers = []webrtc.ICEServer{is}
			//if the ice server is a stun then set the
			if iceServerInfo.Scheme == stun.SchemeTypeSTUN || iceServerInfo.Scheme == stun.SchemeTypeSTUNS {
				config.WebRTCConfig.ICETransportPolicy = webrtc.ICETransportPolicyAll
			} else {
				config.WebRTCConfig.ICETransportPolicy = webrtc.ICETransportPolicyRelay
			}

			timer := time.NewTimer(20 * time.Second)
			c, err := client.NewClient(config, iceServerInfo)
			if err != nil {
				return err
			}

			iceServerLogger.Info("Calling Run()")
			c.Run()
			iceServerLogger.Info("Called Run(), waiting for timer 10 seconds")
			<-timer.C
			iceServerLogger.Info("Calling Stop()")
			c.Stop()
			<-time.After(2 * time.Second)
			iceServerLogger.Info("Finished")
		}
		providerLogger.Info("Provider Finished")
	}

	// c, err := client.NewClient(config)
	// if err != nil {
	// 	return nil
	// }
	// defer c.Stop()

	// c.Run()

	util.Check(pusher.Push())

	return nil
}

func getConfig(c *cli.Context) (*config.Config, error) {
	configBody := ""
	configFile := c.String("config")
	if configFile != "" {
		content, err := os.ReadFile(configFile)
		if err != nil {
			return nil, err
		}
		configBody = string(content)
	}

	conf, err := config.NewConfig(configBody)
	if err != nil {
		return nil, err
	}

	return conf, nil
}

// func setLogLevel(logger *log.Logger, level string) {
// 	switch level {
// 	case "debug":
// 		logger.SetLevel(slog.DebugLevel)
// 	case "error":
// 		logger.SetLevel(slog.ErrorLevel)
// 	case "fatal":
// 		logger.SetLevel(slog.FatalLevel)
// 	case "panic":
// 		logger.SetLevel(slog.PanicLevel)
// 	case "trace":
// 		logger.SetLevel(slog.TraceLevel)
// 	case "warn":
// 		logger.SetLevel(slog.WarnLevel)
// 	default:
// 		logger.SetLevel(slog.InfoLevel)
// 	}
// }

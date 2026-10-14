package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbrokerclient/demo/wschat"
	"github.com/rs/zerolog"
)

var configfile = flag.String("config", "", "location of toml configuration file")

func main() {
	flag.Parse()

	// 1. Load application configuration (from file or embedded default)
	conf, err := wschat.LoadConfig(*configfile)
	if err != nil {
		log.Fatalf("cannot load configuration: %v", err)
	}

	// 2. Initialize structured logging
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	loggerZero := zerolog.New(output).With().Timestamp().Str("app", "wschat").Logger()
	var logger zLogger.ZLogger = &loggerZero

	// 3. Create thread-safe in-memory message store
	store := wschat.NewMessageStore(conf.Name, conf.Channels, 200)

	// 4. Initialize ChatClient (wsbrokerclient wrapper)
	chatClient, err := wschat.NewChatClient(conf, store, logger)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create chat client")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 5. Connect asynchronously to the broker
	if chatClient != nil {
		if err := chatClient.Start(ctx); err != nil {
			logger.Warn().Err(err).Msg("wsbroker connection error (will retry automatically)")
		}
	}

	// 6. Start the local HTTPS web server with HTMX interface
	server, err := wschat.NewServer(conf, store, chatClient, logger)
	if err != nil {
		log.Fatalf("cannot create http server: %v", err)
	}

	if err := server.Start(); err != nil {
		log.Fatalf("cannot start server: %v", err)
	}

	fmt.Println("==================================================")
	fmt.Printf(" WSChat Demo Application Started\n")
	fmt.Printf(" Client:  %s\n", conf.Name)
	fmt.Printf(" Broker:  %s\n", conf.WSBroker)
	fmt.Printf(" URL:     %s\n", server.URL())
	fmt.Println("==================================================")
	fmt.Println("Press Ctrl+C to terminate...")

	// 7. Await termination signals for graceful shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done

	fmt.Println("\nShutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// 8. Gracefully stop HTTP server and close wsbrokerclient connection
	if err := server.GracefulStop(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("error stopping http server")
	}
	if chatClient != nil {
		if err := chatClient.Close(); err != nil {
			logger.Error().Err(err).Msg("error closing chat client")
		}
	}
	fmt.Println("WSChat stopped.")
}

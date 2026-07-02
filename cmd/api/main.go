package main

import (
	"fmt"

	"github.com/Tharun-bot/goq/internal/config"
)

func main() {
	cfg := config.Load()
	fmt.Printf("GoQ API starting — redis=%s port=%s\n", cfg.RedisURL, cfg.APIPort)
}

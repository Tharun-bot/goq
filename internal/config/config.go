package config

import "os"

type Config struct {
	RedisURL    string
	DatabaseURL string
	APIPort     string
}

func Load() Config {
	return Config{
		RedisURL:    getEnv("REDIS_URL", "redis://localhost:6379"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://goq:goq@localhost:5432/goq?sslmode=disable"),
		APIPort:     getEnv("API_PORT", "8081"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

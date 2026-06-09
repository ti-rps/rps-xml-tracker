// Command migrate applies the embedded goose migrations and exits. Used as a
// one-shot compose service that tracker-api and tracker-poller depend on
// (service_completed_successfully), eliminating the cold-start race where the
// poller could start before the schema exists.
package main

import (
	"log"
	"os"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/migrate"
)

func main() {
	dsn := os.Getenv("TRACKER_PG_DSN")
	if dsn == "" {
		log.Fatal("TRACKER_PG_DSN é obrigatório")
	}
	if err := migrate.Up(dsn); err != nil {
		log.Fatalf("migração: %v", err)
	}
	log.Println("migrações aplicadas")
}

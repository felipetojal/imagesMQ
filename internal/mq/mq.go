// Package mq reúne constantes e utilitários compartilhados pelos serviços da DCompany.
package mq

import (
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// Fila de trabalho: clientes (P) -> conversores (C1, C2).
	// Cada imagem é entregue a apenas UM conversor (competing consumers).
	QueueImages = "images_queue"

	// Exchange "X" do tipo fanout: conversores -> TODOS os servidores de armazenamento.
	// Cada servidor de armazenamento tem sua própria fila ligada a esta exchange,
	// garantindo a redundância (todos recebem todas as imagens).
	ExchangeConverted = "converted_images"
)

// Env devolve o valor da variável de ambiente ou um valor padrão.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Connect conecta ao RabbitMQ com tentativas
func Connect() *amqp.Connection {
	url := Env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	var lastErr error
	for i := 1; i <= 30; i++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn
		}
		lastErr = err
		log.Printf("RabbitMQ indisponível (tentativa %d/30): %v", i, err)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("não foi possível conectar ao RabbitMQ: %v", lastErr)
	return nil
}

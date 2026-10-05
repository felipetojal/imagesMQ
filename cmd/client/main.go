package main

import (
	"context"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	"imagesMQ/internal/mq"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	clientID := mq.Env("CLIENT_ID", "1")
	dir := mq.Env("IMAGES_DIR", "./data/clients/1")
	prefix := "[cliente " + clientID + "] "
	log.SetPrefix(prefix)

	conn := mq.Connect()
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("erro ao abrir canal: %v", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(mq.QueueImages, true, false, false, false, nil); err != nil {
		log.Fatalf("erro ao declarar fila: %v", err)
	}

	// Publisher confirms: a imagem só é considerada enviada quando o broker confirma
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("erro ao habilitar confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("erro ao ler pasta %s: %v", dir, err)
	}

	// Iterando sobre as imagens do diretório para enviá-las
	sent := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext != ".jpg" && ext != ".jpeg" && ext != ".png" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			log.Printf("erro ao ler %s: %v", e.Name(), err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = ch.PublishWithContext(ctx, "", mq.QueueImages, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  mime.TypeByExtension(ext),
			Headers:      amqp.Table{"filename": e.Name(), "client_id": clientID},
			Body:         data,
		})
		cancel()
		if err != nil {
			log.Printf("erro ao publicar %s: %v", e.Name(), err)
			continue
		}
		if c := <-confirms; !c.Ack {
			log.Printf("broker não confirmou %s", e.Name())
			continue
		}
		sent++
		log.Printf("imagem enviada: %s (%d bytes)", e.Name(), len(data))
	}
	log.Printf("concluído: %d imagem(ns) enviada(s)", sent)
}

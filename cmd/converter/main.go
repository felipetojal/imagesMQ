package main

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"image/png"
	"log"
	"time"

	"imagesMQ/internal/mq"

	amqp "github.com/rabbitmq/amqp091-go"
)

// toGrayscale converte a imagem e a recodifica no MESMO formato de origem.
func toGrayscale(raw []byte) ([]byte, string, error) {
	img, format, err := image.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil, "", fmt.Errorf("decodificar: %w", err)
	}
	b := img.Bounds()
	gray := image.NewGray(b)
	draw.Draw(gray, b, img, b.Min, draw.Src)

	var out bytes.Buffer
	switch format {
	case "jpeg":
		err = jpeg.Encode(&out, gray, &jpeg.Options{Quality: 90})
	case "png":
		err = png.Encode(&out, gray)
	default:
		err = fmt.Errorf("formato não suportado: %s", format)
	}
	if err != nil {
		return nil, "", err
	}
	return out.Bytes(), format, nil
}

func main() {
	id := mq.Env("CONVERTER_ID", "1")
	log.SetPrefix("[conversor " + id + "] ")

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
	if err := ch.ExchangeDeclare(mq.ExchangeConverted, "fanout", true, false, false, false, nil); err != nil {
		log.Fatalf("erro ao declarar exchange: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("erro ao habilitar confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	// Uma mensagem por vez por conversor => balanceamento justo entre conversores.
	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("erro no Qos: %v", err)
	}
	msgs, err := ch.Consume(mq.QueueImages, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("erro ao consumir: %v", err)
	}
	log.Println("aguardando imagens...")

	for d := range msgs {
		filename, _ := d.Headers["filename"].(string)
		if filename == "" {
			log.Println("mensagem sem nome de arquivo, descartando")
			d.Nack(false, false)
			continue
		}

		gray, format, err := toGrayscale(d.Body)
		if err != nil {
			log.Printf("falha ao converter %s: %v (descartada)", filename, err)
			d.Nack(false, false)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = ch.PublishWithContext(ctx, mq.ExchangeConverted, "", false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "image/" + format,
			Headers:      amqp.Table{"filename": filename}, // mesmo nome do original
			Body:         gray,
		})
		cancel()
		if err != nil {
			log.Printf("erro ao publicar %s: %v (requeue)", filename, err)
			d.Nack(false, true)
			continue
		}
		if c := <-confirms; !c.Ack {
			log.Printf("broker não confirmou %s (requeue)", filename)
			d.Nack(false, true)
			continue
		}

		d.Ack(false)
		log.Printf("convertida: %s (%d -> %d bytes)", filename, len(d.Body), len(gray))
	}
	log.Println("canal de consumo fechado, encerrando")
}

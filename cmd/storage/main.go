package main

import (
	"log"
	"os"
	"path/filepath"

	"imagesMQ/internal/mq"
)

func main() {
	id := mq.Env("STORAGE_ID", "1")
	outDir := mq.Env("OUTPUT_DIR", "./data/servers/1")
	log.SetPrefix("[armazenamento " + id + "] ")

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("erro ao criar pasta %s: %v", outDir, err)
	}

	conn := mq.Connect()
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("erro ao abrir canal: %v", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(mq.ExchangeConverted, "fanout", true, false, false, false, nil); err != nil {
		log.Fatalf("erro ao declarar exchange: %v", err)
	}

	// Fila PRÓPRIA e durável deste servidor, ligada ao fanout:
	// cada servidor recebe uma cópia de cada imagem (redundância) e, se ficar
	// offline, as imagens aguardam na fila até ele voltar.
	queueName := "storage_queue_" + id
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		log.Fatalf("erro ao declarar fila: %v", err)
	}
	if err := ch.QueueBind(queueName, "", mq.ExchangeConverted, false, nil); err != nil {
		log.Fatalf("erro no bind: %v", err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("erro no Qos: %v", err)
	}
	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("erro ao consumir: %v", err)
	}
	log.Printf("aguardando imagens na fila %s...", queueName)

	for d := range msgs {
		filename, _ := d.Headers["filename"].(string)
		filename = filepath.Base(filename) // evita path traversal
		if filename == "" || filename == "." || filename == "/" {
			log.Println("mensagem sem nome válido, descartando")
			d.Nack(false, false)
			continue
		}

		// Escrita atômica: arquivo temporário + rename.
		dst := filepath.Join(outDir, filename)
		tmp := dst + ".tmp"
		if err := os.WriteFile(tmp, d.Body, 0o644); err != nil {
			log.Printf("erro ao gravar %s: %v (requeue)", filename, err)
			d.Nack(false, true)
			continue
		}
		if err := os.Rename(tmp, dst); err != nil {
			log.Printf("erro ao renomear %s: %v (requeue)", filename, err)
			d.Nack(false, true)
			continue
		}

		d.Ack(false)
		log.Printf("salva: %s (%d bytes)", dst, len(d.Body))
	}
	log.Println("canal de consumo fechado, encerrando")
}

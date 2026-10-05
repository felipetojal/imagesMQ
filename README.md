# ImagesMQ

## Arquitetura
- Fila de trabalho: cada imagem é processada por apenas um conversor (escalabilidade).
- Fanout: cada servidor de armazenamento tem sua fila e recebe TODAS as imagens (redundância).
- O nome do arquivo viaja no header `filename`, então a imagem salva tem o mesmo nome da original.
- Filas/exchange duráveis, mensagens persistentes, ack manual e publisher confirms.

## Estrutura
- cmd/client, cmd/converter, cmd/storage: os 3 programas
- internal/mq: constantes e conexão com retry
- data/clients/N: imagens de cada cliente | data/servers/N: saída de cada servidor de armazenamento

## Executar
    docker compose up --build

Resultado: `data/servers/1` e `data/servers/2` terão as 4 imagens em cinza, com os nomes originais.
Painel do RabbitMQ: http://localhost:15672 (guest/guest)

## Testes úteis
    # mais conversores (escalar)
    docker compose up --build --scale converter1=3

    # reenviar as imagens dos clientes
    docker compose run --rm client1
    docker compose run --rm client2

    # logs
    docker compose logs -f converter1 converter2 storage1 storage2

    # teste de redundância: derrube um storage, envie imagens e suba-o de novo
    docker compose stop storage2
    docker compose run --rm client1
    docker compose start storage2   # recebe as imagens que ficaram na fila

    # limpar
    docker compose down -v
    rm -f data/servers/1/* data/servers/2/*
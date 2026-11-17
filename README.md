# cortex9-outbox-relay-go

Outbox transaccional en Postgres, relay y consumidor idempotente (CX9-6175).

## Qué hace

1. El alta de un pedido escribe la fila de negocio (`orders`) y la fila del outbox en **una sola transacción**.
2. El relay toma filas no publicadas con `SELECT ... FOR UPDATE SKIP LOCKED`, las publica en el broker y recién después las marca como enviadas.
3. El consumidor aplica el efecto de lado (`order_effects`) y registra el `message_id` en `processed_messages` **en la misma transacción**. Un replay no duplica el efecto (`INSERT ... ON CONFLICT DO NOTHING`).

## Broker

El broker **no** es Kafka ni un bus externo. Es la tabla Postgres `broker_messages` en la misma instancia.

Para modelar un broker externo at-least-once, el relay publica y marca el outbox en **dos transacciones distintas**: primero inserta en `broker_messages` (commit propio) y después marca `published_at` en la transacción que sostiene el `FOR UPDATE`. Si el proceso muere entre esos dos commits, el relay reintenta y puede quedar un duplicado a nivel broker. El consumidor los deduplica por `message_id`.

## Cómo correr los tests

Los tests de integración y de fallas necesitan Postgres.

- Si `DATABASE_URL` está definida (como en GitHub Actions), se usa esa instancia.
- Si no, los tests levantan un PostgreSQL embebido (`github.com/fergusstrange/embedded-postgres`). En la máquina donde se midió no había servidor Postgres instalado (solo el cliente 17) y `apt` no pudo instalar el paquete del servidor.

```bash
go vet ./...
go test -race -count=1 ./...
```

El binario `cmd/worker` acepta `-mode produce|relay|consumer|run` y `-database-url`.

## Mediciones

Corridas locales el 17/11/2026 (America/Buenos_Aires), `go test -race -count=1`, PostgreSQL embebido 18.3, lote de 50, 10 000 mensajes ya persistidos en outbox antes de arrancar relay y consumidor (el tiempo de `CreateOrders` no entra en las duraciones). Una sola máquina de desarrollo; no es un bench aislado.

### Test de fallas (`TestFailureKillMidRun`)

Se arrancan dos subprocesos (`cmd/worker -mode relay` y `-mode consumer`) y se los mata con `SIGKILL` a mitad de corrida, en 8 ciclos (16 envíos de `SIGKILL` en total). Después se los deja terminar.

| Métrica | Valor medido |
| --- | --- |
| N | 10 000 |
| Kills (`SIGKILL`) | 16 |
| Filas en el broker | 10 195 |
| `message_id` distintos en el broker | 10 000 |
| Duplicados a nivel broker | 195 |
| Efectos (`order_effects`) | 10 000 |
| Efectos distintos | 10 000 |
| Pérdidas | 0 |
| Efectos duplicados | 0 |
| Duración (relay + consumidor, con kills) | 1,078 s |

### Throughput en corrida limpia (`TestThroughputCleanRun`)

Misma carga, sin kills, mismos dos subprocesos:

| Métrica | Valor medido |
| --- | --- |
| N | 10 000 |
| Duración | 959 ms |
| Throughput | 10 424 msg/s |
| Filas en el broker | 10 000 |
| Duplicados a nivel broker | 0 |

La suite completa (`go test -race -count=1 ./...`, incluyendo tests de atomicidad, `SKIP LOCKED` e idempotencia) terminó en 24,342 s en la primera corrida.

## Límites

- El broker es una tabla en la misma instancia de Postgres, no un sistema de mensajería aparte. El patrón de dos transacciones solo imita un broker externo.
- No hay cola de veneno, backoff exponencial ni particionado del outbox.
- El test de fallas cubre `SIGKILL` del proceso y el reinicio posterior; no cubre corrupción de disco ni un crash a mitad de un `fsync`.
- El unique de `order_effects(message_id)` es una red de seguridad adicional a la deduplicación en `processed_messages`.
- El PostgreSQL embebido de los tests locales se inicializó con locale `C` (encoding `SQL_ASCII`). Los payloads de la prueba son JSON ASCII; en CI se usa `postgres:17` con su encoding por defecto.
- Las cifras de throughput incluyen el arranque de los dos subprocesos y no se midieron con hardware dedicado ni warmup aislado.

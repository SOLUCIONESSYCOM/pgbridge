# PGBridge - PostgreSQL to Kafka Connector

Conector de replicación lógica de PostgreSQL a Kafka que captura cambios en tiempo real mediante Logical Replication y los distribuye a Kafka con capacidades avanzadas de filtrado, agrupación por transacciones y múltiples destinos.

## 📋 Tabla de Contenidos

- [Características](#-características)
- [Arquitectura](#-arquitectura)
- [Requisitos](#-requisitos)
- [Instalación](#-instalación)
- [Configuración](#-configuración)
- [Uso](#-uso)
- [Monitoreo](#-monitoreo)
- [Troubleshooting](#-troubleshooting)
- [Desarrollo](#-desarrollo)

## ✨ Características

### Core
- **Replicación Lógica en Tiempo Real**: Captura cambios de PostgreSQL usando Logical Replication (WAL)
- **Múltiples Targets**: Configuración de múltiples destinos (topics) por tabla
- **Agrupación por Transacciones**: Soporte para enviar transacciones completas agrupadas
- **Tracking Inteligente de LSN**: Coordinador que rastrea el progreso de cada worker y solo avanza LSN cuando no hay eventos pendientes

### Filtrado Avanzado
- Sistema flexible de filtros con múltiples operadores (`==`, `!=`, `>`, `<`, `>=`, `<=`, `in`, `not_in`, `exists`, `is_null`)
- Lógica combinada AND/OR entre condiciones
- Filtrado por acciones (insert, update, delete)
- Acceso a datos antiguos y nuevos (`old_data.field`, `new_data.field`)

### Observabilidad y Resiliencia
- Logging estructurado con niveles configurables
- Métricas Prometheus para monitoreo
- Reconexión automática con backoff exponencial
- Health checks HTTP (`/health`, `/ready`)

## 🏗️ Arquitectura

### Flujo de Datos

```
PostgreSQL (WAL) → Replicator → Decoder → Dispatcher → Workers → Kafka
                                      ↓
                              LSNCoordinator
```

### Componentes Principales

#### 1. **Replicator** (`src/postgres/replicator.go`)
- Lee mensajes del WAL de PostgreSQL mediante Logical Replication
- Gestiona el slot de replicación
- Coordina el avance de LSN con PostgreSQL
- Agrupa cambios por transacción

#### 2. **Decoder** (`src/postgres/decoder.go`)
- Convierte mensajes binarios del WAL en estructuras Go
- Mantiene un mapa de relaciones (tablas) para decodificación
- Procesa mensajes: BEGIN, INSERT, UPDATE, DELETE, COMMIT, RELATION

#### 3. **Dispatcher** (`src/pipeline/dispatcher.go`)
- Distribuye eventos a workers apropiados
- Aplica filtros configurados
- Gestiona dos tipos de workers:
  - **TableWorker**: Procesa eventos individuales
  - **TransactionWorker**: Procesa transacciones completas agrupadas

#### 4. **LSNCoordinator** (`src/pipeline/lsn_coordinator.go`)
- Rastrea el LSN de cada worker
- Calcula el LSN global mínimo (el más conservador)
- Permite que diferentes workers avancen a diferentes velocidades
- Solo avanza LSN cuando todos los mensajes son confirmados por Kafka

#### 5. **Workers** (`src/pipeline/table_worker.go`, `transaction_worker.go`)
- Procesan eventos de forma asíncrona con buffers configurables
- Serializan eventos a JSON
- Envían mensajes a Kafka y esperan confirmación
- Reportan LSN solo cuando Kafka confirma

#### 6. **KafkaSink** (`src/pipeline/sink_kafka.go`)
- Implementación del sink para Kafka
- Rastrea confirmaciones de entrega
- Gestiona transacciones pendientes
- Limpia transacciones huérfanas automáticamente

### Estructura del Proyecto

```
pgbridge/
├── cmd/
│   ├── connector.go      # Punto de entrada principal
│   └── config.json      # Archivo de configuración (ejemplo)
├── src/
│   ├── app/
│   │   └── connector.go      # Orquestador principal
│   ├── config/
│   │   └── config.go          # Carga y validación de configuración
│   ├── postgres/
│   │   ├── replicator.go      # Manejo de replicación lógica
│   │   ├── decoder.go         # Decodificación de mensajes WAL
│   │   ├── connection.go      # Gestión de conexiones
│   │   ├── publication.go     # Gestión de publicaciones
│   │   └── slot.go            # Gestión de slots
│   ├── pipeline/
│   │   ├── dispatcher.go           # Distribución de eventos
│   │   ├── lsn_coordinator.go      # Coordinación de LSN
│   │   ├── table_worker.go          # Worker para eventos individuales
│   │   ├── transaction_worker.go    # Worker para transacciones
│   │   ├── sink_kafka.go            # Sink para Kafka
│   │   └── sink.go                  # Interfaz de sink
│   ├── kafka/
│   │   ├── producer.go        # Cliente productor de Kafka
│   │   └── admin.go           # Administración de Kafka
│   ├── expressions/
│   │   ├── filter.go          # Sistema de filtrado
│   │   └── expressions.go      # Evaluador de expresiones
│   └── observability/
│       ├── logger.go          # Sistema de logging
│       └── metrics.go         # Métricas Prometheus
├── docs/
│   ├── FLUJO.md              # Documentación detallada del flujo
│   └── common_querys.sql      # Consultas SQL útiles
└── main.go                    # Entry point
```

Para una explicación detallada del flujo de datos, consulta [docs/FLUJO.md](docs/FLUJO.md).

## 📦 Requisitos

### Software
- **Go 1.25.1** o superior
- **PostgreSQL 10+** con Logical Replication habilitada
- **Kafka 2.0+** (opcional, si se usa Kafka como sink)

### PostgreSQL
- `wal_level = logical` en `postgresql.conf`
- `max_replication_slots >= 1`
- `max_wal_senders >= 1`
- Usuario con permisos de replicación (`REPLICATION`)

## 🚀 Instalación

### Desde Código Fuente

```bash
# Clonar el repositorio
git clone <repository-url>
cd pgbridge

# Instalar dependencias
go mod download

# Compilar
go build -o connector.exe ./cmd/connector.go

# O usar go run
go run main.go
```

### Docker

```bash
# Construir imagen
docker build -t pgbridge .

# Ejecutar
docker run -v /path/to/config.json:/app/config.json pgbridge
```

## ⚙️ Configuración

El conector se configura mediante el archivo `config.json` ubicado en el mismo directorio que el ejecutable.

### Estructura Básica

```json
{
    "Server": {
        "HttpPort": 9090
    },
    "Log": {
        "Enabled": true,
        "Console": true,
        "MinLevel": "INFO",
        "FilePath": "/path/to/logs"
    },
    "Postgres": {
        "ConnectionString": "host=localhost port=5432 dbname=mydb sslmode=disable",
        "User": "replication_user",
        "Password": "password",
        "SlotName": "cdc_slot",
        "WorkerBufferSize": 320,
        "GoFinalLSN": false,
        "Listeners": [...]
    },
    "Kafka": {
        "BootstrapServers": ["localhost:9092"],
        "ClientID": "postgres_connector"
    }
}
```

### Configuración de PostgreSQL

#### 1. Habilitar Logical Replication

Editar `postgresql.conf`:

```conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

Reiniciar PostgreSQL después de los cambios.

#### 2. Crear Usuario con Permisos

```sql
CREATE USER replication_user WITH REPLICATION PASSWORD 'password';
GRANT CONNECT ON DATABASE mydb TO replication_user;
```

#### 3. Crear Publicación

```sql
CREATE PUBLICATION pub_my_table FOR TABLE schema.table_name;
```

#### 4. Configurar Replica Identity (para UPDATE/DELETE)

Para que UPDATE y DELETE incluyan los datos antiguos:

```sql
ALTER TABLE schema.table_name REPLICA IDENTITY FULL;
```

### Configuración de Listeners

Cada listener define qué tablas replicar y cómo procesarlas:

```json
{
    "Publication": "pub_my_table",
    "Table": "schema.table_name",
    "Actions": ["insert", "update", "delete"],
    "Pipeline": {
        "Targets": [
            {
                "Name": "target_name",
                "Group": "group_name",
                "SendAsTransaction": true,
                "Filter": {
                    "Actions": ["insert", "update"],
                    "Conditions": [
                        {
                            "Field": "new_data.status",
                            "Operator": "!=",
                            "Value": null
                        }
                    ],
                    "Logic": "AND"
                }
            }
        ]
    }
}
```

### Parámetros de Configuración

#### Postgres
| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `ConnectionString` | string | String de conexión a PostgreSQL (sin user/password) |
| `User` | string | Usuario de PostgreSQL con permisos de replicación |
| `Password` | string | Contraseña del usuario |
| `SlotName` | string | Nombre del slot de replicación lógica |
| `WorkerBufferSize` | int | Tamaño del buffer de cada worker (default: 320) |
| `GoFinalLSN` | bool | Si es `true`, inicia desde el LSN final del slot (útil para empezar desde cero) |

#### Listeners
| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `Publication` | string | Nombre de la publicación PostgreSQL |
| `Table` | string | Tabla a replicar en formato `schema.table` |
| `Actions` | []string | Acciones a capturar: `["insert", "update", "delete"]` |
| `Pipeline` | object | Configuración opcional de procesamiento |

#### Targets
| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `Name` | string | Nombre del target (se usa como nombre del topic en Kafka) |
| `Group` | string | Grupo opcional para agrupar transacciones |
| `SendAsTransaction` | bool | Si es `true`, envía la transacción completa agrupada |
| `Filter` | object | Configuración de filtrado |

#### Filtros
| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `Actions` | []string | Acciones a filtrar: `["insert", "update", "delete"]` |
| `Conditions` | []object | Array de condiciones |
| `Logic` | string | Lógica de combinación: `"AND"` o `"OR"` |

**Condiciones:**
- `Field`: Campo a evaluar (formato: `new_data.field_name` o `old_data.field_name`)
- `Operator`: Operador (`==`, `!=`, `>`, `<`, `>=`, `<=`, `in`, `not_in`, `exists`, `is_null`)
- `Value`: Valor a comparar (puede ser `null`)

### Ejemplo Completo de Configuración

```json
{
    "Server": {
        "HttpPort": 9090
    },
    "Log": {
        "Enabled": true,
        "Console": true,
        "MinLevel": "INFO",
        "FilePath": "./logs"
    },
    "Postgres": {
        "ConnectionString": "host=localhost port=5432 dbname=mydb sslmode=disable",
        "User": "replication_user",
        "Password": "password",
        "SlotName": "cdc_slot",
        "WorkerBufferSize": 320,
        "GoFinalLSN": false,
        "Listeners": [
            {
                "Publication": "pub_users",
                "Table": "public.users",
                "Actions": ["insert", "update", "delete"],
                "Pipeline": {
                    "Targets": [
                        {
                            "Name": "users_topic",
                            "Group": "",
                            "SendAsTransaction": false,
                            "Filter": {
                                "Actions": ["insert", "update"],
                                "Conditions": [
                                    {
                                        "Field": "new_data.status",
                                        "Operator": "==",
                                        "Value": "active"
                                    }
                                ],
                                "Logic": "AND"
                            }
                        }
                    ]
                }
            }
        ]
    },
    "Kafka": {
        "BootstrapServers": ["localhost:9092"],
        "ClientID": "postgres_connector"
    }
}
```

## 🎯 Uso

### Ejecución Básica

```bash
# Ejecutar el conector
./connector.exe

# El conector iniciará:
# - Servidor HTTP en el puerto configurado (default: 9090)
# - Endpoint de métricas: http://localhost:9090/metrics
# - Health check: http://localhost:9090/health
# - Ready check: http://localhost:9090/ready
```

### Endpoints HTTP

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/health` | GET | Health check básico |
| `/ready` | GET | Ready check (verifica que el conector esté listo) |
| `/metrics` | GET | Métricas Prometheus |

### Modos de Operación

#### Eventos Individuales
Por defecto, cada evento (INSERT, UPDATE, DELETE) se envía individualmente a Kafka.

#### Transacciones Agrupadas
Si `SendAsTransaction: true` y se especifica un `Group`, todos los eventos de una transacción se agrupan y se envían como un solo mensaje.

## 📊 Monitoreo

### Métricas Prometheus

El conector expone métricas en `/metrics`:

#### LSN (Log Sequence Number)
- `connector_global_lsn`: LSN global mínimo de todos los workers (indica el progreso más conservador)
- `connector_wal_end`: Posición WAL End leída del último keepalive de PostgreSQL
- `connector_last_keepalive_timestamp`: Timestamp Unix del último keepalive recibido

#### Transacciones
- `connector_transactions_processed_total`: Contador total de transacciones procesadas
- `connector_transactions_processed_by_worker_total`: Contador por worker (labels: `worker`, `type`)
  - `type` puede ser: `table` o `transaction`

#### Workers
- `connector_events_in_process_by_worker`: Número de eventos actualmente en proceso por worker (labels: `worker`, `type`)
- `connector_worker_buffer_size`: Tamaño del buffer configurado para cada worker (labels: `worker`, `type`)

### Consultas Prometheus Útiles

```promql
# Tasa de transacciones procesadas por segundo
rate(connector_transactions_processed_total[5m])

# Transacciones procesadas por worker en la última hora
increase(connector_transactions_processed_by_worker_total[1h])

# Lag de LSN (diferencia entre WAL End y Global LSN)
connector_wal_end - connector_global_lsn

# Eventos en cola por worker
connector_events_in_process_by_worker

# Tiempo desde el último keepalive
time() - connector_last_keepalive_timestamp
```

### Alertas Recomendadas

```yaml
# Lag de LSN muy alto (más de 1GB)
- alert: HighLSNLag
  expr: (connector_wal_end - connector_global_lsn) > 1073741824
  for: 5m
  annotations:
    summary: "LSN lag muy alto en pgbridge"

# Sin keepalive en los últimos 30 segundos
- alert: NoKeepalive
  expr: (time() - connector_last_keepalive_timestamp) > 30
  for: 1m
  annotations:
    summary: "No se reciben keepalives de PostgreSQL"

# Buffer de worker casi lleno (>80%)
- alert: WorkerBufferHigh
  expr: connector_events_in_process_by_worker / connector_worker_buffer_size > 0.8
  for: 5m
  annotations:
    summary: "Buffer de worker casi lleno"
```

## 🔧 Troubleshooting

### El slot de replicación crece indefinidamente

**Síntomas:** El slot de replicación consume cada vez más espacio en disco.

**Causas posibles:**
- Los workers no están procesando eventos
- Kafka no está disponible o no está recibiendo mensajes
- Errores de procesamiento que impiden el avance de LSN

**Solución:**
1. Verificar que los workers estén procesando: revisar métricas `connector_events_in_process_by_worker`
2. Verificar conectividad a Kafka: revisar logs para errores de conexión
3. Revisar logs para errores de procesamiento
4. Verificar que el LSN esté avanzando: `connector_global_lsn` debe incrementar

### Eventos no llegan a Kafka

**Síntomas:** Los eventos se procesan pero no aparecen en Kafka.

**Causas posibles:**
- Problemas de conectividad a Kafka
- Configuración incorrecta de bootstrap servers
- Errores de serialización

**Solución:**
1. Verificar conectividad a Kafka: `telnet <kafka_host> <kafka_port>`
2. Verificar configuración de `BootstrapServers` en `config.json`
3. Revisar logs para errores de producción
4. Verificar que los topics existan en Kafka (o que auto-create esté habilitado)

### LSN no avanza

**Síntomas:** El LSN global se mantiene constante aunque hay actividad.

**Causas posibles:**
- Los workers no están reportando LSN
- Kafka no está confirmando mensajes
- Hay eventos pendientes en los buffers

**Solución:**
1. Verificar que los workers estén reportando LSN: revisar logs con nivel TRACE
2. Verificar que Kafka esté confirmando mensajes: revisar métricas de delivery
3. Verificar eventos pendientes: `connector_events_in_process_by_worker`
4. Revisar el coordinador de LSN en los logs

### Errores de conexión a PostgreSQL

**Síntomas:** El conector no puede conectarse a PostgreSQL.

**Solución:**
1. Verificar que PostgreSQL esté ejecutándose
2. Verificar credenciales en `config.json`
3. Verificar que el usuario tenga permisos de replicación
4. Verificar que `wal_level = logical` esté configurado
5. Verificar conectividad de red

### Consultas SQL Útiles

Ver consultas SQL comunes en [docs/common_querys.sql](docs/common_querys.sql):

```sql
-- Verificar configuración de replicación
SELECT name, setting, unit, short_desc 
FROM pg_settings 
WHERE name IN ('wal_level', 'max_wal_senders', 'max_replication_slots');

-- Ver estado de replicación
SELECT * FROM pg_stat_replication;

-- Ver slots de replicación
SELECT * FROM pg_replication_slots;

-- Ver publicaciones
SELECT pubname, schemaname, tablename 
FROM pg_publication_tables;
```

## 💻 Desarrollo

### Estructura del Código

- El código sigue convenciones estándar de Go
- Interfaces bien definidas para extensibilidad
- Separación clara de responsabilidades
- Manejo robusto de errores y panics

### Compilar y Ejecutar

```bash
# Desarrollo
go run main.go

# Compilar
go build -o connector.exe ./cmd/connector.go

# Ejecutar tests (cuando estén disponibles)
go test ./...
```

### Extensibilidad

El conector está diseñado para ser extensible:

- **Sinks**: Implementar la interfaz `EventSink` para nuevos destinos (además de Kafka)
- **Filtros**: Extender el sistema de filtros en `src/expressions/`
- **Workers**: Crear nuevos tipos de workers si es necesario

### Contribuir

1. Crear una rama para la feature
2. Implementar cambios siguiendo las convenciones del proyecto
3. Agregar tests cuando sea posible
4. Actualizar documentación si es necesario
5. Crear pull request

## 📚 Documentación Adicional

- [Flujo de Datos Detallado](docs/FLUJO.md): Explicación paso a paso del flujo desde PostgreSQL hasta Kafka
- [Consultas SQL Comunes](docs/common_querys.sql): Consultas útiles para gestión y troubleshooting

## 🔒 Garantías del Sistema

### ¿Qué garantiza el conector?

1. **Orden**: Los eventos se procesan en el orden del WAL
2. **No pérdida (dentro de límites)**: Si Kafka confirma, el evento fue procesado
3. **At-least-once**: En caso de fallos, puede haber duplicados (no se garantiza exactly-once)

### ¿Qué NO garantiza?

1. **Exactly-once**: Puede haber duplicados en caso de fallos
2. **Inmediatez**: Hay latencia entre el commit y la producción en Kafka
3. **Orden entre topics**: Si un evento va a múltiples topics, no se garantiza orden entre ellos

## 📄 Licencia

[Especificar licencia]

## 🤝 Soporte

[Información de contacto/soporte]

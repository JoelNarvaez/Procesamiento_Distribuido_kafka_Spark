import csv
import json
import random
from pathlib import Path
from faker import Faker


TOTAL_REGISTROS = 100000

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
DATABASE_DIR = BASE_DIR / "database"

CSV_PATH = RAW_DIR / "logs_metricas.csv"
JSON_PATH = RAW_DIR / "logs_metricas.json"
SQL_PATH = RAW_DIR / "logs_metricas.sql"
INSERTS_PATH = DATABASE_DIR / "inserts.sql"

fake = Faker("es_MX")


SERVIDORES = [
    "server-01",
    "server-02",
    "server-03",
    "server-04",
    "server-05"
]

SERVICIOS = [
    "api-gateway",
    "auth-service",
    "database-service",
    "payment-service",
    "notification-service",
    "web-server",
    "cache-service",
    "logging-service"
]

TIPOS_EVENTO = [
    "request",
    "error",
    "resource",
    "network",
    "security"
]

NIVELES = [
    "INFO",
    "WARNING",
    "ERROR",
    "CRITICAL"
]

ENDPOINTS = [
    "/api/login",
    "/api/logout",
    "/api/usuarios",
    "/api/productos",
    "/api/pagos",
    "/api/reportes",
    "/api/notificaciones",
    "/api/ordenes",
    "/api/metricas",
    "/api/seguridad"
]

MENSAJES = {
    "INFO": [
        "Solicitud procesada correctamente",
        "Servicio funcionando con normalidad",
        "Evento registrado correctamente",
        "Consulta procesada sin errores"
    ],
    "WARNING": [
        "Uso elevado de recursos",
        "Latencia superior al promedio",
        "Tiempo de respuesta elevado",
        "Incremento inusual de peticiones"
    ],
    "ERROR": [
        "Error interno del servicio",
        "Fallo al procesar la solicitud",
        "Error de conexión con la base de datos",
        "Servicio respondió con error"
    ],
    "CRITICAL": [
        "Servidor en estado crítico",
        "Servicio no disponible",
        "Consumo extremo de recursos",
        "Falla crítica detectada en el nodo"
    ]
}


def generar_codigo_estado(nivel):
    if nivel == "INFO":
        return random.choice([200, 201, 204])
    if nivel == "WARNING":
        return random.choice([300, 301, 302, 400, 401, 403, 404])
    if nivel == "ERROR":
        return random.choice([500, 502, 503, 504])
    return random.choice([500, 503, 504])


def generar_metricas_por_nivel(nivel):
    if nivel == "CRITICAL":
        return {
            "tiempo_respuesta_ms": random.randint(1200, 5000),
            "uso_cpu_porcentaje": round(random.uniform(85, 99), 2),
            "uso_ram_porcentaje": round(random.uniform(85, 99), 2),
            "uso_disco_porcentaje": round(random.uniform(80, 98), 2),
            "errores_minuto": random.randint(20, 80),
            "latencia_red_ms": random.randint(250, 1000),
            "temperatura_cpu": round(random.uniform(80, 100), 2)
        }

    if nivel == "ERROR":
        return {
            "tiempo_respuesta_ms": random.randint(700, 2500),
            "uso_cpu_porcentaje": round(random.uniform(60, 90), 2),
            "uso_ram_porcentaje": round(random.uniform(60, 92), 2),
            "uso_disco_porcentaje": round(random.uniform(50, 90), 2),
            "errores_minuto": random.randint(5, 35),
            "latencia_red_ms": random.randint(120, 600),
            "temperatura_cpu": round(random.uniform(65, 90), 2)
        }

    if nivel == "WARNING":
        return {
            "tiempo_respuesta_ms": random.randint(300, 1200),
            "uso_cpu_porcentaje": round(random.uniform(45, 80), 2),
            "uso_ram_porcentaje": round(random.uniform(45, 85), 2),
            "uso_disco_porcentaje": round(random.uniform(40, 85), 2),
            "errores_minuto": random.randint(1, 10),
            "latencia_red_ms": random.randint(80, 300),
            "temperatura_cpu": round(random.uniform(55, 80), 2)
        }

    return {
        "tiempo_respuesta_ms": random.randint(20, 350),
        "uso_cpu_porcentaje": round(random.uniform(5, 55), 2),
        "uso_ram_porcentaje": round(random.uniform(10, 65), 2),
        "uso_disco_porcentaje": round(random.uniform(20, 70), 2),
        "errores_minuto": random.randint(0, 3),
        "latencia_red_ms": random.randint(1, 100),
        "temperatura_cpu": round(random.uniform(35, 65), 2)
    }


def generar_registro(id_log):
    servidor = random.choice(SERVIDORES)
    servicio = random.choice(SERVICIOS)
    tipo_evento = random.choice(TIPOS_EVENTO)

    nivel = random.choices(
        NIVELES,
        weights=[70, 18, 9, 3],
        k=1
    )[0]

    metricas = generar_metricas_por_nivel(nivel)

    registro = {
        "id_log": id_log,
        "timestamp_evento": fake.date_time_between(
            start_date="-30d",
            end_date="now"
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "servidor": servidor,
        "ip_servidor": fake.ipv4_private(),
        "servicio": servicio,
        "tipo_evento": tipo_evento,
        "nivel": nivel,
        "codigo_estado": generar_codigo_estado(nivel),
        "endpoint": random.choice(ENDPOINTS),
        "usuario": fake.user_name(),
        "ciudad": fake.city(),
        "tiempo_respuesta_ms": metricas["tiempo_respuesta_ms"],
        "uso_cpu_porcentaje": metricas["uso_cpu_porcentaje"],
        "uso_ram_porcentaje": metricas["uso_ram_porcentaje"],
        "uso_disco_porcentaje": metricas["uso_disco_porcentaje"],
        "bytes_entrada": random.randint(500, 500000),
        "bytes_salida": random.randint(500, 1000000),
        "peticiones_por_minuto": random.randint(10, 3000),
        "conexiones_activas": random.randint(1, 1000),
        "errores_minuto": metricas["errores_minuto"],
        "latencia_red_ms": metricas["latencia_red_ms"],
        "temperatura_cpu": metricas["temperatura_cpu"],
        "mensaje": random.choice(MENSAJES[nivel])
    }

    return registro


def escapar_sql(valor):
    if isinstance(valor, str):
        return "'" + valor.replace("'", "''") + "'"
    return str(valor)


def generar_archivos():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)

    registros = []

    print(f"Generando {TOTAL_REGISTROS} registros con Faker...")

    for i in range(1, TOTAL_REGISTROS + 1):
        registros.append(generar_registro(i))

    campos = list(registros[0].keys())

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as archivo_csv:
        writer = csv.DictWriter(archivo_csv, fieldnames=campos)
        writer.writeheader()
        writer.writerows(registros)

    with open(JSON_PATH, "w", encoding="utf-8") as archivo_json:
        json.dump(registros, archivo_json, indent=2, ensure_ascii=False)

    insert_header = "INSERT INTO logs_metricas_servidores (\n"
    insert_header += ", ".join(campos)
    insert_header += "\n) VALUES\n"

    valores_sql = []

    for registro in registros:
        valores = [escapar_sql(registro[campo]) for campo in campos]
        valores_sql.append("(" + ", ".join(valores) + ")")

    contenido_sql = "USE monitoreo_servidores;\n\n"
    contenido_sql += insert_header
    contenido_sql += ",\n".join(valores_sql)
    contenido_sql += ";\n"

    with open(SQL_PATH, "w", encoding="utf-8") as archivo_sql:
        archivo_sql.write(contenido_sql)

    with open(INSERTS_PATH, "w", encoding="utf-8") as archivo_inserts:
        archivo_inserts.write(contenido_sql)

    print("Archivos generados correctamente:")
    print(f"- {CSV_PATH}")
    print(f"- {JSON_PATH}")
    print(f"- {SQL_PATH}")
    print(f"- {INSERTS_PATH}")


if __name__ == "__main__":
    generar_archivos()
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "producer-logs-servidores",
  brokers: ["localhost:9092"]
});

const producer = kafka.producer();

const TOPICS = [
  "metricas_recursos",
  "logs_http",
  "logs_errores",
  "metricas_red",
  "logs_seguridad"
];

const servicios = [
  "api-gateway",
  "auth-service",
  "database-service",
  "payment-service",
  "notification-service",
  "web-server"
];

const niveles = ["INFO", "WARNING", "ERROR", "CRITICAL"];

const endpoints = [
  "/api/login",
  "/api/logout",
  "/api/usuarios",
  "/api/productos",
  "/api/pagos",
  "/api/reportes",
  "/api/metricas"
];

function randomItem(lista) {
  return lista[Math.floor(Math.random() * lista.length)];
}

function randomNumber(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomDecimal(min, max) {
  return Number((Math.random() * (max - min) + min).toFixed(2));
}

function generarCodigoEstado(nivel) {
  if (nivel === "INFO") return randomItem([200, 201, 204]);
  if (nivel === "WARNING") return randomItem([300, 301, 400, 401, 403, 404]);
  return randomItem([500, 502, 503, 504]);
}

function generarMetricas(nivel) {
  if (nivel === "CRITICAL") {
    return {
      tiempo_respuesta_ms: randomNumber(1200, 5000),
      uso_cpu_porcentaje: randomDecimal(85, 99),
      uso_ram_porcentaje: randomDecimal(85, 99),
      uso_disco_porcentaje: randomDecimal(80, 98),
      errores_minuto: randomNumber(20, 80),
      latencia_red_ms: randomNumber(250, 1000),
      temperatura_cpu: randomDecimal(80, 100)
    };
  }

  if (nivel === "ERROR") {
    return {
      tiempo_respuesta_ms: randomNumber(700, 2500),
      uso_cpu_porcentaje: randomDecimal(60, 90),
      uso_ram_porcentaje: randomDecimal(60, 92),
      uso_disco_porcentaje: randomDecimal(50, 90),
      errores_minuto: randomNumber(5, 35),
      latencia_red_ms: randomNumber(120, 600),
      temperatura_cpu: randomDecimal(65, 90)
    };
  }

  if (nivel === "WARNING") {
    return {
      tiempo_respuesta_ms: randomNumber(300, 1200),
      uso_cpu_porcentaje: randomDecimal(45, 80),
      uso_ram_porcentaje: randomDecimal(45, 85),
      uso_disco_porcentaje: randomDecimal(40, 85),
      errores_minuto: randomNumber(1, 10),
      latencia_red_ms: randomNumber(80, 300),
      temperatura_cpu: randomDecimal(55, 80)
    };
  }

  return {
    tiempo_respuesta_ms: randomNumber(20, 350),
    uso_cpu_porcentaje: randomDecimal(5, 55),
    uso_ram_porcentaje: randomDecimal(10, 65),
    uso_disco_porcentaje: randomDecimal(20, 70),
    errores_minuto: randomNumber(0, 3),
    latencia_red_ms: randomNumber(1, 100),
    temperatura_cpu: randomDecimal(35, 65)
  };
}

function obtenerTopicPorTipo(tipoEvento) {
  if (tipoEvento === "resource") return "metricas_recursos";
  if (tipoEvento === "request") return "logs_http";
  if (tipoEvento === "error") return "logs_errores";
  if (tipoEvento === "network") return "metricas_red";
  if (tipoEvento === "security") return "logs_seguridad";
  return "logs_http";
}

function generarEvento(id) {
  const tiposEvento = ["request", "error", "resource", "network", "security"];
  const tipo_evento = randomItem(tiposEvento);
  const nivel = randomItem(niveles);
  const servidorNumero = randomNumber(1, 5);
  const metricas = generarMetricas(nivel);

  return {
    id_log: id,
    timestamp_evento: new Date().toISOString(),
    servidor: `server-0${servidorNumero}`,
    ip_servidor: `192.168.1.10${servidorNumero}`,
    servicio: randomItem(servicios),
    tipo_evento,
    nivel,
    codigo_estado: generarCodigoEstado(nivel),
    endpoint: randomItem(endpoints),
    usuario: `usuario_${randomNumber(1, 500)}`,
    ciudad: randomItem(["Aguascalientes", "Guadalajara", "CDMX", "Monterrey", "Zacatecas"]),
    tiempo_respuesta_ms: metricas.tiempo_respuesta_ms,
    uso_cpu_porcentaje: metricas.uso_cpu_porcentaje,
    uso_ram_porcentaje: metricas.uso_ram_porcentaje,
    uso_disco_porcentaje: metricas.uso_disco_porcentaje,
    bytes_entrada: randomNumber(500, 500000),
    bytes_salida: randomNumber(500, 1000000),
    peticiones_por_minuto: randomNumber(10, 3000),
    conexiones_activas: randomNumber(1, 1000),
    errores_minuto: metricas.errores_minuto,
    latencia_red_ms: metricas.latencia_red_ms,
    temperatura_cpu: metricas.temperatura_cpu,
    mensaje: `Evento ${nivel} generado por ${tipo_evento}`
  };
}

async function iniciarProducer() {
  try {
    await producer.connect();
    console.log("Productor conectado a Kafka");

    for (let i = 1; i <= 20; i++) {
      const evento = generarEvento(i);
      const topic = obtenerTopicPorTipo(evento.tipo_evento);

      await producer.send({
        topic,
        messages: [
          {
            key: evento.servidor,
            value: JSON.stringify(evento)
          }
        ]
      });

      console.log(`Mensaje enviado al tópico ${topic}:`, evento);
    }

    await producer.disconnect();
    console.log("Productor desconectado");
  } catch (error) {
    console.error("Error en el productor:", error);
    process.exit(1);
  }
}

iniciarProducer();
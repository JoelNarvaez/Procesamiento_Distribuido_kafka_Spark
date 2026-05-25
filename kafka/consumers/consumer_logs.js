const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "consumer-logs-servidores",
  brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({
  groupId: "grupo-monitoreo-servidores"
});

const TOPICS = [
  "metricas_recursos",
  "logs_http",
  "logs_errores",
  "metricas_red",
  "logs_seguridad"
];

async function iniciarConsumer() {
  try {
    await consumer.connect();
    console.log("Consumidor conectado a Kafka");

    for (const topic of TOPICS) {
      await consumer.subscribe({
        topic,
        fromBeginning: true
      });

      console.log(`Suscrito al tópico: ${topic}`);
    }

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const valor = message.value.toString();

        try {
          const evento = JSON.parse(valor);

          console.log("====================================");
          console.log(`Tópico: ${topic}`);
          console.log(`Partición: ${partition}`);
          console.log(`Offset: ${message.offset}`);
          console.log(`ID Log: ${evento.id_log}`);
          console.log(`Servidor: ${evento.servidor}`);
          console.log(`Servicio: ${evento.servicio}`);
          console.log(`Tipo evento: ${evento.tipo_evento}`);
          console.log(`Nivel: ${evento.nivel}`);
          console.log(`CPU: ${evento.uso_cpu_porcentaje}%`);
          console.log(`RAM: ${evento.uso_ram_porcentaje}%`);
          console.log(`Latencia: ${evento.latencia_red_ms} ms`);
          console.log(`Mensaje: ${evento.mensaje}`);
        } catch (error) {
          console.error("Error al procesar mensaje:", error.message);
          console.error("Mensaje recibido:", valor);
        }
      }
    });
  } catch (error) {
    console.error("Error en el consumidor:", error.message);
    process.exit(1);
  }
}

iniciarConsumer();
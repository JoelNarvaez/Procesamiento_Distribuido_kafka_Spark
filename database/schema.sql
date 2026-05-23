CREATE DATABASE IF NOT EXISTS monitoreo_servidores;

USE monitoreo_servidores;

DROP TABLE IF EXISTS logs_metricas_servidores;

CREATE TABLE logs_metricas_servidores (
  id_log INT PRIMARY KEY,
  timestamp_evento DATETIME NOT NULL,
  servidor VARCHAR(50) NOT NULL,
  ip_servidor VARCHAR(45) NOT NULL,
  servicio VARCHAR(100) NOT NULL,
  tipo_evento VARCHAR(50) NOT NULL,
  nivel VARCHAR(20) NOT NULL,
  codigo_estado INT,
  endpoint VARCHAR(150),
  usuario VARCHAR(100),
  ciudad VARCHAR(100),
  tiempo_respuesta_ms INT,
  uso_cpu_porcentaje DECIMAL(5,2),
  uso_ram_porcentaje DECIMAL(5,2),
  uso_disco_porcentaje DECIMAL(5,2),
  bytes_entrada BIGINT,
  bytes_salida BIGINT,
  peticiones_por_minuto INT,
  conexiones_activas INT,
  errores_minuto INT,
  latencia_red_ms INT,
  temperatura_cpu DECIMAL(5,2),
  mensaje TEXT
);
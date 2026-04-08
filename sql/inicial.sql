-- Crear schema
CREATE SCHEMA IF NOT EXISTS verin_dw;

-- Crear tabla temporalidad
CREATE TABLE IF NOT EXISTS verin_dw.dim_fecha (
    id_fecha DATE PRIMARY KEY,      
    anio INT NOT NULL,          
    mes INT,                    
    mes_nombre VARCHAR(20),
    mes_nombre_gl VARCHAR(20),         
    dia INT,                        
    trimestre INT                     
);

-- Crear tabla temporalidad para el nombre del mes
CREATE TABLE IF NOT EXISTS verin_dw.dim_mes (
    id_mes INT PRIMARY KEY, 
    nombre_es VARCHAR(20) NOT NULL, 
    nombre_gl VARCHAR(20) NOT NULL  
);

-- Insertar datos en la tabla dim_mes
INSERT INTO verin_dw.dim_mes (id_mes, nombre_es, nombre_gl)
VALUES
(1, 'Enero', 'Xaneiro'),
(2, 'Febrero', 'Febreiro'),
(3, 'Marzo', 'Marzo'),
(4, 'Abril', 'Abril'),
(5, 'Mayo', 'Maio'),
(6, 'Junio', 'Xuño'),
(7, 'Julio', 'Xullo'),
(8, 'Agosto', 'Agosto'),
(9, 'Septiembre', 'Setembro'),
(10, 'Octubre', 'Outubro'),
(11, 'Noviembre', 'Novembro'),
(12, 'Diciembre', 'Decembro')
ON CONFLICT (id_mes) DO NOTHING;

-- Función para cargar la tabla dim_fecha
CREATE OR REPLACE FUNCTION verin_dw.cargar_dim_fecha(ano INT)
RETURNS VOID AS $$
BEGIN
    INSERT INTO verin_dw.dim_fecha (id_fecha, anio, mes, mes_nombre, mes_nombre_gl, dia, trimestre)
    SELECT 
        gs::date AS id_fecha,
        EXTRACT(YEAR FROM gs)::int AS anio,
        EXTRACT(MONTH FROM gs)::int AS mes,
        dm.nombre_es AS mes_nombre,
        dm.nombre_gl AS mes_nombre_gl,
        EXTRACT(DAY FROM gs)::int AS dia,
        EXTRACT(QUARTER FROM gs)::int AS trimestre
    FROM generate_series(
        make_date(ano, 1, 1),
        make_date(ano, 12, 31),
        '1 day'
    ) AS gs
    JOIN verin_dw.dim_mes dm
      ON dm.id_mes = EXTRACT(MONTH FROM gs)::int
    ON CONFLICT (id_fecha) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Cargar datos en la tabla dim_fecha para los años 2002 a 2025
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 2002..2025 LOOP
        PERFORM verin_dw.cargar_dim_fecha(y);
    END LOOP;
END;
$$;

-- Crear tabla de hechos de municipios de la comarca de Verín
CREATE TABLE IF NOT EXISTS verin_dw.dim_municipio (
    id_municipio INT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Insertar municipios de la comarca de Verín
INSERT INTO verin_dw.dim_municipio (id_municipio, nombre) VALUES 
(32021, 'Castrelo do Val'),
(32028, 'Cualedro'),
(32039, 'Laza'),
(32050, 'Monterrei'),
(32053, 'Oímbra'),
(32071, 'Riós'),
(32085, 'Verín'),
(32091, 'Vilardevós')
ON CONFLICT (id_municipio) DO NOTHING;
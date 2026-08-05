--Tabla que indica el tipo de negocio de restauración, ya sea restaurante, bar, cafetería, etc.
CREATE TABLE IF NOT EXISTS verin_dw.dim_tipo_negocio (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL
);

--Insertamos los tipos de negocio
INSERT INTO verin_dw.dim_tipo_negocio (nombre) VALUES
('Restaurante'),
('Bar'),
('Cafetería'),
('Otros');


--Tabla que almacena la información de los restaurantes, bares de Verín
CREATE TABLE IF NOT EXISTS verin_dw.data_restauracion (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    direccion VARCHAR(255),
    tipo INT NOT NULL,
    abierto BOOLEAN NOT NULL,
    contacto VARCHAR(255),
    rating FLOAT,
    numero_valoraciones INT,
    url VARCHAR(255),
    FOREIGN KEY (tipo) REFERENCES verin_dw.dim_tipo_negocio(id)
);


--Vista con métricas de restauración
CREATE OR REPLACE VIEW verin_dw.vw_metrica_restauracion AS
SELECT
    -- Negocios abiertos que sean solo restaurantes
    COUNT(*) FILTER (WHERE D.abierto = TRUE AND T.nombre = 'Restaurante') AS restaurantes_abiertos,
    -- Negocios abiertos que sean solo bares
    COUNT(*) FILTER (WHERE D.abierto = TRUE AND T.nombre = 'Bar') AS bares_abiertos,
    -- Negocios abiertos que sean solo cafeterías
    COUNT(*) FILTER (WHERE D.abierto = TRUE AND T.nombre = 'Cafetería') AS cafeterias_abiertas,
    -- Negocios abiertos que sean solo otros tipos de negocio
    COUNT(*) FILTER (WHERE D.abierto = TRUE AND T.nombre = 'Otros') AS otros_abiertos,
    -- Negocios abiertos que sean restaurantes, bares o cafeterías
    (COUNT(*) FILTER (WHERE D.abierto = TRUE and T.nombre IN ('Restaurante', 'Bar', 'Cafetería')) * 1000.0 / (SELECT poblacion_total FROM verin_dw.fact_poblacion WHERE id_municipio = (SELECT id_municipio FROM verin_dw.dim_municipio WHERE nombre = 'Verín') ORDER BY id_fecha DESC LIMIT 1)) AS negocios_abiertos_por_1000_habitantes
FROM verin_dw.data_restauracion D
JOIN verin_dw.dim_tipo_negocio T ON D.tipo = T.id;
-- Tabla de hechos de población
CREATE TABLE IF NOT EXISTS verin_dw.fact_poblacion (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    poblacion_total INT,
    hombres INT,
    mujeres INT,
    PRIMARY KEY (id_fecha, id_municipio),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio)
);

-- Tabla de hechos de nacimientos
CREATE TABLE IF NOT EXISTS verin_dw.fact_nacimientos (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    nacimientos_total INT,
    hombres INT,
    mujeres INT,
    PRIMARY KEY (id_fecha, id_municipio),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio)
);
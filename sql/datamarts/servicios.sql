-- ID para la dimensión de tipo sanitario
CREATE SEQUENCE IF NOT EXISTS verin_dw.tipo_sanitario START 1;

-- Dimensión tipo sanitario
CREATE TABLE IF NOT EXISTS verin_dw.dim_tipo_sanitario (
    id_tipo_sanitario INT PRIMARY KEY DEFAULT nextval('verin_dw.tipo_sanitario'),
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Tabla de hechos de personal sanitario
CREATE TABLE IF NOT EXISTS verin_dw.fact_personal_sanitario (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    id_tipo_sanitario INT NOT NULL,
    total INT,
    PRIMARY KEY (id_fecha, id_municipio, id_tipo_sanitario),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio),
    FOREIGN KEY (id_tipo_sanitario) REFERENCES verin_dw.dim_tipo_sanitario(id_tipo_sanitario)
);

-- ID para la dimensión de tipo de educación
CREATE SEQUENCE IF NOT EXISTS verin_dw.tipo_educacion START 1;

-- Dimensión tipo de educación
CREATE TABLE IF NOT EXISTS verin_dw.dim_tipo_educacion (
    id_tipo_educacion INT PRIMARY KEY DEFAULT nextval('verin_dw.tipo_educacion'),
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Tabla de hechos de alumnos por tipo de educación
CREATE TABLE IF NOT EXISTS verin_dw.fact_alumnos_tipo_educacion (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    id_tipo_educacion INT NOT NULL,
    total INT,
    PRIMARY KEY (id_fecha, id_municipio, id_tipo_educacion),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio),
    FOREIGN KEY (id_tipo_educacion) REFERENCES verin_dw.dim_tipo_educacion(id_tipo_educacion)
);
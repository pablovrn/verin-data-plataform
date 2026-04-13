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

-- Tabla de hechos de defunciones
CREATE TABLE IF NOT EXISTS verin_dw.fact_defunciones (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    defunciones_total INT,
    hombres INT,
    mujeres INT,
    PRIMARY KEY (id_fecha, id_municipio),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio)
);

-- ID para la dimensión de grupo de edad
CREATE SEQUENCE IF NOT EXISTS verin_dw.grupo_edad_id_seq START 1;
-- Dimension grupo de edad
CREATE TABLE IF NOT EXISTS verin_dw.dim_grupo_edad (
    id_grupo_edad INT PRIMARY KEY DEFAULT nextval('verin_dw.grupo_edad_id_seq'),
    rango VARCHAR(20) NOT NULL UNIQUE
);

-- Tabla de hechos de población por edad
CREATE TABLE IF NOT EXISTS verin_dw.fact_poblacion_edad (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    id_grupo_edad INT NOT NULL,
    poblacion_total INT,
    hombres INT,
    mujeres INT,
    PRIMARY KEY (id_fecha, id_municipio, id_grupo_edad),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio),
    FOREIGN KEY (id_grupo_edad) REFERENCES verin_dw.dim_grupo_edad(id_grupo_edad)
);

-- Id para la dimensión de lugar de nacimiento
CREATE SEQUENCE IF NOT EXISTS verin_dw.lugar_nacimiento_id_seq START 1;
-- Dimensión para lugar de nacimiento
CREATE TABLE IF NOT EXISTS verin_dw.dim_lugar_nacimiento (
    id_lugar_nacimiento INT PRIMARY KEY DEFAULT nextval('verin_dw.lugar_nacimiento_id_seq'),
    nombre VARCHAR(100) NOT NULL UNIQUE
);
-- Tabla de hechos de población por lugar de nacimiento
CREATE TABLE IF NOT EXISTS verin_dw.fact_poblacion_lugar (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    id_lugar_nacimiento INT NOT NULL,
    poblacion_total INT,
    hombres INT,
    mujeres INT,
    PRIMARY KEY (id_fecha, id_municipio, id_lugar_nacimiento),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio),
    FOREIGN KEY (id_lugar_nacimiento) REFERENCES verin_dw.dim_lugar_nacimiento(id_lugar_nacimiento)
);



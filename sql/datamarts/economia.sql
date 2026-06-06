-- Dimensión para sector económico
CREATE SEQUENCE IF NOT EXISTS verin_dw.sector_economico_id_seq START 1;
CREATE TABLE IF NOT EXISTS verin_dw.dim_sector_economico (
    id_sector_economico INT PRIMARY KEY DEFAULT nextval('verin_dw.sector_economico_id_seq'),
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Dimensión para tipo de empresa
CREATE SEQUENCE IF NOT EXISTS verin_dw.tipo_empresa_id_seq START 1;
CREATE TABLE IF NOT EXISTS verin_dw.dim_tipo_empresa (
    id_tipo_empresa INT PRIMARY KEY DEFAULT nextval('verin_dw.tipo_empresa_id_seq'),
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Tabla de hechos de empresas por sector económico y tipo de empresa
CREATE TABLE IF NOT EXISTS verin_dw.fact_empresas_sector (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    id_sector_economico INT NOT NULL,
    id_tipo_empresa INT NOT NULL,
    empresas_total INT,
    PRIMARY KEY (id_fecha, id_municipio, id_sector_economico, id_tipo_empresa),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio),
    FOREIGN KEY (id_sector_economico) REFERENCES verin_dw.dim_sector_economico(id_sector_economico),
    FOREIGN KEY (id_tipo_empresa) REFERENCES verin_dw.dim_tipo_empresa(id_tipo_empresa)
);

-- Dimensión para rangos de asalariados
CREATE SEQUENCE IF NOT EXISTS verin_dw.rango_asalariados_id_seq START 1;
CREATE TABLE IF NOT EXISTS verin_dw.dim_rango_asalariados (
    id_rango_asalariados INT PRIMARY KEY DEFAULT nextval('verin_dw.rango_asalariados_id_seq'),
    rango VARCHAR(50) NOT NULL UNIQUE
);

-- Tabla de hechos de empresas por rango de asalariados
CREATE TABLE IF NOT EXISTS verin_dw.fact_empresas_asalariados (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    id_rango_asalariados INT NOT NULL,
    empresas_total INT,
    PRIMARY KEY (id_fecha, id_municipio, id_rango_asalariados),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio),
    FOREIGN KEY (id_rango_asalariados) REFERENCES verin_dw.dim_rango_asalariados(id_rango_asalariados)
);

-- Tabla de hechos para renta bruta per cápita y producto interior bruto per cápita
CREATE TABLE IF NOT EXISTS verin_dw.fact_macros_economicos (
    id_fecha DATE NOT NULL,
    id_municipio INT NOT NULL,
    renta_bruta_per_capita DECIMAL(15, 2),
    pib_per_capita DECIMAL(15, 2),
    PRIMARY KEY (id_fecha, id_municipio),
    FOREIGN KEY (id_fecha) REFERENCES verin_dw.dim_fecha(id_fecha),
    FOREIGN KEY (id_municipio) REFERENCES verin_dw.dim_municipio(id_municipio)
);

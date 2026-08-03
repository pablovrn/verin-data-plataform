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


    

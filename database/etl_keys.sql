
use etl_project;

# Asignamos los keys primarios:

ALTER TABLE sucursal ADD PRIMARY KEY(sucursalId) AUTO_INCREMENT;

ALTER TABLE tipo_sucursal ADD PRIMARY KEY(tipoSucursalId);
ALTER TABLE tipo_sucursal MODIFY COLUMN tipoSucursalId INT NOT NULL AUTO_INCREMENT;

ALTER TABLE localidad  ADD PRIMARY KEY(localidadId);
ALTER TABLE tipo_sucursal MODIFY COLUMN tipoSucursalId INT NOT NULL AUTO_INCREMENT;

ALTER TABLE provincia ADD PRIMARY KEY(provinciaId);
ALTER TABLE provincia MODIFY COLUMN provinciaId INT NOT NULL AUTO_INCREMENT;

ALTER TABLE comercio_bandera ADD PRIMARY KEY(comercioBanderaId);
ALTER TABLE comercio_bandera MODIFY COLUMN comercioBanderaId INT NOT NULL AUTO_INCREMENT;

ALTER TABLE marca ADD PRIMARY KEY(marcaId);
ALTER TABLE marca MODIFY COLUMN marcaId INT NOT NULL AUTO_INCREMENT;

ALTER TABLE producto ADD PRIMARY KEY(productoId);
ALTER TABLE producto MODIFY COLUMN productoId INT NOT NULL AUTO_INCREMENT;

ALTER TABLE precios ADD PRIMARY KEY(precioId);
ALTER TABLE precios MODIFY COLUMN precioId INT NOT NULL AUTO_INCREMENT;


# Realizamos las conexiones:

ALTER TABLE sucursal ADD CONSTRAINT sucursal_fk_comercioBanderaId FOREIGN KEY (comercioBanderaId) REFERENCES comercio_bandera (comercioBanderaId) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE sucursal ADD CONSTRAINT sucursal_fk_sucursalTipoId FOREIGN KEY (tipoSucursalId) REFERENCES tipo_sucursal (tipoSucursalId) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE sucursal ADD CONSTRAINT sucursal_fk_localidadId FOREIGN KEY (localidadId) REFERENCES localidad (localidadId) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE producto ADD CONSTRAINT producto_fk_marcaId FOREIGN KEY (marcaId) REFERENCES marca (marcaId) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE localidad ADD CONSTRAINT localidad_fk_provinciaId FOREIGN KEY (provinciaId) REFERENCES provincia (provinciaId) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE precios ADD CONSTRAINT precios_fk_productoId FOREIGN KEY (productoId) REFERENCES producto (productoId) ON DELETE RESTRICT ON UPDATE RESTRICT;

SET FOREIGN_KEY_CHECKS=0;
ALTER TABLE precios ADD CONSTRAINT precios_fk_sucursalId FOREIGN KEY (sucursalId) REFERENCES sucursal (sucursalId) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE precios_1  DROP FOREIGN KEY precios_fk_sucursalId_copy;
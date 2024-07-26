select count(*) from saber11_test;

DROP TABLE saber11_small;

CREATE TABLE saber11 (
    periodo INT,
    cole_area_ubicacion TEXT,
    cole_naturaleza TEXT,
    cole_cod_dane_establecimiento INT,
    cole_depto_ubicacion TEXT,
    cole_mcpio_ubicacion TEXT,
    cole_nombre_establecimiento TEXT,
    estu_depto_presentacion TEXT,
    estu_mcpio_presentacion TEXT,
    estu_fechanacimiento TEXT,
    estu_estadoinvestigacion TEXT,
    estu_genero TEXT,
    estu_estudiante TEXT,
    fami_estratovivienda TEXT,
    desemp_ingles TEXT,
    punt_ingles INT,
    punt_matematicas INT,
    punt_sociales_ciudadanas INT,
    punt_c_naturales INT,
    punt_lectura_critica INT,
    punt_global INT
);

INSERT INTO saber11 (
    periodo,
    cole_area_ubicacion,
    cole_naturaleza,
    cole_cod_dane_establecimiento,
    cole_depto_ubicacion,
    cole_mcpio_ubicacion,
    cole_nombre_establecimiento,
    estu_depto_presentacion,
    estu_mcpio_presentacion,
    estu_fechanacimiento,
    estu_estadoinvestigacion,
    estu_genero,
    estu_estudiante,
    fami_estratovivienda,
    desemp_ingles,
    punt_ingles,
    punt_matematicas,
    punt_sociales_ciudadanas,
    punt_c_naturales,
    punt_lectura_critica,
    punt_global
)
SELECT
    periodo,
    cole_area_ubicacion,
    cole_naturaleza,
    cole_cod_dane_establecimiento,
    cole_depto_ubicacion,
    cole_mcpio_ubicacion,
    cole_nombre_establecimiento,
    estu_depto_presentacion,
    estu_mcpio_presentacion,
    estu_fechanacimiento,
    estu_estadoinvestigacion,
    estu_genero,
    estu_estudiante,
    fami_estratovivienda,
    desemp_ingles,
    punt_ingles,
    punt_matematicas,
    punt_sociales_ciudadanas,
    punt_c_naturales,
    punt_lectura_critica,
    punt_global
FROM saber11_test
WHERE periodo >= 20142;

select * from saber11 WHERE cole_depto_ubicacion = "Santander" LIMIT 1000000;

UPDATE saber11
SET estu_fechanacimiento = STR_TO_DATE(estu_fechanacimiento, '%d/%m/%Y')
WHERE estu_fechanacimiento IS NOT NULL
  AND estu_fechanacimiento != ''
  AND estu_fechanacimiento LIKE '__/__/____';
  
SELECT * from saberpro limit 10;


CREATE TABLE promedios_departamento_pro AS
SELECT 
	periodo,
    ESTU_INST_DEPARTAMENTO AS departamento,
    AVG(CAST(MOD_COMPETEN_CIUDADA_PUNT AS UNSIGNED)) AS promedio_c_ciudadana,
    AVG(CAST(MOD_COMUNI_ESCRITA_PUNT AS UNSIGNED)) AS promedio_comuni_escrita,
    AVG(CAST(MOD_INGLES_PUNT AS UNSIGNED)) AS promedio_ingles,
    AVG(CAST(MOD_LECTURA_CRITICA_PUNT AS UNSIGNED)) AS promedio_lectura_critica,
    AVG(CAST(MOD_RAZONA_CUANTITAT_PUNT AS UNSIGNED)) AS promedio_razona_cuantitativo
FROM 
    saberpro
GROUP BY 
    periodo,
    departamento
ORDER BY 
    periodo;

SELECT distinct PERIODO FROM promedios_departamento_pro;

CREATE TABLE promedios_colombia_pro AS
SELECT 
	periodo,
    AVG(promedio_c_ciudadana) AS promedio_c_ciudadana,
    AVG(promedio_comuni_escrita) AS promedio_comuni_escrita,
    AVG(promedio_ingles) AS promedio_ingles,
    AVG(promedio_lectura_critica) AS promedio_lectura_critica,
    AVG(promedio_razona_cuantitativo) AS promedio_razona_cuantitativo
FROM 
    promedios_departamento_pro
GROUP BY 
    periodo
ORDER BY 
    periodo;
    
SELECT * FROM promedios_colombia_pro where DEPARTAMENTO = "SANTANDER" LIMIT 10;

UPDATE saber11
SET fami_estratovivienda = CASE
    WHEN fami_estratovivienda REGEXP '^Estrato [0-9]+$' THEN 
        CAST(SUBSTRING(fami_estratovivienda, 9) AS UNSIGNED)
    WHEN fami_estratovivienda = 'Sin Estrato' THEN NULL
    WHEN fami_estratovivienda IS NULL THEN NULL
    ELSE NULL
END;


/* Consulta inicial por departamento y periodo */
SELECT
    *
FROM saber11
WHERE cole_depto_ubicacion = "SANTANDER" AND periodo >= 20141 AND periodo <= 20152
ORDER BY periodo
LIMIT 1000000;

select * from saberpro limit 10;
	
CREATE TABLE saberpro_reduced AS
SELECT
    PERIODO AS periodo,
    ESTU_INST_DEPARTAMENTO AS inst_depto_ubicacion,
    INST_CARACTER_ACADEMICO AS inst_naturaleza,
    INST_COD_INSTITUCION AS inst_cod_institucion,
    ESTU_INST_DEPARTAMENTO,
    ESTU_INST_MUNICIPIO,
    INST_NOMBRE_INSTITUCION,
    ESTU_DEPTO_PRESENTACION,
    ESTU_MCPIO_PRESENTACION,
    ESTU_FECHANACIMIENTO AS estu_fechanacimiento,
    ESTU_ESTADOINVESTIGACION,
    ESTU_GENERO,
    ESTU_ESTUDIANTE,
    FAMI_ESTRATOVIVIENDA,
    MOD_INGLES_DESEM AS desemp_ingles,
    CAST(MOD_INGLES_PUNT AS UNSIGNED) AS punt_ingles,
    CAST(MOD_RAZONA_CUANTITAT_PUNT AS UNSIGNED) AS punt_razona_cuantitativo,
    CAST(MOD_COMPETEN_CIUDADA_PUNT AS UNSIGNED) AS punt_c_ciudadana,
    CAST(MOD_LECTURA_CRITICA_PUNT AS UNSIGNED) AS punt_lectura_critica,
    CAST(MOD_COMUNI_ESCRITA_PUNT AS UNSIGNED) AS punt_comuni_escrita
FROM saberpro;

UPDATE saberpro_reduced
SET fami_estratovivienda = CASE
    WHEN fami_estratovivienda REGEXP '^Estrato [0-9]+$' THEN 
        CAST(SUBSTRING(fami_estratovivienda, 9) AS UNSIGNED)
    WHEN fami_estratovivienda = 'Sin Estrato' THEN NULL
    WHEN fami_estratovivienda IS NULL THEN NULL
    ELSE NULL
END;

select distinct fami_estratovivienda from saberpro_reduced limit 10;
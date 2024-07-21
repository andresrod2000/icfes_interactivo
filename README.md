# 🎓 Backend para API de Datos Educativos

Este repositorio contiene el código backend para una API de Datos Educativos, que proporciona acceso a los resultados de las pruebas SABER 11 y SABER PRO en Colombia. La API está construida usando Python y Flask, e interactúa con una base de datos MySQL alojada en AWS.

## 🫂 Equipo
- Nicolas Mantilla @Vendetta0462
- Hernan Rodriguez @andresrod2000
- Juan Jaimes @juanes2809

## 🛠️ Tecnologías Utilizadas

- Python 🐍
- Flask 🌶️
- MySQL Connector 🗄️
- AWS (EC2 para alojamiento, RDS para base de datos MySQL) 🚀

## 📁 Componentes Principales

- `app.py`: El archivo principal de la aplicación Flask que define las rutas de la API y maneja las solicitudes.
- `consultas.py`: Contiene funciones para ejecutar consultas a la base de datos y procesar datos.

## 🔗 Endpoints de la API

La API proporciona varios endpoints para acceder a datos educativos. Aquí hay un breve resumen de las principales categorías:

- `/saber11/...`: Endpoints relacionados con los resultados de las pruebas SABER 11
- `/saberpro/...`: Endpoints relacionados con los resultados de las pruebas SABER PRO
- `/predict/...`: Endpoints para modelos predictivos (detalles por implementar)
- `/chat`: Endpoint para integración con un chatbot Rasa

## 🚀 Configuración y Despliegue

1. Asegúrate de tener Python 3.x instalado en tu sistema.
2. Instala las dependencias requeridas:
   ```
   pip install requirements.txt
   ```
3. Configura tu base de datos MySQL en AWS RDS y añade el archivo `config.py` con tus credenciales de base de datos.
4. Despliega la aplicación en una instancia EC2 de AWS.
5. Configura tu grupo de seguridad EC2 para permitir tráfico entrante en el puerto 5000.

## 🏃‍♂️ Ejecutando la Aplicación

Para ejecutar la aplicación localmente para desarrollo:

```
python app.py
```

La API estará disponible en `http://localhost:5000`.

## 🔒 Nota sobre Seguridad

Asegúrate de seguir las mejores prácticas de AWS para seguridad, especialmente cuando se trata de credenciales de base de datos y acceso a la API.

## 🤝 Contribuir

Si deseas contribuir a este proyecto, por favor haz un fork del repositorio y envía un pull request con tus cambios propuestos.

## 📄 Licencia

[Agrega aquí la licencia que hayas elegido]
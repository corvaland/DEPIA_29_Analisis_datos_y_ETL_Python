# Build imagen
docker build -t nombre-imagen .
# listar imagenes
docker images
# Eliminar imagen
docker rmi -f id-imagen
# ejecutar contenedor
docker run -d -p 8000:8888 --name jupyter jupyter_notebook
#Listar contenedores
docker ps 
# Ver log de un contenedor
docker logs -f id-contenedor
# eliminar un contenedor
docker rm -f id-contenedor


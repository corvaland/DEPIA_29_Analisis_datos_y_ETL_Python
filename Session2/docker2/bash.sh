# Build de la iamgen
docker build -t web-app .

# Ejecutar contenedor
docker run -d -p 5000:5000 --name container-app web-app
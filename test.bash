# Validar creación de acciones con datos correctos
echo " "
echo "Probando creación de acciones con datos correctos..."
response=$(curl -s -X POST http://127.0.0.1:8000/stocks/ \
  -H "Content-Type: application/json" \
  -d '[
        {"date": "2023-09-22", "open": 150.00, "high": 155.00, "low": 149.00, "close": 153.00, "adj_close": 153.00, "volume": 10000}
      ]')
echo "Respuesta: $response"
echo " "

# Validar creación de acciones con datos duplicados
echo "Probando creación de acciones con fecha duplicada..."
response=$(curl -s -X POST http://127.0.0.1:8000/stocks/ \
  -H "Content-Type: application/json" \
  -d '[
        {"date": "2023-09-22", "open": 160.00, "high": 165.00, "low": 159.00, "close": 163.00, "adj_close": 163.00, "volume": 12000}
      ]')
echo "Respuesta: $response"
echo " "

# Validar consulta de acciones
echo "Probando consulta de acciones..."
response=$(curl -s -X 'GET' 'http://127.0.0.1:8000/stocks/?page=100&limit=5')
echo "Respuesta: $response"
echo " "

# Validar consulta de acciones con paginación incorrecta
echo "Probando consulta con paginación incorrecta..."
response=$(curl -s -X 'GET' 'http://127.0.0.1:8000/stocks/?page=2086&limit=5')
echo "Respuesta: $response"

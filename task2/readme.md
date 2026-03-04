## Порядок запуска

```bash
go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest
oapi-codegen -config openapi/oapi-codegen.yaml openapi/openapi.yaml
make all 
./server/server
```




### Проверка API

После запуска `./server` можно дергать эндпоинты, например:

- `GET http://localhost:8080/products` — список товаров (с пагинацией `?page=0&size=20`)
- `POST http://localhost:8080/products` — создание товара (тело JSON: name, price, stock, category, status)
- `GET http://localhost:8080/products/{id}` — товар по ID
- `PUT http://localhost:8080/products/{id}` — обновление
- `DELETE http://localhost:8080/products/{id}` — мягкое удаление

Для POST/PUTзаголовок `Content-Type: application/json`.

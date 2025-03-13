package com.aws.lambda_inventario;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.w3c.dom.Attr;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class lambda_productos implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayProxyResponseEvent> {

    private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(Region.US_EAST_1).build();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String tableName = "tabla-productos";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayV2HTTPEvent request, Context context) {
        try {
            if (request == null) {
                context.getLogger().log("ERROR: La solicitud recibida es NULL");
                return createResponse(500, "Error: La solicitud recibida es NULL");
            }
            String httpMethod = "UNKNOWN";
            if (request.getRequestContext() != null &&
                    request.getRequestContext().getHttp() != null &&
                    request.getRequestContext().getHttp().getMethod() != null) {
                httpMethod = request.getRequestContext().getHttp().getMethod();
            }
            context.getLogger().log("Método HTTP recibido: " + httpMethod);
            switch (httpMethod) {
                case "POST":
                    return createProducto(request.getBody(), context);
                case "GET":
                    return getAllProductos(context);
                case "DELETE":
                    return deleteProductobyID(request.getBody(), context);
                case "PUT":
                    return modifyProductobyID(request.getBody(), context);
                default:
                    return createResponse(400, "Método HTTP no soportado: " + httpMethod);
            }
        } catch (Exception e) {
            context.getLogger().log("ERROR en handleRequest: " + e.getMessage());
            return createResponse(500, "Error interno: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent modifyProductobyID(String body, Context context) {
        try {
            context.getLogger().log("Cuerpo recibido en PUT: " + body);
            if (body == null || body.trim().isEmpty()) {
                return createResponse(400, "El cuerpo de la solicitud está vacío.");
            }

            // Convertimos el JSON a un Map
            Map<String, Object> rawMap = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});

            // Extraemos la clave primaria (id_producto)
            String id_producto = (String) rawMap.get("id_producto");
            if (id_producto == null || id_producto.trim().isEmpty()) {
                return createResponse(400, "Falta el campo 'id_producto'.");
            }

            // Construimos itemKey solo con la clave primaria
            Map<String, AttributeValue> itemKey = new HashMap<>();
            itemKey.put("id_producto", AttributeValue.builder().s(id_producto).build());

            // Construimos la expresión de actualización
            StringBuilder updateExpression = new StringBuilder("SET ");
            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            Map<String, String> expressionAttributeNames = new HashMap<>();

            int count = 0;
            for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                String key = entry.getKey();
                if (!key.equals("id_producto")) {  // Excluimos la clave primaria
                    count++;
                    String fieldKey = "#field" + count;
                    String valueKey = ":val" + count;

                    updateExpression.append(count > 1 ? ", " : "").append(fieldKey).append(" = ").append(valueKey);
                    expressionAttributeNames.put(fieldKey, key);

                    if (entry.getValue() instanceof String) {
                        expressionAttributeValues.put(valueKey, AttributeValue.builder().s((String) entry.getValue()).build());
                    } else if (entry.getValue() instanceof Number) {
                        expressionAttributeValues.put(valueKey, AttributeValue.builder().n(entry.getValue().toString()).build());
                    } else if (entry.getValue() instanceof Boolean) {
                        expressionAttributeValues.put(valueKey, AttributeValue.builder().bool((Boolean) entry.getValue()).build());
                    }
                }
            }

            if (count == 0) {
                return createResponse(400, "No hay campos válidos para actualizar.");
            }

            // Construimos la solicitud UpdateItemRequest
            UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(itemKey)
                    .updateExpression(updateExpression.toString())
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues)
                    .build();

            dynamoDbClient.updateItem(request);
            return createResponse(200, "Producto actualizado correctamente.");

        } catch (Exception e) {
            context.getLogger().log("ERROR en modifyProductobyID: " + e.getMessage());
            return createResponse(500, "Error al actualizar producto: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent createProducto(String body, Context context) {
        try {
            context.getLogger().log("Cuerpo recibido en POST: " + body);
            if (body == null || body.trim().isEmpty()) {
                return createResponse(400, "El cuerpo de la solicitud está vacío.");
            }
            Producto producto = objectMapper.readValue(body, Producto.class);
            if (producto.getNombre() == null || producto.getNombre().trim().isEmpty()) {
                return createResponse(400, "Falta el campo 'nombre'");
            }
            if (producto.getId_producto() == null || producto.getId_producto().trim().isEmpty()) {
                producto.setId_producto(UUID.randomUUID().toString());
            }
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id_producto", AttributeValue.builder().s(producto.getId_producto()).build());
            item.put("nombre", AttributeValue.builder().s(producto.getNombre()).build());
            if (producto.getPrecio() != 0) {
                item.put("precio", AttributeValue.builder().s(String.valueOf(producto.getPrecio())).build());
            }
            if (producto.getCantidad() != 0) {
                item.put("cantidad", AttributeValue.builder().s(String.valueOf(producto.getCantidad())).build());
            }

            dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
            context.getLogger().log("Producto creado con ID: " + producto.getId_producto());
            return createResponse(200, objectMapper.writeValueAsString(producto));
        } catch (Exception e) {
            context.getLogger().log("ERROR en createProducto: " + e.getMessage());
            return createResponse(500, "Error al crear producto: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent getAllProductos(Context context) {
        try {
            context.getLogger().log("Consultando todos los productos en la tabla: " + tableName);
            ScanResponse scanResponse = dynamoDbClient.scan(ScanRequest.builder().tableName(tableName).build());
            List<Map<String, AttributeValue>> items = scanResponse.items();
            if (items == null || items.isEmpty()) {
                return createResponse(200, "[]");
            }
            List<Map<String, Object>> convertedItems = items.stream().map(item -> {
                Map<String, Object> map = new HashMap<>();
                item.forEach((key, attributeValue) -> {
                    if (attributeValue.s() != null) {
                        map.put(key, attributeValue.s());
                    } else if (attributeValue.n() != null) {
                        map.put(key, attributeValue.n());
                    } else if (attributeValue.ss() != null && !attributeValue.ss().isEmpty()) {
                        map.put(key, attributeValue.ss());
                    }
                });
                return map;
            }).collect(Collectors.toList());
            String json = objectMapper.writeValueAsString(convertedItems);
            return createResponse(200, json);
        } catch (Exception e) {
            context.getLogger().log("ERROR en getAllProductos: " + e.getMessage());
            return createResponse(500, "Error al obtener productos: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        return new APIGatewayProxyResponseEvent().withStatusCode(statusCode).withBody(body);
    }


    private APIGatewayProxyResponseEvent deleteProductobyID(String body, Context context) {
        try {
            context.getLogger().log("Cuerpo recibido en DELETE: " + body);
            if (body == null || body.trim().isEmpty()) {
                return createResponse(400, "El cuerpo de la solicitud está vacío.");
            }

            // Mapeamos con ObjectMapper el body en búsqueda del parametro ID
            String id_producto = "";
            Map<String, Object> bodyMap = objectMapper.readValue(body, new TypeReference<>() {
            });
            if (bodyMap.containsKey("id_producto")) {
                id_producto = (String) bodyMap.get("id_producto");
                System.out.println("Se ha identificado el ID: " + body);
            } else {
                context.getLogger().log("No se ha identificado el ID: " + body);
                System.out.println("No se ha identificado el ID: " + body);
                return createResponse(500, "Error al eliminar producto con id" + id_producto);

            }

            Map<String, AttributeValue> key = new HashMap<>();
            key.put("id_producto", AttributeValue.fromS(id_producto));
            // ReturnValue.ALL_OLD es para que devuelva el objeto eliminado
            Object objeto = dynamoDbClient.deleteItem(DeleteItemRequest.builder().tableName(tableName).key(key).returnValues(ReturnValue.ALL_OLD).build());
            context.getLogger().log("Objeto eliminado: " + objeto.toString());
            return createResponse(200, objectMapper.writeValueAsString(bodyMap));
        } catch (Exception e) {
            context.getLogger().log("ERROR en deleteProductobyID: " + e.getMessage());
            return createResponse(500, "Error al eliminar producto: " + e.getMessage());
        }
    }

public static class Producto {
    private String id_producto;
    private String nombre;
    private double precio;
    private int cantidad;


    public String getId_producto() {
        return id_producto;
    }

    public void setId_producto(String id_producto) {
        this.id_producto = id_producto;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public double getPrecio() {
        return precio;
    }

    public void setPrecio(double precio) {
        this.precio = precio;
    }

    public int getCantidad() {
        return cantidad;
    }

    public void setCantidad(int cantidad) {
        this.cantidad = cantidad;
    }
}
}

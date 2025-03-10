package com.aws.lambda_inventario;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
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
                default:
                    return createResponse(400, "Método HTTP no soportado: " + httpMethod);
            }
        } catch (Exception e) {
            context.getLogger().log("ERROR en handleRequest: " + e.getMessage());
            return createResponse(500, "Error interno: " + e.getMessage());
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
                item.put("precio", AttributeValue.builder().s(String.valueOf(producto.getCantidad())).build());
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

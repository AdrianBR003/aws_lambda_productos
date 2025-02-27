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
                return createResponse(400, "Falta el campo 'nombre'.");
            }
            if (producto.getId_producto() == null || producto.getId_producto().trim().isEmpty()) {
                producto.setId_producto(UUID.randomUUID().toString());
            }
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id_producto", AttributeValue.builder().s(producto.getId_producto()).build());
            item.put("nombre", AttributeValue.builder().s(producto.getNombre()).build());
            if (producto.getDescripcion() != null) {
                item.put("descripcion", AttributeValue.builder().s(producto.getDescripcion()).build());
            }
            if (producto.getCategoria() != null) {
                item.put("categoria", AttributeValue.builder().s(producto.getCategoria()).build());
            }
            if (producto.getFabricante() != null) {
                item.put("fabricante", AttributeValue.builder().s(producto.getFabricante()).build());
            }
            if (producto.getEstado() != null) {
                item.put("estado", AttributeValue.builder().s(producto.getEstado()).build());
            }
            if (producto.getNumero_serie() != null) {
                item.put("numero_serie", AttributeValue.builder().s(producto.getNumero_serie()).build());
            }
            if (producto.getResponsable() != null) {
                item.put("responsable", AttributeValue.builder().s(producto.getResponsable()).build());
            }
            if (producto.getUbicacion_actual() != null) {
                item.put("ubicacion_actual", AttributeValue.builder().s(producto.getUbicacion_actual()).build());
            }
            if (producto.getId_coleccion() != null) {
                item.put("id_coleccion", AttributeValue.builder().s(producto.getId_coleccion()).build());
            }
            if (producto.getCosto_adquisicion() != null) {
                item.put("costo_adquisicion", AttributeValue.builder().n(producto.getCosto_adquisicion().toString()).build());
            }
            if (producto.getFecha_adquisicion() != null) {
                item.put("fecha_adquisicion", AttributeValue.builder().s(producto.getFecha_adquisicion()).build());
            }
            if (producto.getVida_util() != null) {
                item.put("vida_util", AttributeValue.builder().n(producto.getVida_util().toString()).build());
            }
            if (producto.getUltima_version() != null) {
                item.put("ultima_version", AttributeValue.builder().s(producto.getUltima_version()).build());
            }
            if (producto.getHistorial_movimientos() != null && !producto.getHistorial_movimientos().isEmpty()) {
                item.put("historial_movimientos", AttributeValue.builder().ss(producto.getHistorial_movimientos()).build());
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
        private String descripcion;
        private String categoria;
        private String fabricante;
        private String estado;
        private String numero_serie;
        private String responsable;
        private String ubicacion_actual;
        private String id_coleccion;
        private Double costo_adquisicion;
        private String fecha_adquisicion;
        private Integer vida_util;
        private String ultima_version;
        private List<String> historial_movimientos;

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
        public String getDescripcion() {
            return descripcion;
        }
        public void setDescripcion(String descripcion) {
            this.descripcion = descripcion;
        }
        public String getCategoria() {
            return categoria;
        }
        public void setCategoria(String categoria) {
            this.categoria = categoria;
        }
        public String getFabricante() {
            return fabricante;
        }
        public void setFabricante(String fabricante) {
            this.fabricante = fabricante;
        }
        public String getEstado() {
            return estado;
        }
        public void setEstado(String estado) {
            this.estado = estado;
        }
        public String getNumero_serie() {
            return numero_serie;
        }
        public void setNumero_serie(String numero_serie) {
            this.numero_serie = numero_serie;
        }
        public String getResponsable() {
            return responsable;
        }
        public void setResponsable(String responsable) {
            this.responsable = responsable;
        }
        public String getUbicacion_actual() {
            return ubicacion_actual;
        }
        public void setUbicacion_actual(String ubicacion_actual) {
            this.ubicacion_actual = ubicacion_actual;
        }
        public String getId_coleccion() {
            return id_coleccion;
        }
        public void setId_coleccion(String id_coleccion) {
            this.id_coleccion = id_coleccion;
        }
        public Double getCosto_adquisicion() {
            return costo_adquisicion;
        }
        public void setCosto_adquisicion(Double costo_adquisicion) {
            this.costo_adquisicion = costo_adquisicion;
        }
        public String getFecha_adquisicion() {
            return fecha_adquisicion;
        }
        public void setFecha_adquisicion(String fecha_adquisicion) {
            this.fecha_adquisicion = fecha_adquisicion;
        }
        public Integer getVida_util() {
            return vida_util;
        }
        public void setVida_util(Integer vida_util) {
            this.vida_util = vida_util;
        }
        public String getUltima_version() {
            return ultima_version;
        }
        public void setUltima_version(String ultima_version) {
            this.ultima_version = ultima_version;
        }
        public List<String> getHistorial_movimientos() {
            return historial_movimientos;
        }
        public void setHistorial_movimientos(List<String> historial_movimientos) {
            this.historial_movimientos = historial_movimientos;
        }
    }
}

"""
Genera los 3 insumos (capa RAW) del proyecto Retail Medallion:

  - clientes.csv   -> maestro de clientes (CSV)
  - productos.json -> catálogo de productos (JSON multilínea)
  - ventas.csv     -> transacciones de venta (CSV)

Los datos incluyen "suciedad" intencional (duplicados, nulos, espacios,
mayúsculas/minúsculas mezcladas, cantidades negativas, fechas inválidas)
para que el ETL en PySpark tenga reglas de calidad reales que aplicar.

Uso:  python generar_datasets.py   (semilla fija => resultado reproducible)
"""
import csv
import json
import random
from datetime import date, timedelta

random.seed(2026)

REGIONES = {
    "Costa": ["Lima", "Trujillo", "Chiclayo", "Piura", "Ica"],
    "Sierra": ["Arequipa", "Cusco", "Huancayo", "Cajamarca", "Puno"],
    "Selva": ["Iquitos", "Pucallpa", "Tarapoto"],
}
SEGMENTOS = ["Consumidor", "Corporativo", "Pequeña Empresa"]
NOMBRES = ["Ana", "Luis", "María", "Jorge", "Carla", "Pedro", "Lucía", "Diego",
           "Rosa", "Miguel", "Sofía", "Carlos", "Valeria", "José", "Daniela", "Raúl"]
APELLIDOS = ["Quispe", "Flores", "Rojas", "García", "Torres", "Huamán", "Mendoza",
             "Castillo", "Vargas", "Chávez", "Ramos", "Salazar", "Paredes", "Ríos"]

CATALOGO = {
    "Tecnología": {
        "Laptops": (1800, 4500), "Celulares": (600, 3200),
        "Accesorios": (30, 250), "Monitores": (450, 1500),
    },
    "Muebles": {
        "Sillas": (150, 900), "Escritorios": (350, 1600), "Estantes": (120, 600),
    },
    "Oficina": {
        "Papelería": (5, 60), "Archivadores": (25, 180), "Impresoras": (380, 1400),
    },
}
PROVEEDORES = ["Andina SAC", "TecnoPerú", "Distribuidora Inca", "OfiMax", "Mueblería Norte"]
MODOS_ENVIO = ["Estándar", "Express", "Mismo día", "Recojo en tienda"]
CANALES = ["Web", "Tienda", "App", "Televenta"]


def generar_clientes(n=600):
    filas = []
    for i in range(1, n + 1):
        region = random.choice(list(REGIONES))
        ciudad = random.choice(REGIONES[region])
        nombre = f"{random.choice(NOMBRES)} {random.choice(APELLIDOS)}"
        registro = date(2021, 1, 1) + timedelta(days=random.randint(0, 1200))
        email = f"{nombre.lower().replace(' ', '.')}{i}@correo.pe"
        segmento = random.choice(SEGMENTOS)
        # suciedad: espacios, mayúsculas, nulos
        if random.random() < 0.05:
            ciudad = f"  {ciudad.upper()} "
        if random.random() < 0.03:
            email = ""
        if random.random() < 0.02:
            segmento = ""
        filas.append([f"C{i:04d}", nombre, segmento, ciudad, region,
                      registro.isoformat(), email])
    # duplicados exactos (el ETL debe eliminarlos)
    filas += random.sample(filas, 15)
    random.shuffle(filas)
    with open("clientes.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cliente_id", "nombre", "segmento", "ciudad", "region",
                    "fecha_registro", "email"])
        w.writerows(filas)
    return [f"C{i:04d}" for i in range(1, n + 1)]


def generar_productos():
    productos, i = [], 1
    for categoria, subcats in CATALOGO.items():
        for sub, (pmin, pmax) in subcats.items():
            for k in range(1, 9):
                precio = round(random.uniform(pmin, pmax), 2)
                costo = round(precio * random.uniform(0.55, 0.85), 2)
                productos.append({
                    "productId": f"P{i:03d}",
                    "productName": f"{sub} modelo {chr(64 + k)}{random.randint(100, 999)}",
                    "category": categoria,
                    "subCategory": sub,
                    "listPrice": precio,
                    "unitCost": costo,
                    "supplier": random.choice(PROVEEDORES),
                    "active": random.random() > 0.05,
                })
                i += 1
    with open("productos.json", "w", encoding="utf-8") as f:
        json.dump(productos, f, ensure_ascii=False, indent=2)
    return productos


def generar_ventas(clientes, productos, n=25000):
    filas = []
    inicio = date(2023, 1, 1)
    for i in range(1, n + 1):
        fecha = inicio + timedelta(days=random.randint(0, 1050))
        envio = fecha + timedelta(days=random.choice([0, 1, 2, 3, 4, 5, 7, 10]))
        prod = random.choice(productos)
        cantidad = random.choices([1, 2, 3, 4, 5, 8, 10], [40, 25, 12, 8, 7, 5, 3])[0]
        descuento = random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2, 0.3])
        fila = [f"ORD-{i:06d}", fecha.isoformat(), envio.isoformat(),
                random.choice(clientes), prod["productId"], cantidad, descuento,
                random.choice(MODOS_ENVIO), random.choice(CANALES)]
        r = random.random()
        if r < 0.01:
            fila[5] = -cantidad          # cantidad negativa
        elif r < 0.02:
            fila[3] = ""                 # cliente nulo
        elif r < 0.025:
            fila[1] = "2024-13-45"       # fecha inválida
        elif r < 0.03:
            fila[8] = fila[8].lower()    # canal sin normalizar
        filas.append(fila)
    filas += random.sample(filas, 120)  # duplicados de orden
    random.shuffle(filas)
    with open("ventas.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["orden_id", "fecha_orden", "fecha_envio", "cliente_id",
                    "producto_id", "cantidad", "descuento", "modo_envio", "canal"])
        w.writerows(filas)


if __name__ == "__main__":
    ids = generar_clientes()
    prods = generar_productos()
    generar_ventas(ids, prods)
    print("Datasets generados: clientes.csv, productos.json, ventas.csv")

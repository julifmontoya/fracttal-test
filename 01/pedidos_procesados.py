import asyncio
import aiohttp
import aiosqlite
import logging
from pydantic import BaseModel, Field, ValidationError
from typing import List, Dict, Any, Optional
import json
import hashlib
from datetime import datetime

# --- 1. Configuración y Modelos de Datos ---

# Configuración de Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# URL base de la API externa
FAKE_STORE_API_URL = "https://fakestoreapi.com"
DB_NAME = "pedidos_procesados.db"
MAX_RETRIES = 3  # Número máximo de reintentos por pedido

# Modelos Pydantic para la estructura de datos


class Item(BaseModel):
    sku: str
    cantidad: int = Field(..., gt=0)
    precio_unitario: float = Field(..., gt=0)
    # Campos enriquecidos
    api_id: Optional[int] = None
    title: Optional[str] = None
    category: Optional[str] = None
    subtotal: Optional[float] = None


class PedidoEntrada(BaseModel):
    id: int
    cliente: str
    productos: List[Item]
    fecha: str


class PedidoProcesado(PedidoEntrada):
    total_bruto: float
    descuento_porcentaje: float
    descuento_aplicado: float
    total_neto: float
    hash_id: str
    estado: str = "PROCESADO"
    fecha_procesamiento: str = datetime.now().isoformat()

# --- 2. Base de Datos Asíncrona (SQLite) ---


async def init_db():
    """Inicializa la base de datos y crea la tabla si no existe."""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pedidos (
                id INTEGER PRIMARY KEY,
                cliente TEXT,
                total_neto REAL,
                hash_id TEXT UNIQUE,
                fecha_procesamiento TEXT,
                data_json TEXT
            )
        """)
        await db.commit()
    logger.info(f"Base de datos '{DB_NAME}' inicializada.")


async def persistir_pedido(pedido: PedidoProcesado) -> bool:
    """
    Persiste el pedido procesado. Retorna True si es exitoso, False si falla (ej. duplicado).
    """
    data_json = pedido.model_dump_json()
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("""
                INSERT INTO pedidos (id, cliente, total_neto, hash_id, fecha_procesamiento, data_json)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (pedido.id, pedido.cliente, pedido.total_neto, pedido.hash_id, pedido.fecha_procesamiento, data_json))
            await db.commit()
        logger.info(f"💾 Pedido {pedido.id} persistido exitosamente.")
        return True
    except aiosqlite.IntegrityError:
        logger.warning(
            f"🚫 Pedido {pedido.id} ya existe (IntegrityError). Omitiendo persistencia.")
        return False
    except Exception as e:
        logger.error(f"❌ Error al persistir pedido {pedido.id}: {e}")
        return False

# --- 3. Procesador de Pedidos: Etapas del Flujo ---


class OrderProcessor:
    """Clase que encapsula la lógica de procesamiento de un pedido."""

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    # 3.a. Validación de los datos del pedido
    def validar_pedido(self, data: Dict[str, Any]) -> Optional[PedidoEntrada]:
        """Valida la estructura del pedido de entrada."""
        try:
            pedido = PedidoEntrada(**data)
            logger.info(f"✅ Pedido {pedido.id}: Validación de datos correcta.")
            return pedido
        except ValidationError as e:
            logger.error(
                f"❌ Pedido {data.get('id', 'N/A')}: Error de validación: {e}")
            return None
        except Exception as e:
            logger.error(
                f"❌ Pedido {data.get('id', 'N/A')}: Error inesperado en validación: {e}")
            return None

    # 3.b. Enriquecimiento de los productos
    async def enriquecer_producto(self, item: Item) -> Optional[Item]:
        """Consulta la API externa para enriquecer la información del producto."""
        # NOTA: La API de fakestore no usa SKU, asumiremos que el SKU 'P001' se mapea al ID '1', 'P002' al '2', etc.
        try:
            # Intentar mapear SKU a ID de la API
            api_id = int(item.sku.replace('P', ''))

            url = f"{FAKE_STORE_API_URL}/products/{api_id}"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    item.api_id = data.get('id')
                    item.title = data.get('title')
                    item.category = data.get('category')
                    logger.debug(
                        f"ℹ️ Producto {item.sku} enriquecido: {item.title}")
                    return item
                else:
                    logger.warning(
                        f"⚠️ API Fallida para SKU {item.sku}: HTTP {response.status}")
                    # En caso de falla, devolvemos el ítem sin enriquecer (para no detener todo)
                    return item
        except Exception as e:
            logger.error(f"❌ Error al consultar API para SKU {item.sku}: {e}")
            return None  # Falla crítica en el enriquecimiento

    async def enriquecer_pedido(self, pedido: PedidoEntrada) -> Optional[PedidoEntrada]:
        """Ejecuta el enriquecimiento para todos los productos en paralelo."""

        # Crear tareas concurrentes para cada producto
        enriquecimiento_tasks = [self.enriquecer_producto(
            item) for item in pedido.productos]

        # Esperar a que todas las tareas terminen
        resultados = await asyncio.gather(*enriquecimiento_tasks)

        # Reemplazar productos con los enriquecidos (o manejar fallos)
        productos_enriquecidos = [res for res in resultados if res is not None]

        if len(productos_enriquecidos) != len(pedido.productos):
            logger.error(
                f"❌ Pedido {pedido.id}: Fallo en el enriquecimiento de uno o más productos.")
            return None  # Fallo en la etapa

        pedido.productos = productos_enriquecidos
        logger.info(
            f"✅ Pedido {pedido.id}: Enriquecimiento de productos completado.")
        return pedido

    # 3.c. Cálculo de totales, descuentos y generación de Hash/ID único

    def calcular_y_finalizar(self, pedido: PedidoEntrada) -> PedidoProcesado:
        """Calcula totales, aplica descuentos y genera el Hash/ID único."""

        total_bruto = 0.0

        # 1. Calcular subtotal y total bruto
        for item in pedido.productos:
            subtotal = item.cantidad * item.precio_unitario
            item.subtotal = round(subtotal, 2)
            total_bruto += subtotal

        total_bruto = round(total_bruto, 2)

        # 2. Aplicar lógica de descuentos
        descuento_porcentaje = 0.0
        if total_bruto > 500:
            descuento_porcentaje = 10.0  # 10% de descuento

        descuento_aplicado = round(
            total_bruto * (descuento_porcentaje / 100.0), 2)
        total_neto = round(total_bruto - descuento_aplicado, 2)

        # 3. Generar Hash/ID único del pedido procesado
        # Usar una cadena que incluya datos relevantes para generar un hash único del *contenido* procesado
        hash_data = json.dumps(
            {
                "id": pedido.id,
                "cliente": pedido.cliente,
                "total_neto": total_neto,
                "productos_skus": sorted([f"{item.sku}-{item.cantidad}" for item in pedido.productos]),
            }
        ).encode('utf-8')
        hash_id = hashlib.sha256(hash_data).hexdigest()

        # 4. Crear el modelo de pedido procesado
        pedido_proc = PedidoProcesado(
            **pedido.model_dump(),
            total_bruto=total_bruto,
            descuento_porcentaje=descuento_porcentaje,
            descuento_aplicado=descuento_aplicado,
            total_neto=total_neto,
            hash_id=hash_id
        )

        logger.info(f"✅ Pedido {pedido.id}: Cálculos y Hash/ID generado ({hash_id[:8]}...)."
                    f" Total Neto: ${total_neto:.2f} (Descuento: {descuento_porcentaje:.1f}%)")
        return pedido_proc

    # 3.d. Persistencia de los resultados
    async def persistir(self, pedido_proc: PedidoProcesado) -> bool:
        """Guarda el resultado en la base de datos."""
        return await persistir_pedido(pedido_proc)

# --- 4. Consumidor de Cola (Worker Asíncrono) ---


async def worker(worker_id: int, queue: asyncio.Queue, processor: OrderProcessor):
    """Función que actúa como un worker para procesar pedidos de la cola."""
    logger.info(f"⚙️ Worker {worker_id} iniciado.")

    while True:
        # Obtener ítem de la cola (espera si está vacía)
        order_data, retries = await queue.get()
        pedido_id = order_data.get('id', 'N/A')

        logger.info(
            f"➡️ Worker {worker_id} procesando pedido {pedido_id} (Intento: {retries + 1}/{MAX_RETRIES}).")

        try:
            # 1. Validación de los datos del pedido
            pedido_valido = processor.validar_pedido(order_data)
            if not pedido_valido:
                raise ValueError(
                    f"Fallo en la validación del pedido {pedido_id}.")

            # 2. Enriquecimiento de los productos
            pedido_enriquecido = await processor.enriquecer_pedido(pedido_valido)
            if not pedido_enriquecido:
                raise RuntimeError(
                    f"Fallo en el enriquecimiento de la API para pedido {pedido_id}.")

            # 3. Cálculo de totales y generación de Hash/ID único
            pedido_procesado = processor.calcular_y_finalizar(
                pedido_enriquecido)

            # 4. Persistencia de los resultados
            persistido = await processor.persistir(pedido_procesado)

            if not persistido and pedido_procesado.hash_id:
                # Si no se persistió por duplicado (IntegrityError), lo consideramos ÉXITO.
                logger.info(f"⭐ Pedido {pedido_id} finalizado (ya existía).")
            elif persistido:
                logger.info(f"⭐ Pedido {pedido_id} finalizado exitosamente.")
            else:
                raise RuntimeError(
                    f"Fallo en la persistencia del pedido {pedido_id} (Error de DB desconocido).")

        except (ValueError, RuntimeError, aiohttp.ClientError) as e:
            logger.error(
                f"❌ Worker {worker_id} - Error en procesamiento del pedido {pedido_id}: {e}")

            # Gestión de reintentos
            if retries < MAX_RETRIES - 1:
                new_retries = retries + 1
                # Volver a encolar con el contador de reintentos actualizado
                await queue.put((order_data, new_retries))
                logger.warning(
                    f"🔄 Pedido {pedido_id} reencolado para reintento {new_retries}.")
                # Opcional: añadir un pequeño delay para el reintento
                await asyncio.sleep(1)
            else:
                logger.critical(
                    f"🛑 Pedido {pedido_id} descartado. Máximo de {MAX_RETRIES} reintentos alcanzado.")

        finally:
            # Notificar que la tarea actual ha terminado
            queue.task_done()

# --- 5. Ejecución Principal ---


async def main():

    # Inicializar la DB
    await init_db()

    # Definición de la cola de trabajo y el número de workers concurrentes
    queue = asyncio.Queue()
    NUM_WORKERS = 3  # Número de workers (concurrencia)

    # Ejemplo de pedidos de entrada (uno grande, uno pequeño, uno que fallará, uno duplicado)
    pedidos_entrada = [
        {
            "id": 123,
            "cliente": "ACME Corp",
            "productos": [
                {"sku": "P001", "cantidad": 3, "precio_unitario": 10},
                {"sku": "P002", "cantidad": 5, "precio_unitario": 20},
            ],
            "fecha": "2025-01-01T10:30:00Z",
        },
        {
            "id": 124,
            "cliente": "Wayne Ent.",
            "productos": [
                {"sku": "P010", "cantidad": 25, "precio_unitario": 25},
                {"sku": "P011", "cantidad": 1, "precio_unitario": 12},
            ],
            "fecha": "2025-01-02T08:00:00Z",
        },
        # Duplicate (tests idempotency; should update attempts but not double-insert items)
        {
            "id": 123,
            "cliente": "ACME Corp (retry)",
            "productos": [
                {"sku": "P001", "cantidad": 3, "precio_unitario": 10},
                {"sku": "P002", "cantidad": 5, "precio_unitario": 20},
            ],
            "fecha": "2025-01-01T10:30:00Z",
        },

    ]

    logger.info("🎬 Iniciando la simulación de encolamiento de pedidos...")

    # Encolar los pedidos (con contador de reintentos en 0)
    for pedido in pedidos_entrada:
        await queue.put((pedido, 0))  # (data, retries)

    # Crear una sesión de aiohttp para compartir entre todos los workers (mejor rendimiento)
    async with aiohttp.ClientSession() as session:
        processor = OrderProcessor(session)

        # Iniciar los workers
        workers = [asyncio.create_task(
            worker(i + 1, queue, processor)) for i in range(NUM_WORKERS)]

        # Esperar a que todos los ítems de la cola sean procesados
        await queue.join()

        logger.info(
            "✅ Todos los pedidos de la cola inicial han sido procesados.")

        # Detener los workers (cancelar sus tareas)
        for w in workers:
            w.cancel()

        # Esperar a que las cancelaciones se completen
        await asyncio.gather(*workers, return_exceptions=True)

    logger.info("👋 Proceso de pedidos asíncrono finalizado.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Proceso detenido por el usuario.")

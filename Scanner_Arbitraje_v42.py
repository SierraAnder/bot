import threading
import sqlite3
import time
import json
import asyncio
from decimal import Decimal, getcontext, ROUND_DOWN
from datetime import datetime
from binance.client import Client
from binance import AsyncClient, BinanceSocketManager
import nest_asyncio
from queue import Queue
import time
from binance.client import Client
from binance.enums import SIDE_SELL, ORDER_TYPE_MARKET
import sys
from concurrent.futures import ThreadPoolExecutor


# Configurar la precisión decimal adecuada para manejar operaciones financieras
getcontext().prec = 28

# Permitir la ejecución de bucles anidados
nest_asyncio.apply()

# Binance API credentials
API_KEY = ''
API_SECRET = ''

# Inicializar cliente
client = Client(API_KEY, API_SECRET)

# Sincronizar tiempo con el servidor de Binance
server_time = client.get_server_time()['serverTime']
local_time = int(time.time() * 1000)
client.TIME_OFFSET = server_time - local_time

# Variables de configuración
cantidad_usdt = Decimal('10.0')
comision = Decimal('0.001')
ratio = Decimal('1.005')
cantidad_alt_neta_global = Decimal('0')  # Variable global para almacenar cantidad_alt_neta

# Mensaje de inicio de ejecución
print("🚀 Inicio de la ejecución del script de arbitraje...")
# Diccionario global para almacenar info base/quote de cada par de margin
symbol_info_cache = {}
exchange_info = client.get_exchange_info()
exchange_map = {s['symbol']: s for s in exchange_info['symbols']}
# Obtener el saldo inicial de USDT al inicio del script
# Inicializar información de pares Margin y cache
try:
    cross_count = 0
    isolated_count = 0

    margin_symbols = client._request_margin_api('get', 'margin/allPairs')

    for symbol_data in margin_symbols:
        symbol = symbol_data['symbol']
        is_isolated = symbol_data.get('isIsolated', False)
        is_margin = symbol_data.get('isMarginTrade', False)

        info = exchange_map.get(symbol)
        if not info:
            continue

        base_asset = info['baseAsset']
        quote_asset = info['quoteAsset']

        precision = None
        step_size = None
        min_qty = None
        min_notional = None

        for filtro in info['filters']:
            if filtro['filterType'] == 'PRICE_FILTER':
                precision = -Decimal(filtro['tickSize']).as_tuple().exponent
            elif filtro['filterType'] == 'LOT_SIZE':
                step_size = Decimal(filtro['stepSize'])
                min_qty = Decimal(filtro['minQty'])
            elif filtro['filterType'] == 'MIN_NOTIONAL':
                min_notional = Decimal(filtro['minNotional'])

        symbol_info_cache[symbol] = {
            'base': base_asset,
            'quote': quote_asset,
            'precision': precision,
            'step_size': str(step_size),
            'min_qty': str(min_qty),
            'min_notional': str(min_notional),
            'is_isolated': is_isolated,
            'is_margin': is_margin
        }

        if is_margin:
            cross_count += 1
        if is_isolated:
            isolated_count += 1

    print(f"\n📊 Total de pares disponibles en Margin:")
    print(f"   🔁 Cross Margin:   {cross_count}")
    print(f"   🔒 Isolated Margin: {isolated_count}")

    # Obtener balances actuales
    margin_info = client.get_margin_account()
    for asset in margin_info['userAssets']:
        if asset['asset'] == 'USDT':
            print(f"\n💼 USDT en Margin Wallet:")
            print(f"   Disponible (free):     {asset['free']}")
            print(f"   En préstamo (borrowed): {asset['borrowed']}")
            print(f"   Interés acumulado:     {asset['interest']}")
            print(f"   Total neto:            {asset['netAsset']}")

    # Mostrar primer símbolo en cache
    if symbol_info_cache:
        primer_symbol = next(iter(symbol_info_cache))
        print(f"\n🔍 Primer símbolo en symbol_info_cache: {primer_symbol}")
        print("📦 Información completa del símbolo:")
        print(json.dumps(symbol_info_cache[primer_symbol], indent=4))
    else:
        print("⚠️ symbol_info_cache está vacío.")

except Exception as e:
    print(f"❌ Error al obtener información de margin: {e}")

# Diccionario para almacenar los precios en tiempo real
precios = {}
precision_tickers = {}

# Contadores y variables globales
contador = 0
contador_oportunidades = 0
total_ganancias = Decimal('0.0')
closing = False
arbitrage_queue = Queue()
arbitrage_lock = threading.Lock()

# Configuración de la base de datos SQLite
def crear_conexion(retries=5, delay=1):
    for i in range(retries):
        try:
            conn = sqlite3.connect('arbitraje.db', timeout=10)  # Aumentar el tiempo de espera a 10 segundos
            cursor = conn.cursor()
            return conn, cursor
        except sqlite3.OperationalError as e:
            if 'database is locked' in str(e):
                print(f"Database is locked, retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise
    raise sqlite3.OperationalError("Failed to connect to the database after multiple retries.")
# Crear tablas en la base de datos
def setup_database():
    conn, cursor = crear_conexion()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS busquedas (
            fecha_hora TEXT,
            par1 TEXT,
            par2 TEXT,
            par3 TEXT,
            precio_compra_par1 REAL,
            precio_compra_par2 REAL,
            precio_venta_par3 REAL,
            cantidad_inicial_usdt REAL,
            cantidad_final_usdt REAL,
            ratio_arbitraje REAL,
            ganancia REAL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS precios (
            symbol TEXT PRIMARY KEY,
            price REAL,
            timestamp TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS criptomonedas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT UNIQUE
        )
    ''')
    # Crear la tabla 'operaciones' para registrar operaciones individuales
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS operaciones (
            fecha_hora TEXT,
            tipo TEXT,
            par TEXT,
            cantidad REAL,
            precio REAL,
            cantidad_usdt REAL,
            balance_inicial REAL,
            balance_final REAL
        )
    ''')
    conn.commit()
    conn.close()

def actualizar_criptomonedas_margin():
    print("🔄 Actualizando tabla 'criptomonedas' con pares de Margin...")

    try:
        # Obtener pares de margin cruzado
        margin_pairs = client._request_margin_api('get', 'margin/allPairs')
        margin_symbols = set(item['symbol'] for item in margin_pairs)

        # Obtener pares de margin aislado (extra)
        isolated_data = client.get_all_isolated_margin_symbols()
        isolated_symbols = set(item['symbol'] for item in isolated_data)

        # Unir ambos conjuntos
        all_margin_symbols = margin_symbols.union(isolated_symbols)

        # Conexión a la base de datos
        conn, cursor = crear_conexion()

        nuevos = 0
        for symbol in all_margin_symbols:
            try:
                cursor.execute("INSERT OR IGNORE INTO criptomonedas (symbol) VALUES (?)", (symbol,))
                if cursor.rowcount > 0:
                    nuevos += 1
            except Exception as e:
                print(f"⚠️ Error insertando {symbol}: {e}")

        conn.commit()
        conn.close()

        # Mostrar resultado
        if nuevos > 0:
            print(f"✅ Se insertaron {nuevos} nuevos pares de Margin.")
        else:
            print("ℹ️ No se insertaron nuevos pares. Todos ya existían.")

        print(f"📊 Total posibles pares de Margin procesados: {len(all_margin_symbols)}")

    except Exception as e:
        print(f"❌ Error al actualizar criptomonedas de margin: {e}")
        

setup_database()



# Función para insertar precios en la base de datos
def insert_price(symbol, price, timestamp):
    conn, cursor = crear_conexion()
    cursor.execute('''
        INSERT OR REPLACE INTO precios (symbol, price, timestamp) VALUES (?, ?, ?)
    ''', (symbol, price, timestamp))
    conn.commit()
    conn.close()

# Función para insertar múltiples precios en la base de datos
def insert_prices(prices):
    conn, cursor = crear_conexion()
    cursor.executemany('''
        INSERT OR REPLACE INTO precios (symbol, price, timestamp) VALUES (?, ?, ?)
    ''', prices)
    conn.commit()
    conn.close()

# Función para manejar los mensajes recibidos del WebSocket
async def handle_message(msg):
    global precios
    if msg:
        try:
            data = msg.get('data')
            if not data:
                print(f"Mensaje recibido sin datos válidos: {msg}")
                return  # Ignorar el mensaje si no tiene la clave 'data'
            
            if 's' in data and 'c' in data:  # Usar 'c' para 'price' en lugar de 'b' y 'a'
                symbol = data['s']
                price = Decimal(data['c'])
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                precios[symbol] = {
                    'price': price,
                    'timestamp': timestamp
                }
                
            else:
                print(f"Mensaje recibido no tiene el formato esperado: {data}")
        except KeyError as e:
            print(f"Error procesando mensaje: falta clave {e}. Mensaje: {msg}")

# Obtener las principales criptomonedas desde la tabla criptomonedas en la base de datos
def get_top_cryptos():
    conn, cursor = crear_conexion()
    cursor.execute('SELECT symbol FROM criptomonedas')
    result = cursor.fetchall()
    conn.close()
    return [row[0] for row in result]

# Iniciar el WebSocket
async def start_websocket(symbols):
    global closing
    client = await AsyncClient.create(API_KEY, API_SECRET)
    bm = BinanceSocketManager(client)

    streams = [f"{symbol.lower()}@ticker" for symbol in symbols]
    ms = bm.multiplex_socket(streams)

    try:
        async with ms as stream:
            while not closing:
                res = await stream.recv()
                await handle_message(res)
    except asyncio.CancelledError:
        print("WebSocket connection was cancelled.")
    except Exception as e:
        print(f"Error en WebSocket: {e}")
    finally:
        await client.close_connection()
        await client.close()
        print("WebSocket and client session closed.")

# Ejecutar el WebSocket en un bucle de asyncio
def run(symbols):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_websocket(symbols))
    loop.run_until_complete(asyncio.sleep(0))  # Asegura que todas las tareas se ejecuten
    loop.close()

# Iniciar los WebSockets en hilos separados si es necesario
def start_websockets():
    symbols = get_top_cryptos()

    # Eliminar duplicados de la lista de símbolos
    symbols = list(set(symbols))

    # Dividir los símbolos en grupos si exceden el límite sugerido
    symbol_groups = [symbols[i:i + 200] for i in range(0, len(symbols), 200)]

    threads = []
    for group in symbol_groups:
        thread = threading.Thread(target=run, args=(group,))
        thread.start()
        threads.append(thread)

    return threads

# Iniciar los WebSockets
ws_threads = start_websockets()

# Función para obtener la precisión, stepSize, baseAsset, quoteAsset y minQty del símbolo
def obtener_info_simbolo(client, symbol):
    info = client.get_symbol_info(symbol)
    if not info:
        raise ValueError(f"No se pudo obtener la información para el par {symbol}.")

    precision = None
    step_size = None
    min_qty = None
    base_asset = info['baseAsset']
    quote_asset = info['quoteAsset']

    for filtro in info['filters']:
        if filtro['filterType'] == 'PRICE_FILTER':
            precision = Decimal(filtro['tickSize']).as_tuple().exponent
        elif filtro['filterType'] == 'LOT_SIZE':
            step_size = Decimal(filtro['stepSize'])
            min_qty = Decimal(filtro['minQty'])

    if precision is None or step_size is None or min_qty is None:
        raise ValueError(f"Información de precisión, LOT_SIZE o minQty no encontrada para {symbol}.")
    
    return -precision, step_size, min_qty, base_asset, quote_asset

def obtener_info_simbolo2(client, symbol):
    """
    Obtiene la información del par de trading desde Binance.
    Extrae precision, step_size, min_qty, max_qty, base_asset y quote_asset.
    """
    info = client.get_symbol_info(symbol)
    if not info:
        raise ValueError(f"No se pudo obtener la información para el par {symbol}.")

    precision = None
    step_size = None
    min_qty = None
    max_qty = None  # Se agrega maxQty
    base_asset = info['baseAsset']
    quote_asset = info['quoteAsset']

    for filtro in info['filters']:
        if filtro['filterType'] == 'PRICE_FILTER':
            precision = Decimal(filtro['tickSize']).as_tuple().exponent
        elif filtro['filterType'] == 'LOT_SIZE':  # Filtro de cantidad estándar
            step_size = Decimal(filtro['stepSize'])
            min_qty = Decimal(filtro['minQty'])
            max_qty = Decimal(filtro['maxQty'])  # Extraer maxQty
        elif filtro['filterType'] == 'MARKET_LOT_SIZE':  # Filtro de cantidad en órdenes de mercado
            max_qty = Decimal(filtro['maxQty'])  # Si existe, sobrescribe maxQty

    if precision is None or step_size is None or min_qty is None or max_qty is None:
        raise ValueError(f"Información incompleta para {symbol}: precision, step_size, min_qty o max_qty no encontrada.")
    
    return -precision, step_size, min_qty, max_qty, base_asset, quote_asset


# Función para ajustar la cantidad según la precisión y el stepSize del símbolo
def ajustar_cantidad(cantidad, precision, step_size):
    cantidad = Decimal(cantidad).quantize(Decimal('1e-{0}'.format(precision)), rounding=ROUND_DOWN)
    
    # Validar si la cantidad es múltiplo de stepSize
    if (cantidad % step_size) != 0:
        cantidad = int(cantidad / step_size) * step_size

    return cantidad

# Función para obtener el balance disponible de un activo
def obtener_balance_disponible(asset):
    balance = client.get_asset_balance(asset=asset)
    if balance:
        return Decimal(balance['free'])
    return Decimal('0')

# Función para encontrar oportunidades de arbitraje usando el 'price'
def encontrar_oportunidad_arbitraje(client):
    tiempo_inicio_busqueda = time.time()  # Inicio de la función
    global precios, contador_oportunidades, total_ganancias
    conn, cursor = crear_conexion()
    oportunidades = []
    registros_guardados = 0

    # Marcar el inicio de la búsqueda
    tiempo_inicio_busqueda = time.time()
    
    # Guardar precios en la base de datos

    prices_to_save = [(symbol, float(data['price']), data['timestamp']) for symbol, data in precios.items()]
    insert_prices(prices_to_save)
   

    tiempo_inicio_calculo = time.time()
    symbols = list(precios.keys())
    for par1 in symbols:
        if par1.endswith('USDT'):
            base = par1[:-4]
            for par2 in symbols:
                if par2.endswith(base) and par2 != par1:
                    alt = par2[:-len(base)]
                    par3 = alt + 'USDT'
                    if par3 in precios:
                        # Obtener los precios
                        precio1 = precios[par1]['price']
                        precio2 = precios[par2]['price']
                        precio3 = precios[par3]['price']

                        # Verificar los precios obtenidos
                        if precio1 <= 0 or precio2 <= 0 or precio3 <= 0:
                            continue

                        # 1) Cálculo de cantidades
                        # -------------------------
                        # Paso 1: USDT -> Base (par1)
                        cantidad_base = cantidad_usdt / precio1  # Cantidad teórica de 'base' que compramos
                        cantidad_base_neta = cantidad_base * (Decimal('1.0') - comision)  # Descontamos comisión

                        # Obtenemos info de par1 para ajustar a precisión y stepSize
                        precision1 = symbol_info_cache[par1]['precision']
                        step_size1 = Decimal(symbol_info_cache[par1]['step_size'])
                        min_qty1 = Decimal(symbol_info_cache[par1]['min_qty'])

                        cantidad_base_neta = ajustar_cantidad(cantidad_base_neta, precision1, step_size1)

                        # Paso 2: Base -> Alt (par2)
                        cantidad_alt = cantidad_base_neta / precio2
                        cantidad_alt_neta = cantidad_alt * (Decimal('1.0') - comision)

                        precision2 = symbol_info_cache[par2]['precision']
                        step_size2 = Decimal(symbol_info_cache[par2]['step_size'])
                        min_qty2 = Decimal(symbol_info_cache[par2]['min_qty'])
                        cantidad_alt_neta = ajustar_cantidad(cantidad_alt_neta, precision2, step_size2)

                        # Paso 3: Alt -> USDT (par3)
                        # Notar que aquí, al vender alt, no siempre hace falta “ajustar” la salida en USDT
                        # porque la venta se ajusta sola según tu cantidad de alt.
                        # Aun así, si deseas, puedes ajustar la cantidad de alt antes de vender.
                        cantidad_final_usdt = cantidad_alt_neta * precio3 * (Decimal('1.0') - comision)
                        precision3, step_size3, min_qty3, _, _ = obtener_info_simbolo(client, par3)

                        # Validar si las cantidades calculadas cumplen con los mínimos permitidos
                        if cantidad_base_neta < min_qty1:
                            print(f"Oportunidad descartada: {par1} - Cantidad base {cantidad_base_neta} menor que min_qty {min_qty1}")
                            continue
                        if cantidad_alt_neta < min_qty2:
                            print(f"Oportunidad descartada: {par2} - Cantidad alt {cantidad_alt_neta} menor que min_qty {min_qty2}")
                            continue
                        if cantidad_final_usdt < min_qty3:
                            print(f"Oportunidad descartada: {par3} - Cantidad final USDT {cantidad_final_usdt} menor que min_qty {min_qty3}")
                            continue

                        # Calcular el ratio de arbitraje
                        ratio_arbitraje = cantidad_final_usdt / cantidad_usdt
                        ganancia = ((cantidad_final_usdt - cantidad_usdt) / cantidad_usdt) * 100

                        # Crear la oportunidad de arbitraje
                        oportunidad = {
                            'Fecha y Hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                            'Par1': par1,
                            'Par2': par2,
                            'Par3': par3,
                            'Precio Compra Par1': precio1,
                            'Precio Compra Par2': precio2,
                            'Precio Venta Par3': precio3,
                            'Cantidad Inicial USDT': cantidad_usdt,
                            'Cantidad Final USDT': cantidad_final_usdt,
                            'Ratio Arbitraje': ratio_arbitraje,
                            'Ganancia': ganancia
                        }
                        tiempo_inico_guardado = time.time()
                        # Guardar la búsqueda en la base de datos
                        guardar_busqueda_en_db(cursor, oportunidad)
                        registros_guardados += 1
                        
                        if ratio_arbitraje >= ratio:
                            print(f"Oportunidad de arbitraje encontrada: {oportunidad}")
                            print(f"cantidad base neta: {cantidad_base_neta}")
                            print(f"cantidad alt neta: {cantidad_alt_neta}")
                            print(f"cantidad final usdt: {cantidad_final_usdt}")
                            contador_oportunidades += 1
                            total_ganancias += ganancia

                            conn.commit()
                            conn.close()
                            
                            # ✅ Ejecutar arbitraje aquí mismo
                            ejecutar_arbitraje(client,symbol_info_cache, [oportunidad],cantidad_alt_neta,cantidad_base_neta)
                            sys.exit
                            return [], registros_guardados  # salimos tras ejecutar el arbitraje
                        
    tiempo_fin_calculo = time.time()
    print(f"⏱️ Tiempo para calcular oportunidades: {tiempo_fin_calculo - tiempo_inicio_calculo:.4f} segundos")
                     
    conn.commit()
    conn.close()
    return [], registros_guardados  # no ejecutó nada


# Función para guardar una búsqueda de arbitraje en la base de datos
def guardar_busqueda_en_db(cursor, oportunidad):
    cursor.execute('''
        INSERT INTO busquedas (
            fecha_hora, par1, par2, par3, 
            precio_compra_par1, precio_compra_par2, precio_venta_par3,
            cantidad_inicial_usdt, cantidad_final_usdt, 
            ratio_arbitraje, ganancia
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        oportunidad['Fecha y Hora'],
        oportunidad['Par1'],
        oportunidad['Par2'],
        oportunidad['Par3'],
        float(oportunidad['Precio Compra Par1']),
        float(oportunidad['Precio Compra Par2']),
        float(oportunidad['Precio Venta Par3']),
        float(oportunidad['Cantidad Inicial USDT']),
        float(oportunidad['Cantidad Final USDT']),
        float(oportunidad['Ratio Arbitraje']),
        float(oportunidad['Ganancia'])
    ))
# WebSocket dedicado para mantener actualizado el precio de par2 en tiempo real
async def escuchar_precio_en_tiempo_real(symbol, stop_event):
    try:
        client = await AsyncClient.create(api_key=API_KEY, api_secret=API_SECRET)
        bm = BinanceSocketManager(client)
        socket = bm.symbol_ticker_socket(symbol.lower())
        # Registrar el tiempo de inicio
        tiempo_inicio = time.time()
        async with socket as stream:
            while not stop_event.is_set():
                msg = await stream.recv()
                if 'c' in msg:
                    # Registrar el tiempo cuando se recibe el primer mensaje
                    tiempo_primera_respuesta = time.time()
                    latencia_inicial = tiempo_primera_respuesta - tiempo_inicio
                    print(f"⏱️ Tiempo para recibir el primer mensaje de {symbol}: {latencia_inicial:.4f} segundos")

                    precio_actual = Decimal(msg['c'])
                    precios[symbol] = {
                        'price': precio_actual,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                    }
                    print(f"🔄 Precio actualizado de {symbol}: {precio_actual}")
                  
    except Exception as e:
        print(f"❌ Error en WebSocket de {symbol}: {e}")
    finally:
        try:
            await client.close_connection()
        except Exception as e:
            print(f"⚠️ Error al cerrar el cliente: {e}")




# Función auxiliar para ejecutar tareas en un hilo y capturar el resultado
def run_in_thread(func, *args):
    result = {}
    def wrapper():
        result['value'] = func(*args)
    t = threading.Thread(target=wrapper)
    t.start()
    return t, result

# Verificación en tiempo real del ratio de arbitraje
def es_arbitraje_valido(client, par1, par2, par3, umbral):
    try:
        precio1 = precios[par1]['price']
        precio2 = precios[par2]['price']
        precio3 = precios[par3]['price']
        ratio = (precio3 / precio2) / precio1
        print(f"📈 Verificación de ratio en tiempo real (WebSocket): {ratio:.6f}")
        return ratio >= umbral
    except Exception as e:
        print(f"⚠️ Error usando precios de WebSocket: {e}")
        return False

# === GLOBAL: WebSocket activo para precios en tiempo real ===
ws_activos = {}

def iniciar_websocket_par(symbol):
    if symbol in ws_activos:
        return  # Ya está corriendo

    stop_event = threading.Event()

    def websocket_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(escuchar_precio_en_tiempo_real(symbol, stop_event))

    thread = threading.Thread(target=websocket_thread)
    thread.start()
    ws_activos[symbol] = (thread, stop_event)

def calcular_cantidad_con_comision_objetivo(cantidad_objetivo, tasa_comision=Decimal('0.001')):
    """
    Calcula la cantidad bruta a comprar para recibir una cantidad neta (después de comisión).
    """
    return cantidad_objetivo / (Decimal('1') - tasa_comision)

def compra(symbol, cantidad):
    try:
        order = client.create_margin_order(
            symbol=symbol,
            side='BUY',
            type='MARKET',
            quantity=float(cantidad),
            isIsolated='FALSE',  # Cross Margin
            sideEffectType='NO_SIDE_EFFECT'  # Compra normal sin préstamo
        )

        print(f"✅ COMPRA normal ejecutado ({symbol}): {order}")
        return order
    except Exception as e:
        print(f"❌ Error COMPRA normal ({symbol}): {e}")

def short_compra(symbol, cantidad,cantidad_base_neta):
    print(f"⏱️ inicio solicitud de prestamo {datetime.now().strftime('%H:%M:%S.%f')}")
    time.sleep(0.2)
    prestamo = solicitar_prestamo_para_par(symbol, cantidad_base_neta)
    if not prestamo or 'tranId' not in prestamo:
        print(f"❌ Préstamo fallido para {symbol}. Deteniendo arbitraje.")
        sys.exit()
        return
    else:
        print(f"✅ Préstamo confirmado: tranId {prestamo['tranId']}")
    try:
        order = client.create_margin_order(
            symbol=symbol,
            side='BUY',
            type='MARKET',
            quantity=float(cantidad),
            isIsolated='FALSE',
            sideEffectType='MARGIN_BUY'  # Solución ✅
        )

        print(f"✅ Short de COMPRA ejecutado ({symbol}): {order}")
        return order
    except Exception as e:
        print(f"❌ Error short compra ({symbol}): {e}")
        sys.exit()
        
def short_venta(symbol, cantidad):
    try:
        order = client.create_margin_order(
            symbol=symbol,
            side='SELL',
            type='MARKET',
            quantity=float(cantidad),
            isIsolated='FALSE',
            sideEffectType='MARGIN_BUY'  # Solución ✅
        )

        print(f"✅ Short de VENTA ejecutado ({symbol}): {order}")
        return order
    except Exception as e:
        print(f"❌ Error short venta ({symbol}): {e}")
    
def solicitar_prestamo_para_par(symbol, cantidad_quote):
    try:
        # Obtener quoteAsset desde el diccionario en caché
        quote_asset = symbol_info_cache[symbol]['quote']
        
        # Calcular monto ajustado para cubrir comisión e intereses
        cantidad_ajustada = calcular_total_cobertura(Decimal(cantidad_quote)).quantize(Decimal('1e-8'))#aqui se puedo sacar el decima de ajuste ?
        
        print(f"💸 Solicitando préstamo de {cantidad_ajustada} {quote_asset} para {symbol} (cobertura incluida)...")
        
        response = client.create_margin_loan(
            asset=quote_asset,
            amount=str(cantidad_ajustada),
            isIsolated='FALSE'
        )
        print(f"✅ Préstamo solicitado de {cantidad_ajustada} {quote_asset} para operar en {symbol}")
        return response

    except KeyError:
        print(f"❌ Error: el par {symbol} no está en el cache de símbolos de margin.")
        return None
    except Exception as e:
        print(f"❌ Error al solicitar préstamo de {symbol}: {e}")
        return None

def calcular_total_cobertura(valor_prestamo):      
    interes_estimado = Decimal('0.0002')  # 0.02% conservador  comision 0.1%
    return valor_prestamo * (Decimal('1') - comision - interes_estimado)

# Función para ejecutar el arbitraje
def ejecutar_arbitraje(client, symbol_info_cache, oportunidades, basealt, cantidad_base_neta):
    if not oportunidades:
        print("⚠️ No hay oportunidades disponibles.")
        return

    oportunidad = max(oportunidades, key=lambda x: x['Ratio Arbitraje'])
    par1, par2, par3 = oportunidad['Par1'], oportunidad['Par2'], oportunidad['Par3']
    ratio_minimo = oportunidad['Ratio Arbitraje'] * Decimal('0.995')

    if not es_arbitraje_valido(client, par1, par2, par3, ratio_minimo):
        print("❌ El arbitraje ya no es rentable.")
        return

    loan_manager = MarginLoanManager(client, symbol_info_cache)
    executor = ArbitrageExecutor(client, loan_manager)
    executor.execute_trio(par1, par2, par3, base_amount=cantidad_base_neta, alt_amount=basealt)

class MarginLoanManager:
    def __init__(self, client, symbol_info_cache):
        self.client = client
        self.symbol_info_cache = symbol_info_cache

    def request_loan(self, symbol, amount):
        max_retries = 3
        asset = self._get_quote_asset(symbol)
        adjusted_amount = self._calculate_adjusted_amount(amount)

        for attempt in range(max_retries):
            try:
                print(f"⏱️ Ejecutando solicitude de prestamo {datetime.now().strftime('%H:%M:%S.%f')}")
                print(f"💸 Solicitando préstamo de {adjusted_amount} {asset} para {symbol}...")
                response = self.client.create_margin_loan(
                    asset=asset,
                    amount=str(adjusted_amount),
                    isIsolated='FALSE'
                )
                if self._verify_loan(asset, adjusted_amount):
                    print(f"✅ Préstamo confirmado: tranId {response['tranId']}")
                    return response
            except Exception as e:
                print(f"❌ Error al solicitar préstamo: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(1.2 * (attempt + 1))
        raise Exception("Préstamo fallido tras varios intentos.")
    print(f"⏱️ verificando prestamo {datetime.now().strftime('%H:%M:%S.%f')}")
    def _verify_loan(self, asset, amount, timeout=5):
        print(f"⏱️ Verificando préstamo para {asset}...")
        start_time = time.time()
        min_expect = Decimal(amount) * Decimal('0.95')

        while time.time() - start_time < timeout:
            margin_info = self.client.get_margin_account()
            balances = {b['asset']: b for b in margin_info['userAssets']}
            borrowed = Decimal(balances.get(asset, {}).get('borrowed', '0'))
            free = Decimal(balances.get(asset, {}).get('free', '0'))

            print(f"   🔍 borrowed: {borrowed}, free: {free} / esperando al menos: {min_expect}")

            if borrowed >= min_expect:
                print(f"🔄 Préstamo registrado: {borrowed} {asset}")

            if free >= min_expect:
                print(f"✅ Préstamo disponible para operar: {free} {asset}")
                return True

            time.sleep(0.2)

        print("❌ Timeout esperando disponibilidad del préstamo.")
        return False


    def _get_quote_asset(self, symbol):
        return self.symbol_info_cache[symbol]['quote']

    def _calculate_adjusted_amount(self, amount):
        comision = Decimal('0.001')
        interes_estimado = Decimal('0.0002')
        return float(Decimal(amount) * (Decimal('1') + comision + interes_estimado))

class ArbitrageExecutor:
    def __init__(self, client, loan_manager):
        self.client = client
        self.loan_manager = loan_manager

    def execute_trio(self, par1, par2, par3, base_amount, alt_amount):
        print(f"⏱️ Ejecutando arbitraje a las {datetime.now().strftime('%H:%M:%S.%f')}")
        try:
            # 1. Préstamo anticipado para par2
            self.loan_manager.request_loan(par2, base_amount)
            time.sleep(0.15)  # Esperar un poco para asegurar que el préstamo se registre
            # 2. Ejecutar las órdenes
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_short_buy = executor.submit(self._short_buy, par2, alt_amount)
                time.sleep(0.3)  # Darle prioridad a la orden más volátil

                future_normal_buy = executor.submit(self._normal_buy, par1, base_amount)
                future_short_sell = executor.submit(self._short_sell, par3, alt_amount)

                results = {
                    'short_buy': future_short_buy.result(),
                    'normal_buy': future_normal_buy.result(),
                    'short_sell': future_short_sell.result()
                }

            print("✅ Todas las órdenes han sido ejecutadas.")
            return results

        except Exception as e:
            print(f"❌ Error durante la ejecución del arbitraje: {e}")
            self._emergency_cleanup()
            raise

    def _normal_buy(self, symbol, quantity):
        print(f"🔹 Ejecutando COMPRA normal ({symbol})")
        return self.client.create_margin_order(
            symbol=symbol,
            side='BUY',
            type='MARKET',
            quantity=float(quantity),
            isIsolated='FALSE',
            sideEffectType='NO_SIDE_EFFECT'
        )

    def _short_buy(self, symbol, quantity):
        print(f"🔹 Ejecutando SHORT COMPRA ({symbol})")
        return self.client.create_margin_order(
            symbol=symbol,
            side='BUY',
            type='MARKET',
            quantity=float(quantity),
            isIsolated='FALSE',
            sideEffectType='MARGIN_BUY'
        )

    def _short_sell(self, symbol, quantity):
        print(f"🔹 Ejecutando SHORT VENTA ({symbol})")
        return self.client.create_margin_order(
            symbol=symbol,
            side='SELL',
            type='MARKET',
            quantity=float(quantity),
            isIsolated='FALSE',
            sideEffectType='MARGIN_BUY'
        )

    def _emergency_cleanup(self):
        print("⚠️ Ejecutando limpieza de emergencia... (implementa cancelaciones o logs si es necesario)")
        # Aquí podrías implementar cancelaciones de órdenes abiertas, repago de préstamos, etc.

def repaso_arbitraje():
    try:
        print("\n📊 REPASO DEL ARBITRAJE MARGIN CROSS:")

        # Obtener deuda y activos del wallet Margin Cross
        cross_account = client.get_margin_account()
        
        activos_con_deuda = []
        saldo_total = Decimal('0')

        for asset in cross_account['userAssets']:
            asset_name = asset['asset']
            borrowed = Decimal(asset['borrowed'])
            interest = Decimal(asset['interest'])
            free = Decimal(asset['free'])

            if borrowed > 0 or interest > 0:
                activos_con_deuda.append({
                    'activo': asset_name,
                    'prestado': borrowed,
                    'interes': interest,
                    'disponible': free
                })

        if not activos_con_deuda:
            print("✅ No tienes deudas activas en Margin Cross. Todo fue repagado correctamente con AUTO_BORROW_REPAY.")
        else:
            print("❗ Se detectan deudas activas en Margin Cross:")
            for deuda in activos_con_deuda:
                print(f"🔹 {deuda['activo']}: Prestado = {deuda['prestado']}, Interés = {deuda['interes']}, Disponible = {deuda['disponible']}")
                
                if deuda['disponible'] >= deuda['prestado'] + deuda['interes']:
                    print(f"   🟢 Puedes repagar ahora mismo el total usando el balance disponible.")
                else:
                    faltante = (deuda['prestado'] + deuda['interes']) - deuda['disponible']
                    print(f"   🔴 Te faltan {faltante} {deuda['activo']} para repagar todo.")

    except Exception as e:
        print(f"❌ Error al revisar el estado del arbitraje: {e}")
  
def convertir_dust_a_bnb_y_usdt(client):
    
    try:

        # Obtener el balance actual de USDT (Spot)
        balance_actual_spot = Decimal(client.get_asset_balance(asset='USDT', recvWindow=10000)['free'])
        print(f"Balance actual en Spot (USDT): {balance_actual_spot}")

        # Obtener todos los balances (Spot)
        account_info = client.get_account(recvWindow=10000)
        balances = account_info['balances']
        
        # Filtrar activos con saldo disponible, excepto USDT
        activos_a_evaluar = {b['asset']: Decimal(b['free']) for b in balances if Decimal(b['free']) > 0 and b['asset'] != 'USDT'}
        print(f"Activos a evaluar (excluyendo USDT): {activos_a_evaluar}")

        valor_total_usdt = Decimal('0')
        detalles_valores = {}
        
        # Calcular el valor en USDT de cada activo
        for asset, cantidad in activos_a_evaluar.items():
            symbol = f"{asset}USDT"
            try:
                # Obtener el precio del activo en USDT
                ticker = client.get_symbol_ticker(symbol=symbol)
                precio_usdt = Decimal(ticker['price'])

                # Calcular el valor total en USDT
                valor_en_usdt = cantidad * precio_usdt
                detalles_valores[asset] = valor_en_usdt
                valor_total_usdt += valor_en_usdt

                print(f"{asset}: {cantidad} * {precio_usdt} = {valor_en_usdt} USDT")
            except Exception as e:
                print(f"No se pudo obtener el precio de {asset} en USDT: {e}")

        # Imprimir el valor total de todos los activos en USDT
        print(f"Valor total de los saldos (sin incluir USDT): {valor_total_usdt} USDT")

        # Obtener balance actual de Futuros USDT
        try:
            futures_balance = client.futures_account_balance()
            balance_actual_futuros = next((Decimal(f['balance']) for f in futures_balance if f['asset'] == 'USDT'), Decimal('0'))
            print(f"📊 Balance actual en Futuros USDT: {balance_actual_futuros}")
        except Exception as e:
            print(f"⚠️ Error obteniendo balance de Futuros: {e}")
            balance_actual_futuros = Decimal('0')

        # Calcular el balance total actual (Spot USDT + Altcoins + Futuros USDT)
        balance_total_actual = balance_actual_spot + valor_total_usdt + balance_actual_futuros
        print(f"Balance total actual (Spot USDT + Altcoins + Futuros USDT): {balance_total_actual} USDT")

        # Retornar la información de los activos y el valor total
        return detalles_valores, balance_total_actual

    except Exception as e:
        print(f"Error al obtener el valor de los saldos en USDT: {e}")
        return None, None
try:
    while True:
        # Medir el tiempo de inicio del ciclo
        tiempo_inicio = time.time()

        # Buscar oportunidades de arbitraje
        oportunidades, registros_guardados = encontrar_oportunidad_arbitraje(client)

        # Medir el tiempo total del ciclo
        tiempo_total_ciclo = time.time() - tiempo_inicio
        print(f"⏱️ Tiempo total del ciclo: {tiempo_total_ciclo:.4f} segundos")

        # Mostrar resumen cada 10 ciclos
        contador += 1
        if contador % 10 == 0:
            print(f"🔁 Iteración: {contador} | Oportunidades detectadas: {contador_oportunidades} | Ganancia total: {total_ganancias:.4f} USDT")

        # Esperar antes de iniciar el siguiente ciclo
     
        time.sleep(5)

except KeyboardInterrupt:
    # Manejo de interrupción manual
    print("🛑 Script detenido manualmente.")
    closing = True

    # Detener todos los hilos del WebSocket
    for thread in ws_threads:
        thread.join()

    print("✅ Todos los hilos del WebSocket han sido detenidos. Finalizando el script.")
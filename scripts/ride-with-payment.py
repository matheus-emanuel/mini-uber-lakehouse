from concurrent.futures import ThreadPoolExecutor
import psycopg2
import random
import time
import math

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "uber-race",
    "user": "postgresql",
    "password": "postgresql"
}

LAT_MIN, LAT_MAX = -3.90, -3.65
LON_MIN, LON_MAX = -38.70, -38.40

# ---------------- FUNÇÕES AUXILIARES ----------------

def gerar_ponto():
    return (
        random.uniform(LAT_MIN, LAT_MAX),
        random.uniform(LON_MIN, LON_MAX)
    )

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def calcular_duracao(distancia_km):
    velocidade = random.uniform(20, 40)
    return max(3, round((distancia_km / velocidade) * 60))

def calcular_preco(distancia_km, duracao_min):
    bandeirada = 5
    preco_km = 2.5
    preco_min = 0.4
    return round(bandeirada + distancia_km * preco_km + duracao_min * preco_min, 2)

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# ---------------- ETAPA 1: CRIAR CORRIDA ----------------

def criar_corrida():
    origem_lat, origem_lon = gerar_ponto()
    destino_lat, destino_lon = gerar_ponto()

    distancia = haversine(origem_lat, origem_lon, destino_lat, destino_lon)
    duracao = calcular_duracao(distancia)
    valor = calcular_preco(distancia, duracao)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO uberprd.corridas (
            passageiro_id, motorista_id,
            origem_latitude, origem_longitude,
            destino_latitude, destino_longitude,
            distancia_km, duracao_min,
            valor_estimado, status, data_solicitacao
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'solicitada',NOW())
        RETURNING id, valor_estimado;
    """, (
        random.randint(1,100),
        random.randint(1,50),
        origem_lat, origem_lon,
        destino_lat, destino_lon,
        round(distancia,2),
        duracao,
        valor
    ))

    corrida_id, valor_estimado = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    print(f"[CRIADA] Corrida {corrida_id} -> solicitada")
    return corrida_id, valor_estimado

# ---------------- ETAPA 2: EM ANDAMENTO ----------------

def iniciar_corrida(corrida_id):
    time.sleep(random.randint(1,3))

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE uberprd.corridas
        SET status = 'em_andamento',
            data_inicio = NOW()
        WHERE id = %s;
    """, (corrida_id,))

    conn.commit()
    cur.close()
    conn.close()

    print(f"[UPDATE] Corrida {corrida_id} -> em_andamento")

# ---------------- ETAPA 3: FINALIZAR OU CANCELAR ----------------

def finalizar_ou_cancelar(corrida_id, valor_estimado):
    time.sleep(random.randint(3,7))

    cancelada = random.random() < 0.15  # 15% de cancelamento

    conn = get_conn()
    cur = conn.cursor()

    if cancelada:
        cur.execute("""
            UPDATE uberprd.corridas
            SET status = 'cancelada',
                data_fim = NOW()
            WHERE id = %s;
        """, (corrida_id,))
        print(f"[CANCELADA] Corrida {corrida_id}")

    else:
        cur.execute("""
            UPDATE uberprd.corridas
            SET status = 'finalizada',
                valor_final = %s,
                data_fim = NOW()
            WHERE id = %s;
        """, (valor_estimado, corrida_id))

        print(f"[FINALIZADA] Corrida {corrida_id}")

        criar_pagamento(corrida_id, valor_estimado)

    conn.commit()
    cur.close()
    conn.close()

# ---------------- ETAPA 4: CRIAR PAGAMENTO ----------------

def criar_pagamento(corrida_id, valor):
    forma_pagamento = random.choice(['credito', 'debito', 'pix', 'dinheiro'])

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO uberprd.pagamentos (
            corrida_id, forma_pagamento, status, valor
        )
        VALUES (%s, %s, 'pendente', %s)
        RETURNING id;
    """, (corrida_id, forma_pagamento, valor))

    pagamento_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    print(f"[PAGAMENTO] Criado {pagamento_id} -> pendente")

    processar_pagamento(pagamento_id)

# ---------------- ETAPA 5: PROCESSAR PAGAMENTO ----------------

def processar_pagamento(pagamento_id):
    time.sleep(random.randint(2,3))

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE uberprd.pagamentos
        SET status = 'processando'
        WHERE id = %s;
    """, (pagamento_id,))

    conn.commit()
    cur.close()
    conn.close()

    print(f"[PAGAMENTO] {pagamento_id} -> processando")

    finalizar_pagamento(pagamento_id)

# ---------------- ETAPA 6: FINALIZAR PAGAMENTO ----------------

def finalizar_pagamento(pagamento_id):
    time.sleep(random.randint(1,3))

    falhou = random.random() < 0.08  # 8% de falha no pagamento

    status_final = 'falhou' if falhou else 'pago'

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE uberprd.pagamentos
        SET status = %s,
            data_pagamento = NOW()
        WHERE id = %s;
    """, (status_final, pagamento_id))

    conn.commit()
    cur.close()
    conn.close()

    print(f"[PAGAMENTO] {pagamento_id} -> {status_final}")

# ---------------- LOOP PRINCIPAL ----------------

def simular_corrida_com_pagamento():
    corrida_id, valor = criar_corrida()
    iniciar_corrida(corrida_id)
    finalizar_ou_cancelar(corrida_id, valor)

# def loop_simulacao():
#     while True:
#         simular_corrida_com_pagamento()
#         time.sleep(random.randint(3,5))

# ---------------- PARALELISMO ----------------

def worker():
    while True:
        try:
            simular_corrida_com_pagamento()
            time.sleep(random.randint(1,4))
        except Exception as e:
            print("Erro no worker:", e)

def loop_paralelo(qtd_threads=5):
    with ThreadPoolExecutor(max_workers=qtd_threads) as executor:
        for _ in range(qtd_threads):
            executor.submit(worker)

# ---------------- MAIN ----------------

if __name__ == "__main__":
    loop_paralelo(qtd_threads=10)

import psycopg2
from psycopg2 import sql


# Função para conectar ao banco de dados PostgreSQL
def conexao():
    try:
        conn = psycopg2.connect(
            dbname="loja_virtual",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None
    

def list_pedidos(conn, id):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM pedidos WHERE cliente_id = {id}")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")


def itens_pedido_especifico(conn, query_opcao):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"select itens.id, itens.nome, itens.descricao, itens.preco, itens.estoque itens from itens_pedido\
                        join itens on itens.id = itens_pedido.item_id \
                        join pedidos on pedidos.id = itens_pedido.pedido_id \
                        where pedidos.id = {query_opcao}")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")


def list_client_data(conn, init, end):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT pedidos.cliente_id, clientes.nome FROM pedidos \
                            JOIN clientes on pedidos.id = clientes.id \
                            WHERE pedidos.data_pedido BETWEEN '{init}' AND '{end}' ")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")

def pedidos_clientes_respectivos(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("select pedidos.id, pedidos.data_pedido, pedidos.status, clientes.id, clientes.nome from pedidos \
                            join clientes on pedidos.cliente_id = clientes.id")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")


def list_item_vendedor(conn, id):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"   SELECT itens.id,itens.nome FROM itens_pedido \
                                JOIN pedidos ON itens_pedido.pedido_id = pedidos.id \
                                JOIN itens ON itens_pedido.item_id = itens.id \
                                WHERE pedidos.vendedor_id = {id};")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")

def pagamentos_intervalo_data(conn, data_inicio, data_fim):
    try:
        with conn.cursor() as cursor:
            cursor.execute("select * from pedidos \
                            where pedidos.status = 'Concluído' and pedidos.data_pedido between '2024-10-15' and '2025-12-31'")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")



def list_forma_pagamento(conn, pagamento):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"   SELECT pedidos.id, formas_pagamento.descricao FROM pedidos \
                                JOIN formas_pagamento ON pedidos.forma_pagamento_id = formas_pagamento.id \
                                WHERE formas_pagamento.descricao = '{pagamento}';")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")



def clientes_e_enderecos(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("select clientes.id, clientes.nome, concat(enderecos.rua, ', ', enderecos.cidade, ', ', enderecos.estado)  as endereço  from clientes \
                    join enderecos on enderecos.cliente_id = clientes.id")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")


def list_vendedor_data(conn, init, end):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT pedidos.vendedor_id, vendedores.nome, pedidos.data_pedido FROM pedidos \
                            JOIN vendedores on pedidos.id = vendedores.id \
                            WHERE pedidos.data_pedido BETWEEN '{init}' AND '{end}' ")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")

def itens_fora_estoque(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("select * from itens\
                        where itens.estoque = 0")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Erro ao ler dados: {e}")
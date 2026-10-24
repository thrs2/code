
import json
import pika
import uuid

conexao = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

canal = conexao.channel()

canal.queue_declare(
    queue="notificacoes",
    durable=True
)

pedido = {
    "id": str(uuid.uuid4()),
    "cliente": "Maria",
    "valor": 150.00
}

mensagem = json.dumps(pedido, ensure_ascii=False)

canal.basic_publish(
    exchange="",
    routing_key="notificacoes",
    body=mensagem,
    properties=pika.BasicProperties(
        delivery_mode=2,
        content_type="application/json"
    )
)

print(f"Pedido enviado: {mensagem}")

conexao.close()

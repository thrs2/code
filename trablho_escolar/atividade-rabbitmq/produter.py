
import json
import pika
from datetime import datetime

conexao = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

canal = conexao.channel()

canal.queue_declare(
    queue="notificacoes",
    durable=True
)

# Processa uma mensagem por vez.
canal.basic_qos(prefetch_count=1)


def processar_mensagem(canal, metodo, propriedades, corpo):
    pedido = json.loads(corpo.decode("utf-8"))

    print(
        f"[{datetime.now()}] "
        f"Pedido ID={pedido['id']} | "
        f"Cliente={pedido['cliente']} | "
        f"Valor=R${pedido['valor']:.2f}"
    )

    # Confirma que o processamento terminou.
    canal.basic_ack(
        delivery_tag=metodo.delivery_tag
    )


canal.basic_consume(
    queue="notificacoes",
    on_message_callback=processar_mensagem,
    auto_ack=False
)

print("Consumidor aguardando mensagens...")

try:
    canal.start_consuming()
except KeyboardInterrupt:
    print("\nEncerrando consumidor...")
    canal.stop_consuming()
finally:
    conexao.close()

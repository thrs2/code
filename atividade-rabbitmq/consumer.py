
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



# Guarda os IDs que já falharam na primeira tentativa.
falhas_simuladas = set()


def processar_mensagem(canal, metodo, propriedades, corpo):
    pedido = json.loads(corpo.decode("utf-8"))
    pedido_id = pedido["id"]

    print(
        f"Recebido ID={pedido_id} | "
        f"Redelivery={metodo.redelivered}"
    )

    if pedido_id not in falhas_simuladas:
        falhas_simuladas.add(pedido_id)

        print("Falha simulada! Devolvendo mensagem à fila.")

        canal.basic_nack(
            delivery_tag=metodo.delivery_tag,
            requeue=True
        )
        return

    print(
        f"Processamento concluído: "
        f"cliente={pedido['cliente']}, "
        f"valor=R${pedido['valor']:.2f}"
    )

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

import base64
from io import BytesIO

from PIL import Image
from azure.servicebus import ServiceBusMessage, ServiceBusClient, ServiceBusReceiveMode
from flask import Flask, render_template, request, redirect, make_response

app = Flask(__name__)
app_queue = "appqueue"
web_queue = "webqueue"
connection_str = "Endpoint=sb://lab204servicebussuriya.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=s3zh+wqoJE64ndS2mS2W3pmVpC0BuQATCIqwjgDGpJ8="


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/', methods=['POST'])
def upload_image():
    if 'file' not in request.files:
        print('No file part')
        return redirect(request.url)
    file = request.files['file']
    print(file.filename)
    img = Image.open(file.stream)
    buffered = BytesIO()
    img.save(buffered, format="JPEG")
    img_str = base64.b64encode(buffered.getvalue()).decode()
    message = ServiceBusMessage(body=img_str, content_type="text/plain")
    print(str(message.body))
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str, logging_enable=True)
    with servicebus_client:
        # get a Queue Sender object to send messages to the queue
        sender = servicebus_client.get_queue_sender(queue_name=app_queue)
        with sender:
            sender.send_messages(message)
    print(f"Message published to topic: {app_queue}")
    return redirect(request.url)


@app.route('/view', methods=['GET'])
def retrieve_img():
    res = {}
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str, logging_enable=True)
    with servicebus_client:
        # get the Queue Receiver object for the queue
        receiver = servicebus_client.get_queue_receiver(queue_name=web_queue, max_wait_time=5, receive_mode=ServiceBusReceiveMode.PEEK_LOCK)
        msgs = receiver.receive_messages(max_message_count=1)
        if len(msgs) == 1:
            msg = msgs[0]
            print(f"Received: {msg.body}.")
            res["dec_str"] = str(msg)
            receiver.complete_message(message=msg)

    if len(res) < 1:
        return "fail", 400
    else:
        response = make_response(res["dec_str"])
        response.headers.set('Content-Type', 'image/gif')
        response.headers.set('Content-Disposition', 'attachment', filename='image.gif')
        return response


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True)

from kafka import KafkaProducer
from kafka.errors import KafkaError
import wiringpi
from time import sleep
from flask import Flask
from flask_mail import Mail, Message

wiringpi.wiringPiSetup()
DIGITAL_INPUT = 1
DIGITAL_OUTPUT = 4
INPUT = 0
OUTPUT = 1
wiringpi.pinMode(DIGITAL_INPUT,INPUT)
wiringpi.pinMode(DIGITAL_OUTPUT,OUTPUT)
producer = KafkaProducer(bootstrap_servers=['35.224.157.91:9094'])
#Change bootstrap IP as in cloud kafka IP
app = Flask(__name__)
mail_settings = {
        "MAIL_SERVER": 'smtp.gmail.com',
        "MAIL_USE_TLS": True,
        "MAIL_USE_SSL": False,
        "MAIL_PORT": 587,
        "MAIL_USERNAME": 'xxxxx@gmail.com',
        "MAIL_PASSWORD": 'xxxx'
    }

app.config.update(mail_settings)
mail = Mail(app)
count = 0
while True:
	if wiringpi.digitalRead(DIGITAL_INPUT):
	    wiringpi.digitalWrite(DIGITAL_OUTPUT,1)
            # Asynchronous by default
            future = producer.send('my-topic', b'1')
            count = count + 1
	else:
            # Asynchronous by default
            future = producer.send('my-topic', b'0')
	    wiringpi.digitalWrite(DIGITAL_OUTPUT,0)
            count = 0
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
        # Decide what to do if produce request failed...
            log.exception()
            pass
        if (count == 2):
            with app.app_context():
                msg = Message(sender=app.config.get("MAIL_USERNAME"),
                                    recipients=["xxxxx@gmail.com"])
                msg.subject = " Notification: Please Turn off your room light "
                msg.body = """ Please turn off your light when its not in use """
                mail.send(msg)
        # Successful result returns assigned partition and offset
        #print (record_metadata.topic)
        #print (record_metadata.partition)
        #print (record_metadata.offset)
        sleep(30)

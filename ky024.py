import wiringpi
from time import sleep
wiringpi.wiringPiSetup()
DIGITAL_INPUT = 1
DIGITAL_OUTPUT = 4
INPUT = 0
OUTPUT = 1
wiringpi.pinMode(DIGITAL_INPUT,INPUT)
wiringpi.pinMode(DIGITAL_OUTPUT,OUTPUT)
while True:
	if wiringpi.digitalRead(DIGITAL_INPUT):
		wiringpi.digitalWrite(DIGITAL_OUTPUT,1)
	else:
		wiringpi.digitalWrite(DIGITAL_OUTPUT,0)
	sleep(1)

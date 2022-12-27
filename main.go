package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/djfurman/go-micro-listener-service/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// try to connect to RabbitMQ
	rabbitConn, err := connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer rabbitConn.Close()

	// start listening for messages
	log.Println("Listening for and consuming RabbitMQ messages")

	// create consumer
	consumer, err := event.NewConsumer(rabbitConn)
	if err != nil {
		panic(err)
	}

	// watch the queue and consume events
	err = consumer.Listen([]string{"log.INFO", "log.WARNING", "log.Error"})
	if err != nil {
		log.Println(err)
	}
}

func connect() (*amqp.Connection, error) {
	var counts int64
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	// don't continue until RabbitMQ is ready

	for {
		c, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s", os.Getenv("RABBIT_CLIENT_ID"), os.Getenv("RABBIT_CLIENT_SECRET"), os.Getenv("RABBIT_HOST")))
		if err != nil {
			fmt.Println("RabbitMQ not yet ready")
			counts++
		} else {
			log.Println("Successfully connected to RabbitMQ")
			connection = c
			break
		}

		if counts > 5 {
			fmt.Println(err)
			return nil, err
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Printf("Backing off for %d seconds...", backOff)
		time.Sleep(backOff)
		continue
	}

	return connection, nil
}

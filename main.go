package main
import (
    "github.com/spf13/cobra"
    "github.com/streadway/amqp"
    "log"
    "fmt"
    "io/ioutil"
    "encoding/json"
    "strconv"
    "os/exec"
)

var cfgFile string;
var Config ConfigData;

// Structure containing the config data
type ConfigData struct {
    Stocks        []string    `json:"stocks"`
    Rabbitmq_host string      `json:"rabbitmq_host"`
    Rabbitmq_port int         `json:"rabbitmq_port"`
    Rabbitmq_user string      `json:"rabbitmq_user"`
    Rabbitmq_pass string      `json:"rabbitmq_pass"`
}

func main() {
    var cmdMain = &cobra.Command{
        Use: "stock",
        Short: "Get stock prices of stocks and send them to rabbitmq",
        Long: "Get the stock prices of a list of given stocks and send them to rabitmq",
    }
    cmdMain.PersistentFlags().StringVar(&cfgFile, "config", "", "config file with stock and connection information")
    cmdMain.Execute()

    readConfig();
    listenForUpdates();
}

func readConfig() {
    content, err := ioutil.ReadFile(cfgFile)
    failOnError(err, "Unable to open file")
    failOnError(json.Unmarshal(content, &Config), "Unable to parse json data")
}

func listenForUpdates() {
    var amqpAddress = "amqp://" + Config.Rabbitmq_user + ":" + Config.Rabbitmq_pass + "@" + Config.Rabbitmq_host + ":" + strconv.Itoa(Config.Rabbitmq_port)
    conn, err := amqp.Dial(amqpAddress)
    failOnError(err, "Failed to connect to RabbitMQ")
    defer conn.Close()

    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")
    defer ch.Close()

    err = ch.ExchangeDeclare(
        "amq.topic", // name
        "topic", // type
        true, // durable
        false, // auto-deleted
        false, // internal
        false, // no-wait
        nil, // arguments
    )
    failOnError(err, "Failed to declare an exchange")

    q, err := ch.QueueDeclare(
        "stocks_update", // name
        false, // durable
        false, // delete when usused
        true, // exclusive
        false, // no-wait
        nil, // arguments
    )
    failOnError(err, "Failed to declare a queue")

    err = ch.QueueBind(
        q.Name, // queue name
        "update", // routing key
        "amq.topic", // exchange
        false,
        nil)
    failOnError(err, "Failed to bind a queue")

    msgs, err := ch.Consume(
        q.Name, // queue
        "", // consumer
        true, // auto ack
        false, // exclusive
        false, // no local
        false, // no wait
        nil, // args
    )
    failOnError(err, "Failed to register a consumer")

    forever := make(chan bool)

    go func() {
        for _ = range msgs {
            log.Print("Update message received")
            var cmd = exec.Command("./stocks", "--config="+cfgFile)
            err = cmd.Run()
            failOnError(err, "Error running command")
        }
    }()

    log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
    <-forever
}

func failOnError(err error, msg string) {
    if err != nil {
        log.Fatalf("%s: %s", msg, err)
        panic(fmt.Sprintf("%s: %s", msg, err))
    }
}
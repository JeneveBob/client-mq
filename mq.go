package mq

import (
	"fmt"

	"github.com/streadway/amqp"
)

type Mq struct {
	Host       string // mq链接地址
	Port       int    // mq链接端口号
	Vhost      string // 虚拟主机名
	Login      string // 登录用户名
	Password   string // 登录密码
	TimeOut    int    // http超时设置
	Verify     bool   // 是否开启tls双向证书认证
	HttpCa     string
	HttpCert   string
	HttpKey    string
	connection *amqp.Connection
	channel    *amqp.Channel
}

/*
 * 创建对象
 */
func New(host string, port int, login string, password string, vhost string) *Mq {
	return &Mq{
		Host:     host,
		Port:     port,
		Vhost:    vhost,
		Login:    login,
		Password: password,
		TimeOut:  1,
		Verify:   false,
		HttpCa:   "",
		HttpCert: "",
		HttpKey:  "",
	}
}

/*
 * 直连发送数据
 *
 * @param msg 发送的信息
 */
func (mq *Mq) DirectMsg(msg string, exchange string, routeKey string, queue string) error {
	if "" == msg || "" == exchange || "" == routeKey {
		return fmt.Errorf("illegal parameter.")
	}
	if err := mq.makeConnection(); err != nil {
		return err
	}
	defer mq.connection.Close()
	if err := mq.declareExchange(exchange, "direct"); err != nil {
		return err
	}
	if "" != queue {
		if err := mq.declareQueue(queue, routeKey, exchange); err != nil {
			return err
		}
	}
	if err := mq.publish(msg, routeKey, exchange); err != nil {
		return err
	}
	return nil
}

/*
 * 广播模式发送数据
 *
 * @param msg      发送的信息
 * @param exchange 通过哪个交换发送
 * @param queue    绑定哪个队列，若不需要绑定传空
 */
func (mq *Mq) BroadcastMsg(msg string, exchange string, queue string) error {
	if "" == msg || "" == exchange {
		return fmt.Errorf("illegal parameter.")
	}
	if err := mq.makeConnection(); err != nil {
		return err
	}
	defer mq.connection.Close()
	if err := mq.declareExchange(exchange, "fanout"); err != nil {
		return err
	}
	if "" != queue {
		if err := mq.declareQueue(queue, "", exchange); err != nil {
			return err
		}
	}
	if err := mq.publish(msg, "", exchange); err != nil {
		return err
	}

	return nil
}

/*
 * 创建链接
 */
func (mq *Mq) makeConnection() error {
	amqpURI := fmt.Sprintf("amqp://%s:%s@%s:%d/%s", mq.Login, mq.Password, mq.Host, mq.Port, mq.Vhost)
	// TODO 添加tls
	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	mq.connection = connection
	// connect channel
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("Connect Channel: %s", err)
	}
	mq.channel = channel
	return nil
}

/*
 * 申明交换类型
 */
func (mq *Mq) declareExchange(exchange string, exchangeType string) error {
	return mq.channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
}

/*
 * 申明队列并绑定对应交换
 */
func (mq *Mq) declareQueue(queue string, routeKey string, exchange string) error {
	if _, err := mq.channel.QueueDeclare(
		queue, // name
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // noWait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("Queue Declare: %s", err)
	}
	// queue bind
	if err := mq.channel.QueueBind(
		queue,
		routeKey,
		exchange,
		false, // noWait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("Queue Bind: %s", err)
	}

	return nil
}

/*
 * 发送数据
 */
func (mq *Mq) publish(body string, routeKey string, exchange string) error {
	// TODO Reliable publisher confirms.
	if err := mq.channel.Publish(
		exchange, // publish to an exchange
		routeKey, // routing to 0 or more queues
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
			Priority:        0,               // 0-9
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	return nil
}

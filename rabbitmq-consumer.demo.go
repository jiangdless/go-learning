// rabbitmq consumer demo
// 学习使用go 写一个rabbitmq的consumer 异步处理各种任务
package main

import (
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"github.com/streadway/amqp"
	"time"
	"github.com/go-xorm/xorm"
	"go-console/common"
	"strconv"
	"encoding/json"
	"go-console/taskManage/tasks"
	"strings"
	"github.com/go-redis/redis"
)

var (
	engine *xorm.Engine
	redisClient *redis.Client

	taskList []taskConf
	maxRoutineNum int
	maxRoutineNumCh chan int
)

type taskConf struct {
	TaskId		int
	Type        string
	Name		string
	EventList	string
	Activity	int64
	Ext 		taskExt
}

type taskExt struct {
	Num			int
}

func init() {
	now := time.Now()
	fmt.Println("run begin at ", now)

	engine = common.GetOrmEngine("wwjrmysql")
	redisClient = common.GetRedisCli("redis")
}

func main() {
    // 读取mq相关配置
	mqConfig := new(common.Config)
	mqConfig.InitConfig(mqConfig.GetConfPath()+"/service.conf")
	lifetime := mqConfig.Read("taskMq", "lifetime")

	routineNumStr := mqConfig.Read("taskMq", "routinenum")
	maxRoutineNum, _ = strconv.Atoi(routineNumStr)

    // 设置最大的消费者个数，开启maxRouuine个协程来消费队列
	maxRoutineNumCh = make(chan int, maxRoutineNum)
	for i:=0; i<maxRoutineNum; i++ {
		maxRoutineNumCh <- 1

		// 开启协程
		go newConsume(i, lifetime)
	}

	// 此处循环
	for ;len(maxRoutineNumCh) > 0; {
		time.Sleep(time.Second)
	}

	fmt.Println("shutting down all")
	// 进程结束后 由supervisor重新拉起
}

func newConsume(i int, lifetime string) {
	c, err := common.GetMqConsume("taskMq")
	if err != nil {
		fmt.Println("new consumer error", err)
	}

	go handle(c.Deliveries, c.Done)

	lifetimeSec, _ := strconv.Atoi(lifetime)
	if lifetimeSec > 0 {
		fmt.Println("running for %s", lifetimeSec)
		time.Sleep(time.Duration(lifetimeSec)*time.Second)
	} else {
		fmt.Println("running forever")
		select {}
	}

	fmt.Println("shutting down @@@"+strconv.Itoa(i))

    // 超时之后 关闭consumer
	if err := c.Shutdown(); err != nil {
		fmt.Println("error during shutdown:", err,"@@@"+strconv.Itoa(i))
	}

	<-maxRoutineNumCh
}

type msgModel struct {
	Type		string
	Uid			int
	CreatedAt	int64
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	var msg msgModel

	for d := range deliveries {
		// 从队列中读取读取消息，json decode之后 根据不同的type
		// 处理业务逻辑
		json.Unmarshal(d.Body, &msg)
		fmt.Println("receive msg ", msg)

		// 根据已有的任务类型，继续填入业务逻辑
		if msg.Type == "xxx" {
			doTask()
		}

        // 成功之后给出响应
		d.Ack(false)
	}

	fmt.Println("handle: deliveries channel closed")
	done <- nil
}

func doTask() {
    // business logic
}

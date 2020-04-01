package taskqueue

import (
	stan "github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
)

type TaskQueue struct {
	host       string
	clusterID  string
	clientName string
	client     stan.Conn
}

func CreateConnector(host string, clusterID string, clientName string) *TaskQueue {
	return &TaskQueue{
		host:       host,
		clusterID:  clusterID,
		clientName: clientName,
	}
}

func (tq *TaskQueue) Connect() error {

	log.WithFields(log.Fields{
		"host":       tq.host,
		"clientName": tq.clientName,
		"clusterID":  tq.clusterID,
	}).Info("Connecting to task queue")

	// Connect to queue server
	sc, err := stan.Connect(tq.clusterID, tq.clientName, stan.NatsURL(tq.host))
	if err != nil {
		return err
	}

	tq.client = sc

	return nil
}

func (tq *TaskQueue) Close() {
	tq.client.Close()
}

func (tq *TaskQueue) Emit(channelName string, data []byte) error {

	if err := tq.client.Publish(channelName, data); err != nil {
		return err
	}

	return nil
}

func (tq *TaskQueue) Subscribe(channelName string, queueName string, fn func(*stan.Msg)) (stan.Subscription, error) {

	sub, err := tq.client.QueueSubscribe(channelName, queueName, fn)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

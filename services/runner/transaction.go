package runner

import (
	"strconv"
	"sync"
	"time"
	app "twist-runner/app/interface"
	pb "twist-runner/pb"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type Transaction struct {
	App          app.AppImpl
	ID           string
	Tasks        []*Task
	PreConfirmed int
	Confirmed    int

	Context   context.Context
	Done      func()
	CmdSub    *nats.Subscription
	WaitGroup sync.WaitGroup
}

func CreateTransaction(id string, a app.AppImpl) *Transaction {
	return &Transaction{
		App:          a,
		ID:           id,
		Tasks:        make([]*Task, 0),
		PreConfirmed: 0,
		Confirmed:    0,
	}
}

func (t *Transaction) Start(timeout int) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	t.Context = ctx
	t.Done = cancel

	t.Run()
}

func (t *Transaction) InitEventHandler() error {

	sb := t.App.GetSignalBus()
	sub, err := sb.QueueSubscribe("twist.transaction."+t.ID+".cmdReceived", "twist-runner", func(msg *nats.Msg) {

		log.Info("Received command")

		var command pb.TransactionCommand
		err := proto.Unmarshal(msg.Data, &command)
		if err != nil {
			log.Warn(err)
			return
		}

		switch command.Command {
		case "confirm":
			var request pb.ConfirmTransactionRequest
			err := ptypes.UnmarshalAny(command.Payload, &request)
			if err != nil {
				log.Warn(err)
				return
			}

			t.ConfirmRequest(&request)

		case "cancel":
			t.Cancel()
		}

	})

	if err != nil {
		return err
	}

	t.CmdSub = sub

	return nil
}

func (t *Transaction) Run() {

	err := t.InitEventHandler()
	if err != nil {
		return
	}

	// Getting application ID
	idStr := strconv.FormatUint(t.App.GetID(), 16)

	// Prepare event
	event := &pb.TransactionEvent{
		TransactionID: t.ID,
		RunnerID:      idStr,
		EventName:     "",
		Payload:       "",
	}

	select {
	case <-t.Context.Done():

		// Success
		if t.Confirmed == len(t.Tasks) {
			log.WithFields(log.Fields{
				"id": t.ID,
			}).Info("transaction was completed")

			event.EventName = "Confirmed"

			break
		}

		// Failure
		switch t.Context.Err() {
		case context.Canceled:
			// Transaction was canceled
			log.WithFields(log.Fields{
				"id": t.ID,
			}).Error("transaction was canceled")

			event.EventName = "Canceled"

		case context.DeadlineExceeded:
			// Timeout
			log.WithFields(log.Fields{
				"id": t.ID,
			}).Error("transaction timeout")

			event.EventName = "Timeout"
		}
	}

	data, err := proto.Marshal(event)
	if err != nil {
		return
	}

	// Emit event
	sb := t.App.GetSignalBus()
	err = sb.Emit("twist.transaction."+t.ID+".eventEmitted", data)
}

func (t *Transaction) ConfirmRequest(request *pb.ConfirmTransactionRequest) {

	// Loading all tasks
	for _, transactionTask := range request.Tasks {

		task := CreateTask()

		task.ConfirmAction = &Action{
			Type:    transactionTask.Confirm.Type,
			Method:  transactionTask.Confirm.Method,
			Uri:     transactionTask.Confirm.Uri,
			Headers: transactionTask.Confirm.Headers,
			Payload: transactionTask.Confirm.Payload,
		}

		task.CancelAction = &Action{
			Type:    transactionTask.Cancel.Type,
			Method:  transactionTask.Cancel.Method,
			Uri:     transactionTask.Cancel.Uri,
			Headers: transactionTask.Cancel.Headers,
			Payload: transactionTask.Cancel.Payload,
		}

		t.RegisterTask(task)
	}

	// Start to do action
	t.PreConfirm()
}

func (t *Transaction) RegisterTask(task *Task) {
	t.Tasks = append(t.Tasks, task)
}

func (t *Transaction) PreConfirm() {

	log.WithFields(log.Fields{
		"id":    t.ID,
		"tasks": len(t.Tasks),
	}).Info("Pre-Confirm transaction")

	// Pre-Confirm
	for _, task := range t.Tasks {

		t.WaitGroup.Add(1)

		go func(t *Transaction, task *Task) {

			defer t.WaitGroup.Done()

			err := task.PreConfirm(t)
			if err == nil {
				t.PreConfirmed++
				return
			}
		}(t, task)
	}

	t.WaitGroup.Wait()

	// Success
	if t.PreConfirmed == len(t.Tasks) {
		// Second phase
		t.Confirm()
		return
	}

	// Cancel all tasks
	t.Cancel()
}

func (t *Transaction) Confirm() {

	log.WithFields(log.Fields{
		"id":    t.ID,
		"tasks": len(t.Tasks),
	}).Info("Confirm transaction")

	// Pre-Confirm
	for _, task := range t.Tasks {

		t.WaitGroup.Add(1)

		go func(t *Transaction, task *Task) {

			defer t.WaitGroup.Done()

			err := task.Confirm(t)
			if err == nil {
				t.Confirmed++
				return
			}
		}(t, task)
	}

	t.WaitGroup.Wait()

	// Success
	if t.Confirmed == len(t.Tasks) {
		t.Done()
		return
	}

	// TODO: retry
}

func (t *Transaction) Cancel() {

	log.WithFields(log.Fields{
		"id":    t.ID,
		"tasks": len(t.Tasks),
	}).Info("Cancel transaction")

	for _, task := range t.Tasks {
		t.WaitGroup.Add(1)

		go func(t *Transaction, task *Task) {

			defer t.WaitGroup.Done()

			task.Cancel(t)
		}(t, task)
	}

	t.WaitGroup.Wait()

	t.Done()
}

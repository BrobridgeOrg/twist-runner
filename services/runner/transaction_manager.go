package runner

import (
	"strconv"
	"time"
	app "twist-runner/app/interface"
	pb "twist-runner/pb"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	"github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type TransactionManager struct {
	App          app.AppImpl
	transactions map[string]*Transaction
	incoming     stan.Subscription
}

func CreeateTransactionManager(a app.AppImpl) *TransactionManager {

	return &TransactionManager{
		App:          a,
		transactions: make(map[string]*Transaction),
	}
}

func (tr *TransactionManager) Init() error {

	log.WithFields(log.Fields{
		"queueName": "twist.transactions",
	}).Info("Listening to queue")

	// Getting application ID
	idStr := strconv.FormatUint(tr.App.GetID(), 16)

	eb := tr.App.GetEventBus()
	incoming, err := eb.QueueSubscribe("twist.transactions", idStr, func(msg *stan.Msg) {

		log.Info("Received a transaction")

		var request pb.TransactionRequest
		err := proto.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Warn(err)
			return
		}

		err = tr.HandleTransaction(&request)
		if err != nil {
			log.Error(err)
			return
		}

		msg.Ack()
	})

	if err != nil {
		return err
	}

	tr.incoming = incoming

	return nil
}

func (tr *TransactionManager) HandleTransaction(request *pb.TransactionRequest) error {

	// Create a new transaction handler
	transaction := CreateTransaction(request.TransactionID, tr.App)

	// Register
	tr.transactions[request.TransactionID] = transaction

	// Set up a connection to supervisor.
	address := viper.GetString("supervisor.host")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Error("did not connect: ", err)
		return err
	}
	defer conn.Close()

	// Preparing context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Getting application ID
	idStr := strconv.FormatUint(tr.App.GetID(), 16)

	req := &pb.UpdateAssignmentRequest{
		RunnerID:      idStr,
		TransactionID: request.TransactionID,
	}

	// Notify supervisor that we're handling this transaction now
	_, err = pb.NewSupervisorClient(conn).UpdateAssignment(ctx, req)
	if err != nil {
		return err
	}

	// Start this transaction handler, it expires in 30 seconds
	go func() {
		defer delete(tr.transactions, request.TransactionID)

		transaction.Start(30)
	}()

	return nil
}

package runner

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Task struct {
	Status        string
	Completed     bool
	ConfirmAction *Action
	CancelAction  *Action
	Transport     *http.Transport
}

type Action struct {
	Type    string
	Method  string
	Uri     string
	Headers map[string]string
	Payload string
}

type ConfirmRequest struct {
	TransactionID string `json:"transactionID"`
	Phase         string `json:"phase"`
	Payload       string `json:"payload"`
}

type CancelRequest struct {
	TransactionID string `json:"transactionID"`
	Payload       string `json:"payload"`
}

func CreateTask() *Task {
	return &Task{
		Status:    "Ready",
		Completed: false,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
}

func (task *Task) Run(transaction *Transaction, action *Action, payload []byte) error {

	// Create a request
	request, err := http.NewRequest(strings.ToUpper(action.Method), action.Uri, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	// Preparing header
	request.Header.Add("Twist-Transaction-ID", transaction.ID)
	request.Header.Add("Content-Type", "application/json")
	for key, value := range action.Headers {
		request.Header.Add(key, value)
	}

	client := http.Client{
		Transport: task.Transport,
	}
	resp, err := client.Do(request)
	if err != nil {
		return err
	}

	// Require body
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// TODO: save body for logging
	log.Info("success")

	return nil
}

func (task *Task) PreConfirm(transaction *Transaction) error {

	log.WithFields(log.Fields{
		"method": task.ConfirmAction.Method,
		"url":    task.ConfirmAction.Uri,
	}).Info("Pre-Confirm task")

	// Prepare body
	body := &ConfirmRequest{
		TransactionID: transaction.ID,
		Phase:         "pre-confirm",
		Payload:       task.ConfirmAction.Payload,
	}

	payload, err := json.Marshal(body)
	if err != nil {
		task.Status = "Failed"
		log.Error(err)
		return err
	}

	// Connect to service to do pre-confirm
	task.Status = "Pre-Confirming"
	err = task.Run(transaction, task.ConfirmAction, payload)
	if err != nil {
		task.Status = "Failed"
		log.Error(err)
		return err
	}

	task.Status = "Success"

	return nil
}

func (task *Task) Confirm(transaction *Transaction) error {

	log.WithFields(log.Fields{
		"method": task.ConfirmAction.Method,
		"url":    task.ConfirmAction.Uri,
	}).Info("Confirm task")

	// Prepare body
	body := &ConfirmRequest{
		TransactionID: transaction.ID,
		Phase:         "confirm",
		Payload:       task.ConfirmAction.Payload,
	}

	payload, err := json.Marshal(body)
	if err != nil {
		task.Status = "Failed"
		log.Error(err)
		return err
	}

	// Connect to service to do confirm
	task.Status = "Confirming"
	err = task.Run(transaction, task.ConfirmAction, payload)
	if err != nil {
		task.Status = "Failed"
		log.Error(err)
		return err
	}

	task.Status = "Success"

	return nil
}

func (task *Task) Cancel(transaction *Transaction) error {

	log.WithFields(log.Fields{
		"method": task.CancelAction.Method,
		"url":    task.CancelAction.Uri,
	}).Info("Cancel task")

	// Prepare body
	body := &CancelRequest{
		TransactionID: transaction.ID,
		Payload:       task.ConfirmAction.Payload,
	}

	payload, err := json.Marshal(body)
	if err != nil {
		task.Status = "Failed"
		log.Error(err)
		return err
	}

	// Connect to service to do cancel
	task.Status = "Canceling"
	err = task.Run(transaction, task.CancelAction, payload)
	if err != nil {
		log.Error(err)
		return err
	}

	task.Status = "Canceled"

	return nil
}

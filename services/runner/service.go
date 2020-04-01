package runner

import (
	app "twist-runner/app/interface"

	log "github.com/sirupsen/logrus"
)

type Service struct {
	app                app.AppImpl
	transactionManager *TransactionManager
}

func CreateService(a app.AppImpl) *Service {

	transactionManager := CreeateTransactionManager(a)
	err := transactionManager.Init()
	if err != nil {
		log.Error(err)
		return nil
	}

	// Preparing service
	service := &Service{
		app:                a,
		transactionManager: transactionManager,
	}

	return service
}

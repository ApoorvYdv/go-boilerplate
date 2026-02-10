package service

import (
	"github.com/ApoorvYdv/go-boilerplate/internal/lib/job"
	"github.com/ApoorvYdv/go-boilerplate/internal/repository"
	"github.com/ApoorvYdv/go-boilerplate/internal/server"
)

type Services struct {
	Auth *AuthService
	Job  *job.JobService
}

func NewServices(s *server.Server, repos *repository.Repositories) (*Services, error) {
	authService := NewAuthService(s)

	return &Services{
		Auth: authService,
		Job:  s.Job,
	}, nil
}

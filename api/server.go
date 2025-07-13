package api

import (
	"os"

	"github.com/gin-gonic/gin"
)

func Start(store Store) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "6090"
	}

	handler := NewHandler(store)
	server := gin.Default()
	gin.SetMode(gin.ReleaseMode)

	Routes(server, handler)
	server.Run(":" + port)
}

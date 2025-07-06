package api

import "github.com/gin-gonic/gin"

func Routes(router *gin.Engine, handler *Handler) {
	routes := router.Group("/")
	{
		routes.GET("", handler.Get)
		routes.POST("", handler.Set)
	}
}

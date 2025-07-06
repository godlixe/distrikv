package api

import (
	"distrikv/storage"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Store interface {
	Get(key string) (*storage.KVData, error)
	Set(key string, value string)
}

type Handler struct {
	store Store
}

func NewHandler(store Store) *Handler {
	return &Handler{
		store: store,
	}
}

func (h *Handler) Get(ctx *gin.Context) {
	key := ctx.Query("key")

	res, err := h.store.Get(key)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, err)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

func (h *Handler) Set(ctx *gin.Context) {
	key := ctx.Query("key")
	value := ctx.Query("value")

	h.store.Set(key, value)

	ctx.JSON(http.StatusOK, "success")
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"myproject/api"

	openapi_types "github.com/oapi-codegen/runtime/types"
)

func writeError(w http.ResponseWriter, code int, err api.ErrorResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(err)
}

type validationViolation struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

func validateCreate(body api.ProductCreate) []validationViolation {
	var res []validationViolation
	if len(body.Name) < 1 || len(body.Name) > 255 {
		res = append(res, validationViolation{"name", "minLength: 1, maxLength: 255"})
	}

	if body.Description != nil && len(*body.Description) > 4000 {
		res = append(res, validationViolation{"description", "maxLength: 4000"})
	}

	if body.Price < 0.01 {
		res = append(res, validationViolation{"price", "minimum: 0.01"})
	}

	if body.Stock < 0 {
		res = append(res, validationViolation{"stock", "minimum: 0"})
	}

	if len(body.Category) < 1 || len(body.Category) > 100 {
		res = append(res, validationViolation{"category", "minLength: 1, maxLength: 100"})
	}

	if !body.Status.Valid() {
		res = append(res, validationViolation{"status", "enum: ACTIVE, INACTIVE, ARCHIVED"})
	}

	return res
}

func validateUpdate(body api.ProductUpdate) []validationViolation {
	var res []validationViolation
	if body.Name != nil && (len(*body.Name) < 1 || len(*body.Name) > 255) {
		res = append(res, validationViolation{"name", "minLength: 1, maxLength: 255"})
	}

	if body.Description != nil && len(*body.Description) > 4000 {
		res = append(res, validationViolation{"description", "maxLength: 4000"})
	}

	if body.Price != nil && *body.Price < 0.01 {
		res = append(res, validationViolation{"price", "minimum: 0.01"})
	}

	if body.Stock != nil && *body.Stock < 0 {
		res = append(res, validationViolation{"stock", "minimum: 0"})
	}

	if body.Category != nil && (len(*body.Category) < 1 || len(*body.Category) > 100) {
		res = append(res, validationViolation{"category", "minLength: 1, maxLength: 100"})
	}

	if body.Status != nil && !body.Status.Valid() {
		res = append(res, validationViolation{"status", "enum: ACTIVE, INACTIVE, ARCHIVED"})
	}

	return res
}

func setProductSellerID(p *api.ProductResponse, sellerIDStr *string) {
	if sellerIDStr == nil || *sellerIDStr == "" {
		return
	}
	
	var u openapi_types.UUID
	if err := u.UnmarshalText([]byte(*sellerIDStr)); err == nil {
		p.SellerId = &u
	}
}

func (s *Server) GetProducts(w http.ResponseWriter, r *http.Request, params api.GetProductsParams) {
	page := 0
	if params.Page != nil {
		page = *params.Page
	}

	size := 20
	if params.Size != nil {
		size = *params.Size
	}

	if size < 1 {
		size = 20
	}

	offset := page * size

	where := "WHERE 1=1"
	args := []interface{}{}
	n := 1

	if params.Status != nil {
		where += fmt.Sprintf(" AND status = $%d", n)
		args = append(args, string(*params.Status))

		n++
	}

	if params.Category != nil && *params.Category != "" {
		where += fmt.Sprintf(" AND category = $%d", n)
		args = append(args, *params.Category)

		n++
	}

	var total int

	err := s.pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM products "+where, args...).Scan(&total)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	args = append(args, size, offset)
	listQuery := fmt.Sprintf(`SELECT id::text, name, description, price, stock, category, status, created_at, updated_at, seller_id::text FROM products %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, where, n, n+1)

	rows, err := s.pool.Query(context.Background(), listQuery, args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer rows.Close()

	var list []api.ProductResponse
	
	for rows.Next() {
		var p api.ProductResponse
		var idStr string
		var sellerIDStr *string

		if err := rows.Scan(&idStr, &p.Name, &p.Description, &p.Price, &p.Stock, &p.Category, &p.Status, &p.CreatedAt, &p.UpdatedAt, &sellerIDStr); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_ = p.Id.UnmarshalText([]byte(idStr))
		setProductSellerID(&p, sellerIDStr)
		list = append(list, p)
	}

	resp := api.ProductListResponse{
		ProductList:   list,
		TotalElements: total,
		Page:          page,
		PageSize:      size,
	}

	w.Header().Set("Content-Type", "application/json")


	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) PostProducts(w http.ResponseWriter, r *http.Request) {
	role := getRoleFromContext(r.Context())
	if role == "USER" {
		writeError(w, http.StatusForbidden, api.ErrorResponse{
			ErrorCode: api.ACCESSDENIED,
			Message:   "Недостаточно прав",
		})

		return
	}
	
	userID := getUserIDFromContext(r.Context())
	if userID == nil || *userID == "" {
		writeError(w, http.StatusForbidden, api.ErrorResponse{
			ErrorCode: api.ACCESSDENIED,
			Message:   "Недостаточно прав",
		})
		
		return
	}

	var body api.ProductCreate
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, api.ErrorResponse{
			ErrorCode: api.VALIDATIONERROR,
			Message:   "Ошибка валидации входных данных",
			Details:   &map[string]interface{}{"reason": err.Error()},
		})

		return
	}

	if v := validateCreate(body); len(v) > 0 {
		details := map[string]interface{}{"violations": v}
		writeError(w, http.StatusBadRequest, api.ErrorResponse{
			ErrorCode: api.VALIDATIONERROR,
			Message:   "Ошибка валидации входных данных",
			Details:   &details,
		})

		return
	}

	var idStr string
	err := s.pool.QueryRow(r.Context(),
		`INSERT INTO products (name, description, price, stock, category, status, seller_id) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id::text`,
		body.Name, body.Description, body.Price, body.Stock, body.Category, body.Status, *userID,
	).Scan(&idStr)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var sellerIDStr *string
	row := s.pool.QueryRow(r.Context(),
		`SELECT id::text, name, description, price, stock, category, status, created_at, updated_at, seller_id::text FROM products WHERE id::text = $1`, idStr)
	var p api.ProductResponse

	if err := row.Scan(&idStr, &p.Name, &p.Description, &p.Price, &p.Stock, &p.Category, &p.Status, &p.CreatedAt, &p.UpdatedAt, &sellerIDStr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	_ = p.Id.UnmarshalText([]byte(idStr))
	setProductSellerID(&p, sellerIDStr)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(p)
}

func (s *Server) GetProductsId(w http.ResponseWriter, r *http.Request, id openapi_types.UUID) {
	idStr := id.String()
	var sellerIDStr *string
	row := s.pool.QueryRow(r.Context(),
		`SELECT id::text, name, description, price, stock, category, status, created_at, updated_at, seller_id::text FROM products WHERE id = $1`, idStr)
	var p api.ProductResponse

	err := row.Scan(&idStr, &p.Name, &p.Description, &p.Price, &p.Stock, &p.Category, &p.Status, &p.CreatedAt, &p.UpdatedAt, &sellerIDStr)
	if err != nil {
		writeError(w, http.StatusNotFound, api.ErrorResponse{
			ErrorCode: api.PRODUCTNOTFOUND,
			Message:   "Товар не найден",
		})

		return
	}

	_ = p.Id.UnmarshalText([]byte(idStr))
	setProductSellerID(&p, sellerIDStr)
	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(p)
}

func (s *Server) PutProductsId(w http.ResponseWriter, r *http.Request, id openapi_types.UUID) {
	role := getRoleFromContext(r.Context())
	if role == "USER" {
		writeError(w, http.StatusForbidden, api.ErrorResponse{
			ErrorCode: api.ACCESSDENIED,
			Message:   "Недостаточно прав",
		})

		return
	}

	userID := getUserIDFromContext(r.Context())
	if userID == nil {
		writeError(w, http.StatusForbidden, api.ErrorResponse{
			ErrorCode: api.ACCESSDENIED,
			Message:   "Недостаточно прав",
		})
		
		return
	}

	var body api.ProductUpdate
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, api.ErrorResponse{
			ErrorCode: api.VALIDATIONERROR,
			Message:   "Ошибка валидации входных данных",
			Details:   &map[string]interface{}{"reason": err.Error()},
		})

		return
	}

	if v := validateUpdate(body); len(v) > 0 {
		details := map[string]interface{}{"violations": v}
		writeError(w, http.StatusBadRequest, api.ErrorResponse{
			ErrorCode: api.VALIDATIONERROR,
			Message:   "Ошибка валидации входных данных",
			Details:   &details,
		})

		return
	}

	idStr := id.String()
	var sellerIDStr *string
	err := s.pool.QueryRow(r.Context(), `SELECT seller_id::text FROM products WHERE id = $1`, idStr).Scan(&sellerIDStr)
	if err != nil {
		writeError(w, http.StatusNotFound, api.ErrorResponse{
			ErrorCode: api.PRODUCTNOTFOUND,
			Message:   "Товар не найден",
		})

		return
	}

	if role == "SELLER" && (sellerIDStr == nil || *sellerIDStr != *userID) {
		writeError(w, http.StatusForbidden, api.ErrorResponse{
			ErrorCode: api.ACCESSDENIED,
			Message:   "Недостаточно прав",
		})

		return
	}

	row := s.pool.QueryRow(r.Context(),
		`UPDATE products SET
			name = COALESCE($2, name),
			description = COALESCE($3, description),
			price = COALESCE($4, price),
			stock = COALESCE($5, stock),
			category = COALESCE($6, category),
			status = COALESCE($7, status)
		 WHERE id = $1 RETURNING id::text, name, description, price, stock, category, status, created_at, updated_at, seller_id::text`,
		idStr, body.Name, body.Description, body.Price, body.Stock, body.Category, body.Status,
	)

	var p api.ProductResponse
	err = row.Scan(&idStr, &p.Name, &p.Description, &p.Price, &p.Stock, &p.Category, &p.Status, &p.CreatedAt, &p.UpdatedAt, &sellerIDStr)
	if err != nil {
		writeError(w, http.StatusNotFound, api.ErrorResponse{
			ErrorCode: api.PRODUCTNOTFOUND,
			Message:   "Товар не найден",
		})

		return
	}

	_ = p.Id.UnmarshalText([]byte(idStr))
	setProductSellerID(&p, sellerIDStr)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p)
}

func (s *Server) DeleteProductsId(w http.ResponseWriter, r *http.Request, id openapi_types.UUID) {
	role := getRoleFromContext(r.Context())
	if role == "USER" {
		writeError(w, http.StatusForbidden, api.ErrorResponse{
			ErrorCode: api.ACCESSDENIED,
			Message:   "Недостаточно прав",
		})
		return
	}
	userID := getUserIDFromContext(r.Context())
	if userID == nil {
		writeError(w, http.StatusForbidden, api.ErrorResponse{
			ErrorCode: api.ACCESSDENIED,
			Message:   "Недостаточно прав",
		})
		return
	}

	var sellerIDStr *string
	err := s.pool.QueryRow(r.Context(), `SELECT seller_id::text FROM products WHERE id = $1`, id.String()).Scan(&sellerIDStr)
	if err != nil {
		writeError(w, http.StatusNotFound, api.ErrorResponse{
			ErrorCode: api.PRODUCTNOTFOUND,
			Message:   "Товар не найден",
		})

		return
	}

	if role == "SELLER" && (sellerIDStr == nil || *sellerIDStr != *userID) {
		writeError(w, http.StatusForbidden, api.ErrorResponse{
			ErrorCode: api.ACCESSDENIED,
			Message:   "Недостаточно прав",
		})

		return
	}

	cmd, err := s.pool.Exec(r.Context(), `UPDATE products SET status = 'ARCHIVED' WHERE id = $1`, id.String())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	if cmd.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, api.ErrorResponse{
			ErrorCode: api.PRODUCTNOTFOUND,
			Message:   "Товар не найден",
		})

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

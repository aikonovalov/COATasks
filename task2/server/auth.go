package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"myproject/api"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type contextKey string

const (
	userIDContextKey contextKey = "user_id"
	roleContextKey   contextKey = "role"
)

func getUserIDFromContext(ctx context.Context) *string {
	v := ctx.Value(userIDContextKey)
	if v == nil {
		return nil
	}

	s, _ := v.(string)

	if s == "" {
		return nil
	}

	return &s
}

func getRoleFromContext(ctx context.Context) string {
	v := ctx.Value(roleContextKey)
	if v == nil {
		return ""
	}

	s, _ := v.(string)

	return s
}

const (
	accessTokenTTL  = time.Minute * 20
	refreshTokenTTL = time.Hour * 7 * 24
)

type authClaims struct {
	jwt.RegisteredClaims
	UserID string `json:"user_id"`
	Role   string `json:"role"`
	IsRefresh bool `json:"is_refresh,omitempty"`
}

func getJWTSecret() []byte {
	secret := "GOOOOOOOOOOOOOL GOOOOOOOOL!!!!!!!!!!!"
	if s := strings.TrimSpace(getEnv("JWT_SECRET", "")); s != "" {
		return []byte(s)
	}

	return []byte(secret)
}

func getEnv(key, def string) string {
	if v := envGetter(key); v != "" {
		return v
	}

	return def
}

var envGetter = func(key string) string { return "" }

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" || !strings.HasPrefix(auth, "Bearer ") {
			writeError(w, http.StatusUnauthorized, api.ErrorResponse{
				ErrorCode: api.TOKENINVALID,
				Message:   "Невалидный access token",
			})

			return
		}

		tokenStr := strings.TrimPrefix(auth, "Bearer ")
		token, err := jwt.ParseWithClaims(tokenStr, &authClaims{}, func(t *jwt.Token) (interface{}, error) {
			return getJWTSecret(), nil
		})

		if err != nil {
			if errors.Is(err, jwt.ErrTokenExpired) {
				writeError(w, http.StatusUnauthorized, api.ErrorResponse{
					ErrorCode: api.TOKENEXPIRED,
					Message:   "Access token испортился",
				})

				return
			}

			writeError(w, http.StatusUnauthorized, api.ErrorResponse{
				ErrorCode: api.TOKENINVALID,
				Message:   "Невалидный access token",
			})

			return
		}

		claims, ok := token.Claims.(*authClaims)
		if !ok || !token.Valid || claims.IsRefresh {
			writeError(w, http.StatusUnauthorized, api.ErrorResponse{
				ErrorCode: api.TOKENINVALID,
				Message:   "Невалидный access token",
			})

			return
		}

		ctx := context.WithValue(r.Context(), userIDContextKey, claims.UserID)
		ctx = context.WithValue(ctx, roleContextKey, claims.Role)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Login    string `json:"login"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, api.ErrorResponse{
			ErrorCode: api.VALIDATIONERROR,
			Message:   "Ошибка валидации",
			Details:   &map[string]interface{}{"reason": err.Error()},
		})

		return
	}

	if body.Login == "" || len(body.Password) < 6 {
		writeError(w, http.StatusBadRequest, api.ErrorResponse{
			ErrorCode: api.VALIDATIONERROR,
			Message:   "логин обязателен, пароль не меньше 6 символов",
		})

		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(body.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	_, err = s.pool.Exec(r.Context(), `INSERT INTO users (login, password_hash, role) VALUES ($1, $2, 'USER')`, body.Login, string(hash))
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			writeError(w, http.StatusConflict, api.ErrorResponse{
				ErrorCode: api.VALIDATIONERROR,
				Message:   "Пользователь с таким логином существует",
			})

			return
		}

		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]string{"message": "Пользователь зарегистрирован"})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Login    string `json:"login"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, api.ErrorResponse{
			ErrorCode: api.VALIDATIONERROR,
			Message:   "Ошибка валидации",
			Details:   &map[string]interface{}{"reason": err.Error()},
		})

		return
	}

	var id, hash string
	err := s.pool.QueryRow(r.Context(), `SELECT id::text, password_hash FROM users WHERE login = $1`, body.Login).Scan(&id, &hash)
	if err != nil {
		writeError(w, http.StatusUnauthorized, api.ErrorResponse{
			ErrorCode: api.TOKENINVALID,
			Message:   "Неверный логин или пароль",
		})

		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(body.Password)); err != nil {
		writeError(w, http.StatusUnauthorized, api.ErrorResponse{
			ErrorCode: api.TOKENINVALID,
			Message:   "Неверный логин или пароль",
		})

		return
	}

	var role string
	_ = s.pool.QueryRow(r.Context(), `SELECT role FROM users WHERE id::text = $1`, id).Scan(&role)
	if role == "" {
		role = "USER"
	}

	access, _ := issueToken(id, role, false, accessTokenTTL)
	refresh, _ := issueToken(id, role, true, refreshTokenTTL)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"access_token":  access,
		"refresh_token": refresh,
	})
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RefreshToken string `json:"refresh_token"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RefreshToken == "" {
		writeError(w, http.StatusUnauthorized, api.ErrorResponse{
			ErrorCode: api.REFRESHTOKENINVALID,
			Message:   "Невалидный refresh token",
		})

		return
	}

	token, err := jwt.ParseWithClaims(body.RefreshToken, &authClaims{}, func(t *jwt.Token) (interface{}, error) {
		return getJWTSecret(), nil
	})

	if err != nil {
		writeError(w, http.StatusUnauthorized, api.ErrorResponse{
			ErrorCode: api.REFRESHTOKENINVALID,
			Message:   "Невалидный refresh token",
		})

		return
	}

	claims, ok := token.Claims.(*authClaims)
	if !ok || !token.Valid || !claims.IsRefresh {
		writeError(w, http.StatusUnauthorized, api.ErrorResponse{
			ErrorCode: api.REFRESHTOKENINVALID,
			Message:   "Невалидный refresh token",
		})

		return
	}

	access, _ := issueToken(claims.UserID, claims.Role, false, accessTokenTTL)

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]string{"access_token": access})
}

func issueToken(userID, role string, isRefresh bool, ttl time.Duration) (string, error) {
	claims := authClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(ttl)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		UserID:    userID,
		Role:      role,
		IsRefresh: isRefresh,
	}

	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)



	return t.SignedString(getJWTSecret())
}

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
)

var (
	publicDir string
	fs        http.Handler
	cacheMap  = make(map[string]*User)
)

type User struct {
	ID        string    `db:"id" json:"id"`
	Email     string    `db:"email" json:"email"`
	Nickname  string    `db:"nickname" json:"nickname"`
	Staff     bool      `db:"staff" json:"staff"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

type Schedule struct {
	ID           string         `db:"id" json:"id"`
	Title        string         `db:"title" json:"title"`
	Capacity     int            `db:"capacity" json:"capacity"`
	Reserved     int            `db:"reserved" json:"reserved"`
	Reservations []*Reservation `db:"reservations" json:"reservations"`
	CreatedAt    time.Time      `db:"created_at" json:"created_at"`
}

type Reservation struct {
	ID         string    `db:"id" json:"id"`
	ScheduleID string    `db:"schedule_id" json:"schedule_id"`
	UserID     string    `db:"user_id" json:"user_id"`
	User       *User     `db:"user" json:"user"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
}

type UserReservation struct {
	ID            string    `db:"id"`
	ScheduleID    string    `db:"schedule_id"`
	UserID        string    `db:"user_id"`
	CreatedAt     time.Time `db:"created_at"`
	UserEmail     string    `db:"user_email"`
	UserNickname  string    `db:"user_nickname"`
	UserStaff     bool      `db:"user_staff"`
	UserCreatedAt time.Time `db:"user_created_at"`
}

func getUserFromRedis(key string) *User {
	user := &User{}
	user, ok := cacheMap[key]
	if ok {
		return user
	}
	r := rdb.Get(rctx, key)
	if r.Err() == redis.Nil {
		return nil
	}
	juser, err := r.Bytes()
	if err != nil {
		return nil
	}
	if err := json.Unmarshal(juser, user); err != nil {
		return nil
	}
	cacheMap[key] = user
	return user
}

func getCurrentUser(r *http.Request) *User {
	uidCookie, err := r.Cookie("user_id")
	if err != nil || uidCookie == nil {
		return nil
	}
	return getUserFromRedis(uidCookie.Value)
}

func requiredLogin(w http.ResponseWriter, r *http.Request) bool {
	if getCurrentUser(r) != nil {
		return true
	}
	sendErrorJSON(w, fmt.Errorf("login required"), 401)
	return false
}

func requiredStaffLogin(w http.ResponseWriter, r *http.Request) bool {
	if getCurrentUser(r) != nil && getCurrentUser(r).Staff {
		return true
	}
	sendErrorJSON(w, fmt.Errorf("login required"), 401)
	return false
}

func getReservations(r *http.Request, s *Schedule) error {
	var rows *sqlx.Rows
	var err error
	if getCurrentUser(r) != nil && !getCurrentUser(r).Staff {
		rows, err = db.QueryxContext(r.Context(), "SELECT id, schedule_id, user_id, created_at, user_nickname, user_staff, user_created_at FROM `reservations` WHERE `schedule_id` = ?", s.ID)
		if err != nil {
			return err
		}
	} else {
		rows, err = db.QueryxContext(r.Context(), "SELECT id, schedule_id, user_id, created_at, user_email, user_nickname, user_staff, user_created_at FROM `reservations` WHERE `schedule_id` = ?", s.ID)
		if err != nil {
			return err
		}
	}

	defer rows.Close()

	s.Reservations = []*Reservation{}
	for rows.Next() {
		ureservation := &UserReservation{}
		if err := rows.StructScan(ureservation); err != nil {
			return err
		}

		reservation := &Reservation{
			ID:         ureservation.ID,
			ScheduleID: ureservation.ScheduleID,
			UserID:     ureservation.UserID,
			User: &User{
				ID:        ureservation.UserID,
				Email:     ureservation.UserEmail,
				Nickname:  ureservation.UserNickname,
				Staff:     ureservation.UserStaff,
				CreatedAt: ureservation.UserCreatedAt,
			},
			CreatedAt: ureservation.CreatedAt,
		}

		s.Reservations = append(s.Reservations, reservation)
	}

	return nil
}

func getReservationsCount(r *http.Request, s *Schedule) error {
	rows, err := db.QueryxContext(r.Context(), "SELECT * FROM `reservations` WHERE `schedule_id` = ?", s.ID)
	if err != nil {
		return err
	}

	defer rows.Close()

	reserved := 0
	for rows.Next() {
		reserved++
	}
	s.Reserved = reserved

	return nil
}

func getUser(r *http.Request, id string) *User {
	user := getUserFromRedis(id)
	if user == nil {
		return nil
	}
	if getCurrentUser(r) != nil && !getCurrentUser(r).Staff {
		user.Email = ""
	}
	return user
}

func parseForm(r *http.Request) error {
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		return r.ParseForm()
	} else {
		return r.ParseMultipartForm(32 << 20)
	}
}

func serveMux() http.Handler {
	router := mux.NewRouter()

	router.HandleFunc("/initialize", initializeHandler).Methods("POST")
	router.HandleFunc("/api/session", sessionHandler).Methods("GET")
	router.HandleFunc("/api/signup", signupHandler).Methods("POST")
	router.HandleFunc("/api/login", loginHandler).Methods("POST")
	router.HandleFunc("/api/schedules", createScheduleHandler).Methods("POST")
	router.HandleFunc("/api/reservations", createReservationHandler).Methods("POST")
	router.HandleFunc("/api/schedules", schedulesHandler).Methods("GET")
	router.HandleFunc("/api/schedules/{id}", scheduleHandler).Methods("GET")

	dir, err := filepath.Abs(filepath.Join(filepath.Dir(os.Args[0]), "..", "public"))
	if err != nil {
		log.Fatal(err)
	}
	rdb.FlushAll(rctx)
	publicDir = dir
	fs = http.FileServer(http.Dir(publicDir))

	router.PathPrefix("/").HandlerFunc(htmlHandler)

	// return logger(router)
	return nonLogger(router)
}

func nonLogger(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
	})
}

func logger(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		before := time.Now()
		handler.ServeHTTP(w, r)
		after := time.Now()
		duration := after.Sub(before)
		log.Printf("%s % 4s %s (%s)", r.RemoteAddr, r.Method, r.URL.Path, duration)
	})
}

func sendJSON(w http.ResponseWriter, data interface{}, statusCode int) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	enc := json.NewEncoder(w)
	return enc.Encode(data)
}

func sendErrorJSON(w http.ResponseWriter, err error, statusCode int) error {
	log.Printf("ERROR: %+v", err)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	enc := json.NewEncoder(w)
	return enc.Encode(map[string]string{"error": err.Error()})
}

type initializeResponse struct {
	Language string `json:"language"`
}

func initializeHandler(w http.ResponseWriter, r *http.Request) {
	err := transaction(r.Context(), &sql.TxOptions{}, func(ctx context.Context, tx *sqlx.Tx) error {
		if _, err := tx.ExecContext(ctx, "TRUNCATE `reservations`"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "TRUNCATE `schedules`"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "TRUNCATE `users`"); err != nil {
			return err
		}
		user := &User{
			ID:       generateID(tx, "users"),
			Email:    "isucon2021_prior@isucon.net",
			Nickname: "isucon",
			Staff:    true,
		}
		buser, err := json.Marshal(user)

		if err != nil {
			sendErrorJSON(w, err, 500)
			return err
		}

		rdb.Set(rctx, user.ID, buser, 0)
		// user?
		rdb.Set(rctx, user.Email, buser, 0)
		cacheMap[user.ID] = user
		cacheMap[user.Email] = user

		return nil
	})
	if err != nil {
		sendErrorJSON(w, err, 500)
	} else {
		sendJSON(w, initializeResponse{Language: "golang"}, 200)
	}
}

func sessionHandler(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, getCurrentUser(r), 200)
}

func signupHandler(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(r); err != nil {
		sendErrorJSON(w, err, 500)
		return
	}

	user := &User{}
	email := r.FormValue("email")
	nickname := r.FormValue("nickname")
	id := generateID(nil, "users")
	createdAt := time.Now()

	user.ID = id
	user.Email = email
	user.Nickname = nickname
	user.CreatedAt = createdAt
	buser, err := json.Marshal(user)
	if err != nil {
		sendErrorJSON(w, err, 500)
		return
	}

	rdb.Set(rctx, user.ID, buser, 0)
	// user?
	rdb.Set(rctx, user.Email, buser, 0)

	cacheMap[user.ID] = user
	cacheMap[user.Email] = user

	if err != nil {
		sendErrorJSON(w, err, 500)
	} else {
		sendJSON(w, user, 200)
	}
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(r); err != nil {
		sendErrorJSON(w, err, 500)
		return
	}

	email := r.PostFormValue("email")
	user := getUserFromRedis(email)
	if user == nil {
		sendErrorJSON(w, errors.New("user not found"), 403)
		return
	}

	// buser, err := json.Marshal(user)
	// if err != nil {
	// 	sendErrorJSON(w, err, 403)
	// 	return
	// }

	// rdb.Set(rctx, user.ID, buser, 0)
	cookie := &http.Cookie{
		Name:     "user_id",
		Value:    user.ID,
		Path:     "/",
		MaxAge:   86400,
		HttpOnly: true,
	}
	http.SetCookie(w, cookie)

	sendJSON(w, user, 200)
}

func createScheduleHandler(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(r); err != nil {
		sendErrorJSON(w, err, 500)
		return
	}

	if !requiredStaffLogin(w, r) {
		return
	}

	now := time.Now()

	schedule := &Schedule{}
	err := transaction(r.Context(), &sql.TxOptions{}, func(ctx context.Context, tx *sqlx.Tx) error {
		id := generateID(tx, "schedules")
		title := r.PostFormValue("title")
		capacity, _ := strconv.Atoi(r.PostFormValue("capacity"))

		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO `schedules` (`id`, `title`, `capacity`, `created_at`) VALUES (?, ?, ?, ?)",
			id, title, capacity, now.Format("2006-01-02 15:04:05.000"),
		); err != nil {
			return err
		}
		schedule.ID = id
		schedule.Title = title
		schedule.Capacity = capacity
		schedule.CreatedAt = now

		return nil
	})

	if err != nil {
		sendErrorJSON(w, err, 500)
	} else {
		sendJSON(w, schedule, 200)
	}
}

func createReservationHandler(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(r); err != nil {
		sendErrorJSON(w, err, 500)
		return
	}

	if !requiredLogin(w, r) {
		return
	}

	scheduleID := r.PostFormValue("schedule_id")
	userID := getCurrentUser(r).ID
	user := &User{}
	now := time.Now()
	schedule := &Schedule{}
	reservation := &Reservation{}

	res := rdb.Get(rctx, userID)
	if res.Err() == redis.Nil {
		sendErrorJSON(w, fmt.Errorf("user not found"), 403)
		return
	}
	buser, err := res.Bytes()
	if err != nil {
		sendErrorJSON(w, fmt.Errorf("user not found"), 403)
		return
	}
	err = json.Unmarshal(buser, user)
	if err != nil {
		sendErrorJSON(w, fmt.Errorf("user not found"), 403)
		return
	}

	found := 0
	ctx := r.Context()
	db.QueryRowContext(ctx, "SELECT 1 FROM `reservations` WHERE `schedule_id` = ? AND `user_id` = ? LIMIT 1", scheduleID, userID).Scan(&found)
	if found == 1 {
		sendErrorJSON(w, fmt.Errorf("already taken"), 403)
		return
	}

	id := generateID(nil, "schedules")

	err = transaction(r.Context(), &sql.TxOptions{}, func(ctx context.Context, tx *sqlx.Tx) error {
		// join
		if err := tx.QueryRowxContext(ctx, "SELECT reserved, capacity FROM `schedules` WHERE `id` = ? LIMIT 1 FOR UPDATE ", scheduleID).StructScan(schedule); err != nil {
			return sendErrorJSON(w, fmt.Errorf("schedule not found"), 403)
		}

		if schedule.Reserved >= schedule.Capacity {
			return sendErrorJSON(w, fmt.Errorf("capacity is already full"), 403)
		}

		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO `reservations` (`id`, `schedule_id`, `user_id`, `created_at`, `user_email`, `user_nickname`, `user_staff`, `user_created_at`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			id, scheduleID, userID, now.Format("2006-01-02 15:04:05.000"), user.Email, user.Nickname, user.Staff, user.CreatedAt,
		); err != nil {
			return err
		}

		reserved := schedule.Reserved + 1

		if _, err := tx.ExecContext(
			ctx,
			"UPDATE `schedules` SET `reserved` = ? WHERE `id` = ?",
			reserved, scheduleID,
		); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		sendErrorJSON(w, err, 500)
		return
	}

	reservation.ID = id
	reservation.ScheduleID = scheduleID
	reservation.UserID = userID
	reservation.CreatedAt = now

	sendJSON(w, reservation, 200)
}

func schedulesHandler(w http.ResponseWriter, r *http.Request) {
	schedules := []*Schedule{}
	rows, err := db.QueryxContext(r.Context(), "SELECT * FROM `schedules` ORDER BY `id` DESC")
	if err != nil {
		sendErrorJSON(w, err, 500)
		return
	}

	for rows.Next() {
		schedule := &Schedule{}
		if err := rows.StructScan(schedule); err != nil {
			sendErrorJSON(w, err, 500)
			return
		}
		schedules = append(schedules, schedule)
	}

	sendJSON(w, schedules, 200)
}

func scheduleHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	// joins
	schedule := &Schedule{}
	if err := db.QueryRowxContext(r.Context(), "SELECT * FROM `schedules` WHERE `id` = ? LIMIT 1", id).StructScan(schedule); err != nil {

		sendErrorJSON(w, err, 500)
		return
	}

	if err := getReservations(r, schedule); err != nil {
		sendErrorJSON(w, err, 500)
		return
	}

	sendJSON(w, schedule, 200)
}

func htmlHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	realpath := filepath.Join(publicDir, path)

	if stat, err := os.Stat(realpath); !os.IsNotExist(err) && !stat.IsDir() {
		fs.ServeHTTP(w, r)
		return
	} else {
		realpath = filepath.Join(publicDir, "index.html")
	}

	file, err := os.Open(realpath)
	if err != nil {
		sendErrorJSON(w, err, 500)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", "text/html; chartset=utf-8")
	w.WriteHeader(200)
	io.Copy(w, file)
}

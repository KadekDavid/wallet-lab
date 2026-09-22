// A minimal wallet / ledger service.
//
// This is the shape of the service that sits at the centre of any platform where
// money moves: fintech, payments, or a gaming engine. The three things it exists
// to teach you are:
//
//  1. Idempotency  — the same request sent twice must move money once.
//  2. Atomicity    — balance and ledger change together or not at all.
//  3. Concurrency  — two simultaneous bets must not both pass the balance check.
//
// Everything else is detail.
package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

type Server struct{ db *sql.DB }

// ---------- domain errors ----------

var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrKeyReused         = errors.New("idempotency key reused with a different body")
)

// ---------- request / response types ----------

type txRequest struct {
	AccountID int64  `json:"account_id"`
	Amount    int64  `json:"amount"` // always positive; Kind decides the direction
	Kind      string `json:"kind"`   // deposit | bet | win
	Reference string `json:"reference"`
}

type txResponse struct {
	TransactionID int64 `json:"transaction_id"`
	Balance       int64 `json:"balance"`
	Replayed      bool  `json:"replayed"`
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	// Connection pool sizing matters once you are under load. Too many connections
	// is as bad as too few — Postgres forks a backend process per connection.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)

	if err := db.Ping(); err != nil {
		log.Fatalf("cannot reach postgres: %v", err)
	}

	s := &Server{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /accounts", s.createAccount)
	mux.HandleFunc("GET /accounts/{id}", s.getAccount)
	mux.HandleFunc("POST /transactions", s.postTransaction)

	log.Println("wallet listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

// ---------- handlers ----------

func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Owner string `json:"owner"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpError(w, http.StatusBadRequest, "bad json")
		return
	}
	var id int64
	err := s.db.QueryRow(
		`INSERT INTO accounts (owner, kind) VALUES ($1, 'player') RETURNING id`,
		body.Owner,
	).Scan(&id)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": id, "balance": 0})
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad id")
		return
	}
	var owner string
	var balance int64
	err = s.db.QueryRow(`SELECT owner, balance FROM accounts WHERE id = $1`, id).
		Scan(&owner, &balance)
	if errors.Is(err, sql.ErrNoRows) {
		httpError(w, http.StatusNotFound, "no such account")
		return
	}
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "owner": owner, "balance": balance})
}

// postTransaction is the only endpoint that matters. Read it slowly.
func (s *Server) postTransaction(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		httpError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var req txRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "bad json")
		return
	}
	if req.Amount <= 0 {
		httpError(w, http.StatusBadRequest, "amount must be positive")
		return
	}
	if req.Kind != "deposit" && req.Kind != "bet" && req.Kind != "win" {
		httpError(w, http.StatusBadRequest, "kind must be deposit, bet or win")
		return
	}

	resp, err := s.execute(key, req)
	switch {
	case errors.Is(err, ErrInsufficientFunds):
		httpError(w, http.StatusUnprocessableEntity, "insufficient funds")
	case errors.Is(err, ErrKeyReused):
		httpError(w, http.StatusConflict, err.Error())
	case err != nil:
		httpError(w, http.StatusInternalServerError, err.Error())
	default:
		writeJSON(w, http.StatusOK, resp)
	}
}

// execute runs the whole money movement inside ONE database transaction.
//
// The ordering here is deliberate:
//
//	BEGIN
//	  claim the idempotency key   <- unique index does the real work
//	  lock the account row        <- SELECT ... FOR UPDATE serialises concurrent bets
//	  check funds
//	  write transaction + entries
//	  update balance
//	  store the response
//	COMMIT
//,L
// If anything fails, the rollback takes the idempotency key with it, so the
// caller can safely retry.
func (s *Server) execute(key string, req txRequest) (*txResponse, error) {
	hash := hashRequest(req)

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

	// --- 1. claim the key -------------------------------------------------
	var existingHash string
	var existingBody []byte
	err = tx.QueryRow(
		`INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET key = idempotency_keys.key
		 RETURNING request_hash, response_body`,
		key, hash,
	).Scan(&existingHash, &existingBody)
	if err != nil {
		return nil, err
	}

	if existingHash != hash {
		// Same key, different payload. This is a caller bug and must never
		// silently succeed.
		return nil, ErrKeyReused
	}
	if existingBody != nil {
		// Genuine retry of a completed request: replay the stored response.
		var prev txResponse
		if err := json.Unmarshal(existingBody, &prev); err != nil {
			return nil, err
		}
		prev.Replayed = true
		return &prev, tx.Commit()
	}

	// --- 2. lock the account ---------------------------------------------
	// FOR UPDATE blocks any other transaction touching this row until we commit.
	// Without it, two concurrent bets can both read balance=100, both decide
	// they can afford 100, and both succeed. That is the classic double-spend.
	var balance int64
	err = tx.QueryRow(
		`SELECT balance FROM accounts WHERE id = $1 FOR UPDATE`, req.AccountID,
	).Scan(&balance)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("account %d not found", req.AccountID)
	}
	if err != nil {
		return nil, err
	}

	// --- 3. decide the direction -----------------------------------------
	delta := req.Amount // deposit and win credit the player
	if req.Kind == "bet" {
		delta = -req.Amount
	}
	if balance+delta < 0 {
		return nil, ErrInsufficientFunds
	}

	// --- 4. write the double-entry ---------------------------------------
	var txID int64
	if err := tx.QueryRow(
		`INSERT INTO transactions (kind, reference) VALUES ($1, $2) RETURNING id`,
		req.Kind, req.Reference,
	).Scan(&txID); err != nil {
		return nil, err
	}

	const houseAccount = 1
	// Two entries, equal and opposite. Their sum is always zero — that
	// invariant is what the Python reconciler checks.
	if _, err := tx.Exec(
		`INSERT INTO ledger_entries (transaction_id, account_id, amount)
		 VALUES ($1, $2, $3), ($1, $4, $5)`,
		txID, req.AccountID, delta, houseAccount, -delta,
	); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(
		`UPDATE accounts SET balance = balance + $1, version = version + 1 WHERE id = $2`,
		delta, req.AccountID,
	); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(
		`UPDATE accounts SET balance = balance - $1, version = version + 1 WHERE id = $2`,
		delta, houseAccount,
	); err != nil {
		return nil, err
	}

	// --- 5. remember the response ----------------------------------------
	out := &txResponse{TransactionID: txID, Balance: balance + delta}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(
		`UPDATE idempotency_keys SET response_body = $1, transaction_id = $2 WHERE key = $3`,
		body, txID, key,
	); err != nil {
		return nil, err
	}

	return out, tx.Commit()
}

// ---------- helpers ----------

func hashRequest(r txRequest) string {
	raw := strings.Join([]string{
		strconv.FormatInt(r.AccountID, 10),
		strconv.FormatInt(r.Amount, 10),
		r.Kind,
		r.Reference,
	}, "|")
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

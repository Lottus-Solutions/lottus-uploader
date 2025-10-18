package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	amqp "github.com/rabbitmq/amqp091-go"
)

const rabbitMQURL = "amqp://guest:guest@127.0.0.1:5672/"
const queueName = "upload_processamento_fila"
const storagePath = "./uploads_temp"

var rabbitMQChannel *amqp.Channel

func init() {
	if err := os.MkdirAll(storagePath, os.ModePerm); err != nil {
		log.Fatalf("Falha ao criar diretório de uploads: %v", err)
	}

	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Falha ao conectar ao RabbitMQ: %v", err)
	}

	rabbitMQChannel, err = conn.Channel()
	if err != nil {
		log.Fatalf("Falha ao abrir o canal do RabbitMQ: %v", err)
	}

	_, err = rabbitMQChannel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Falha ao declarar a fila: %v", err)
	}
	log.Println("Conectado ao RabbitMQ e fila declarada.")
}

type JobPayload struct {
	FilePath   string `json:"file_path"`
	Finalidade string `json:"finalidade"`
	Token      string `json:"token"`
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Recebida requisição de upload.")
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "Bearer ") {
		http.Error(w, `{"erro": "Token JWT ausente ou inválido."}`, http.StatusUnauthorized)
		return
	}
	token := strings.Split(authHeader, " ")[1]

	finalidade := r.FormValue("finalidade")
	if finalidade == "" {
		http.Error(w, `{"erro": "Finalidade do upload não especificada."}`, http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("arquivo")
	if err != nil {
		http.Error(w, `{"erro": "Nenhum arquivo enviado ou erro ao ler o formulário."}`, http.StatusBadRequest)
		return
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(header.Filename))
	if ext != ".csv" && ext != ".xlsx" {
		http.Error(w, `{"erro": "Formato de arquivo inválido. Use CSV ou XLSX."}`, http.StatusBadRequest)
		return
	}

	uniqueID := uuid.New().String()
	newFilename := fmt.Sprintf("%s%s", uniqueID, ext)
	filePath := filepath.Join(storagePath, newFilename)

	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		log.Printf("Erro ao obter caminho absoluto: %v", err)
		http.Error(w, `{"erro": "Erro interno ao processar o caminho do arquivo."}`, http.StatusInternalServerError)
		return
	}

	dst, err := os.Create(filePath)
	if err != nil {
		log.Printf("Erro ao criar arquivo temp: %v", err)
		http.Error(w, `{"erro": "Erro interno ao armazenar o arquivo."}`, http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		log.Printf("Erro ao salvar arquivo temp: %v", err)
		http.Error(w, `{"erro": "Erro interno ao armazenar o arquivo."}`, http.StatusInternalServerError)
		os.Remove(filePath)
		return
	}

	job := JobPayload{
		FilePath:   absFilePath,
		Finalidade: finalidade,
		Token:      token,
	}

	jobJSON, err := json.Marshal(job)
	if err != nil {
		log.Printf("Erro ao criar JSON do job: %v", err)
		http.Error(w, `{"erro": "Erro interno ao enfileirar o processamento."}`, http.StatusInternalServerError)
		os.Remove(filePath)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = rabbitMQChannel.PublishWithContext(
		ctx,
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         jobJSON,
			DeliveryMode: amqp.Persistent,
		},
	)

	if err != nil {
		log.Printf("Erro ao publicar no RabbitMQ: %v", err)
		http.Error(w, `{"erro": "Erro interno ao enfileirar o processamento."}`, http.StatusInternalServerError)
		os.Remove(filePath)
		return
	} else {
		log.Println("Mensagem publicada no RabbitMQ com sucesso.")
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"mensagem": "Upload recebido e processamento enfileirado com sucesso."}`))
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/upload", uploadHandler).Methods("POST")

	corsHandler := handlers.CORS(
		handlers.AllowedOrigins([]string{"http://localhost:5173"}),
		handlers.AllowedMethods([]string{"POST", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Authorization", "Content-Type"}),
	)

	port := "5000"
	log.Printf("API Gateway rodando na porta %s...", port)
	if err := http.ListenAndServe(":"+port, corsHandler(r)); err != nil {
		log.Fatalf("Erro ao iniciar servidor: %v", err)
	}
}

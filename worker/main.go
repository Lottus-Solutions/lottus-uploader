package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocarina/gocsv"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xuri/excelize/v2"
)

const rabbitMQURL = "amqp://guest:guest@127.0.0.1:5672/"
const queueName = "upload_processamento_fila"
const numWorkers = 5

const maxCategoriaWorkers = 140
const maxLivroWorkers = 25

const maxTurmaWorkers = 140
const maxAlunoWorkers = 140

const javaLivroEndpoint = "http://localhost:8080/livros"
const javaCategoriaEndpoint = "http://localhost:8080/categorias/obter-ou-criar"
const javaAlunoEndpoint = "http://localhost:8080/alunos/cadastrar"
const javaTurmaEndpoint = "http://localhost:8080/turmas"

type JobPayload struct {
	FilePath   string `json:"file_path"`
	Finalidade string `json:"finalidade"`
	Token      string `json:"token"`
}

type Livro struct {
	Nome          string `csv:"Título"`
	Autor         string `csv:"Autor/Autora"`
	CategoriaNome string `csv:"Categoria"`
	Quantidade    int    `csv:"Quantidade"`
	CategoriaID   int    `json:"categoriaId"`
	Descricao     string `json:"descricao"`
}

type Categoria struct {
	ID        int    `json:"id"`
	Nome      string `json:"nome"`
	Descricao string `json:"descricao"`
}

type Aluno struct {
	Nome           string  `csv:"Aluno"`
	TurmaNome      string  `csv:"Turma"`
	QtdBonus       float64 `json:"qtdBonus"`
	QtdLivrosLidos int     `json:"qtdLivrosLidos"`
	TurmaID        int     `json:"turmaId"`
}

type Turma struct {
	ID    int    `json:"id"`
	Serie string `json:"serie"`
}

var httpClient = http.Client{Timeout: 30 * time.Second}

// --- Funções Genéricas de Postagem (Auxiliares) ---

func postRequest(url, token string, payload interface{}) ([]byte, int, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	return bodyBytes, resp.StatusCode, nil
}

// --- Lógica para Turmas (Alunos) ---

func postTurma(nomeTurma, token string) (int, error) {
	payload := map[string]string{"serie": nomeTurma}
	body, status, err := postRequest(javaTurmaEndpoint, token, payload)

	if err != nil {
		return 0, err
	}

	if status == http.StatusCreated || status == http.StatusOK {
		var turmaResponse Turma
		if err := json.Unmarshal(body, &turmaResponse); err != nil {
			return 0, fmt.Errorf("erro ao decodificar resposta da turma: %v", err)
		}
		return turmaResponse.ID, nil
	}

	return 0, fmt.Errorf("falha ao cadastrar turma '%s'. Status: %d. Resposta: %s", nomeTurma, status, string(body))
}

func enviarTurmas(turmasUnicas []string, token string) map[string]int {
	const maxWorkers = maxTurmaWorkers

	jobs := make(chan string, len(turmasUnicas))
	results := make(chan struct {
		name string
		id   int
	}, len(turmasUnicas))

	var wg sync.WaitGroup

	for w := 1; w <= maxWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for nomeTurma := range jobs {
				id, err := postTurma(nomeTurma, token)
				if err != nil {
					log.Printf("Erro ao enviar turma '%s': %v", nomeTurma, err)
				} else {
					results <- struct {
						name string
						id   int
					}{name: nomeTurma, id: id}
				}
			}
		}()
	}

	for _, nomeTurma := range turmasUnicas {
		jobs <- nomeTurma
	}
	close(jobs)

	wg.Wait()
	close(results)

	mapaTurmas := make(map[string]int)
	for res := range results {
		mapaTurmas[res.name] = res.id
	}

	return mapaTurmas
}

func enviarListaAlunos(alunos []Aluno, token string) (int, []string) {
	const maxWorkers = maxAlunoWorkers

	jobs := make(chan Aluno, len(alunos))
	results := make(chan error, len(alunos))

	var wg sync.WaitGroup

	for w := 1; w <= maxWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for aluno := range jobs {
				payload := map[string]interface{}{
					"nome":           aluno.Nome,
					"qtdBonus":       aluno.QtdBonus,
					"qtdLivrosLidos": aluno.QtdLivrosLidos,
					"turmaId":        aluno.TurmaID,
				}
				body, status, err := postRequest(javaAlunoEndpoint, token, payload)
				if err != nil {
					results <- fmt.Errorf("Aluno '%s' (Turma: %s) falhou: Erro de rede: %v", aluno.Nome, aluno.TurmaNome, err)
				} else if status != http.StatusCreated && status != http.StatusOK {
					results <- fmt.Errorf("Aluno '%s' (Turma: %s) falhou: Status: %d, Resposta: %s", aluno.Nome, aluno.TurmaNome, status, string(body))
				} else {
					results <- nil // Sucesso
				}
			}
		}()
	}

	for _, aluno := range alunos {
		jobs <- aluno
	}
	close(jobs)

	wg.Wait()
	close(results)

	sucessos := 0
	errosSlice := []string{}
	for err := range results {
		if err != nil {
			errosSlice = append(errosSlice, err.Error())
		} else {
			sucessos++
		}
	}

	return sucessos, errosSlice
}

func processarAlunos(filePath, token string) (int, []string) {
	var alunos []Aluno

	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		return 0, []string{fmt.Sprintf("Erro ao abrir arquivo: %v", err)}
	}
	defer f.Close()

	err = gocsv.UnmarshalFile(f, &alunos)
	if err != nil || len(alunos) == 0 {
		alunos, err = lerAlunosXLSX(filePath)
		if err != nil {
			return 0, []string{fmt.Sprintf("Erro ao ler o arquivo (CSV/XLSX): %v", err)}
		}
	}

	if len(alunos) == 0 {
		return 0, []string{"O arquivo está vazio após a leitura."}
	}

	turmasUnicasMap := make(map[string]struct{})
	for _, a := range alunos {
		turmasUnicasMap[a.TurmaNome] = struct{}{}
	}

	var turmasUnicas []string
	for turma := range turmasUnicasMap {
		turmasUnicas = append(turmasUnicas, turma)
	}

	mapaTurmas := enviarTurmas(turmasUnicas, token)

	for i := range alunos {
		if id, ok := mapaTurmas[alunos[i].TurmaNome]; ok {
			alunos[i].TurmaID = id
		} else {
			alunos[i].TurmaID = 1
			log.Printf("Aviso: Turma '%s' do aluno '%s' não foi mapeada. Usando ID 1.", alunos[i].TurmaNome, alunos[i].Nome)
		}
		alunos[i].QtdBonus = 0.0
		alunos[i].QtdLivrosLidos = 0
	}

	return enviarListaAlunos(alunos, token)
}

func lerAlunosXLSX(filePath string) ([]Aluno, error) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, err
	}

	if len(rows) < 2 {
		return nil, fmt.Errorf("arquivo XLSX vazio ou sem cabeçalho")
	}

	header := rows[0]
	colMap := map[string]int{}
	for i, col := range header {
		colMap[strings.TrimSpace(col)] = i
	}

	requiredCols := map[string]string{"Aluno": "Nome", "Turma": "TurmaNome"}
	for reqCol := range requiredCols {
		if _, ok := colMap[reqCol]; !ok {
			return nil, fmt.Errorf("coluna obrigatória ausente: %s", reqCol)
		}
	}

	var alunos []Aluno
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		aluno := Aluno{
			Nome:      row[colMap["Aluno"]],
			TurmaNome: row[colMap["Turma"]],
		}
		alunos = append(alunos, aluno)
	}
	return alunos, nil
}

// --- Lógica de Livros (Mantida) ---

func postCategoria(nomeCategoria, token string) (int, error) {
	payload := Categoria{
		Nome:      nomeCategoria,
		Descricao: "Cadastrada via upload Go",
	}
	body, status, err := postRequest(javaCategoriaEndpoint, token, payload)
	if err != nil {
		return 0, err
	}

	if status == http.StatusCreated || status == http.StatusOK {
		var categoriaResponse Categoria
		if err := json.Unmarshal(body, &categoriaResponse); err != nil {
			return 0, fmt.Errorf("erro ao decodificar resposta da categoria: %v", err)
		}
		return categoriaResponse.ID, nil
	}

	return 0, fmt.Errorf("falha ao cadastrar categoria '%s'. Status: %d. Resposta: %s", nomeCategoria, status, string(body))
}

func enviarCategorias(categoriasUnicas []string, token string) map[string]int {
	const maxWorkers = maxCategoriaWorkers

	jobs := make(chan string, len(categoriasUnicas))
	results := make(chan struct {
		name string
		id   int
	}, len(categoriasUnicas))

	var wg sync.WaitGroup

	// Inicia os workers
	for w := 1; w <= maxWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for nomeCategoria := range jobs {
				id, err := postCategoria(nomeCategoria, token)
				if err != nil {
					log.Printf("Erro ao enviar categoria '%s': %v", nomeCategoria, err)
				} else {
					results <- struct {
						name string
						id   int
					}{name: nomeCategoria, id: id}
				}
			}
		}()
	}

	// Envia os jobs
	for _, nomeCategoria := range categoriasUnicas {
		jobs <- nomeCategoria
	}
	close(jobs)

	wg.Wait() // Espera todos os workers terminarem
	close(results)

	// Coleta os resultados
	mapaCategorias := make(map[string]int)
	for res := range results {
		mapaCategorias[res.name] = res.id
	}

	return mapaCategorias
}

func postLivro(livro Livro, token string) error {
	payload := map[string]interface{}{
		"nome":        livro.Nome,
		"autor":       livro.Autor,
		"quantidade":  livro.Quantidade,
		"categoriaId": livro.CategoriaID,
		"descricao":   livro.Descricao,
	}

	body, status, err := postRequest(javaLivroEndpoint, token, payload)

	if err != nil {
		return err
	}

	if status == http.StatusCreated || status == http.StatusOK {
		return nil
	}

	return fmt.Errorf("status: %d, resposta: %s", status, string(body))
}

func enviarListaLivros(livros []Livro, token string) (int, []string) {
	const maxWorkers = maxLivroWorkers

	jobs := make(chan Livro, len(livros))
	results := make(chan error, len(livros))

	var wg sync.WaitGroup

	for w := 1; w <= maxWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for livro := range jobs {
				err := postLivro(livro, token)
				results <- err // Envia o resultado (nil para sucesso, erro para falha)
			}
		}()
	}

	// Envia os jobs
	for _, livro := range livros {
		jobs <- livro
	}
	close(jobs)

	wg.Wait() // Espera todos os workers terminarem
	close(results)

	// Coleta os resultados
	sucessos := 0
	errosSlice := []string{}
	for err := range results {
		if err != nil {
			errosSlice = append(errosSlice, err.Error())
		} else {
			sucessos++
		}
	}

	return sucessos, errosSlice
}

func processarLivros(filePath, token string) (int, []string) {
	var livros []Livro

	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		return 0, []string{fmt.Sprintf("Erro ao abrir arquivo: %v", err)}
	}
	defer f.Close()

	err = gocsv.UnmarshalFile(f, &livros)
	if err != nil || len(livros) == 0 {
		livros, err = lerLivrosXLSX(filePath)
		if err != nil {
			return 0, []string{fmt.Sprintf("Erro ao ler o arquivo (CSV/XLSX): %v", err)}
		}
	}

	if len(livros) == 0 {
		return 0, []string{"O arquivo está vazio após a leitura."}
	}

	categoriasUnicasMap := make(map[string]struct{})
	for _, l := range livros {
		categoriasUnicasMap[l.CategoriaNome] = struct{}{}
	}
	var categoriasUnicas []string
	for cat := range categoriasUnicasMap {
		categoriasUnicas = append(categoriasUnicas, cat)
	}

	mapaCategorias := enviarCategorias(categoriasUnicas, token)

	for i := range livros {
		if id, ok := mapaCategorias[livros[i].CategoriaNome]; ok {
			livros[i].CategoriaID = id
		} else {
			livros[i].CategoriaID = 1
			erros := []string{fmt.Sprintf("Aviso: Categoria '%s' do livro '%s' não foi mapeada. Usando ID 1.", livros[i].CategoriaNome, livros[i].Nome)}
			return 0, erros
		}
		livros[i].Descricao = "Sem descrição"
	}

	return enviarListaLivros(livros, token)
}

func lerLivrosXLSX(filePath string) ([]Livro, error) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, err
	}

	if len(rows) < 2 {
		return nil, fmt.Errorf("arquivo XLSX vazio ou sem cabeçalho")
	}

	header := rows[0]
	colMap := map[string]int{}
	for i, col := range header {
		colMap[strings.TrimSpace(col)] = i
	}

	requiredCols := map[string]string{
		"Título":       "Nome",
		"Autor/Autora": "Autor",
		"Categoria":    "CategoriaNome",
		"Quantidade":   "Quantidade",
	}

	for reqCol := range requiredCols {
		if _, ok := colMap[reqCol]; !ok {
			return nil, fmt.Errorf("coluna obrigatória ausente: %s", reqCol)
		}
	}

	var livros []Livro
	for i := 1; i < len(rows); i++ {
		row := rows[i]

		quantidadeStr := row[colMap["Quantidade"]]
		q, err := strconv.Atoi(quantidadeStr)

		if err != nil {
			log.Printf("Aviso: Quantidade inválida '%s' na linha %d: %v", quantidadeStr, i+1, err)
			continue
		}

		livro := Livro{
			Nome:          row[colMap["Título"]],
			Autor:         row[colMap["Autor/Autora"]],
			CategoriaNome: row[colMap["Categoria"]],
			Quantidade:    q,
		}
		livros = append(livros, livro)
	}
	return livros, nil
}

// --- Lógica Principal do Worker ---

func processarArquivo(job JobPayload) (int, []string) {
	switch job.Finalidade {
	case "livros":
		return processarLivros(job.FilePath, job.Token)
	case "alunos":
		return processarAlunos(job.FilePath, job.Token)
	default:
		return 0, []string{fmt.Sprintf("Finalidade '%s' não implementada.", job.Finalidade)}
	}
}

func worker(deliveries <-chan amqp.Delivery, workerID int) {
	for d := range deliveries {
		var job JobPayload
		if err := json.Unmarshal(d.Body, &job); err != nil {
			log.Printf("Worker %d: Erro ao decodificar JSON: %v", workerID, err)
			d.Reject(false) // Descarta a mensagem se o JSON for inválido
			continue
		}

		log.Printf("Worker %d: Recebido job para arquivo %s, finalidade: %s", workerID, job.FilePath, job.Finalidade)

		// Processa o arquivo
		sucesso, erros := processarArquivo(job)

		if len(erros) == 0 && sucesso > 0 {
			log.Printf("Worker %d: Job para %s processado com %d sucessos.", workerID, job.FilePath, sucesso)
			d.Ack(false) // Confirma o recebimento e processamento
		} else {
			log.Printf("Worker %d: Job para %s falhou. %d sucessos e %d erros. Erros: %v", workerID, job.FilePath, sucesso, len(erros), erros)
			d.Reject(false) // Descarta a mensagem para não bloquear a fila
		}

		// Tenta remover o arquivo após o processamento
		if err := os.Remove(job.FilePath); err != nil {
			log.Printf("Worker %d: Aviso: Não foi possível remover o arquivo temp: %v", workerID, err)
		}
	}
}

func main() {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Falha ao conectar ao RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Falha ao abrir o canal do RabbitMQ: %v", err)
	}
	defer ch.Close()

	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Falha ao configurar QoS: %v", err)
	}

	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatalf("Falha ao declarar a fila: %v", err)
	}

	msgs, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Falha ao registrar consumidor: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 1; i <= numWorkers; i++ {
		go worker(msgs, i)
	}

	log.Println("Worker iniciado, aguardando mensagens...")
	wg.Wait()
}

# Lottus Uploader

Este projeto é um sistema de upload e processamento de arquivos em massa, construído em Go. Ele é projetado para receber arquivos CSV ou XLSX, enfileirar os trabalhos de processamento usando RabbitMQ e processá-los de forma assíncrona para cadastrar os dados em um serviço de backend (API Java).

## Arquitetura

O sistema é composto por três partes principais:

1.  **API de Upload (`main.go`):** Um servidor web que expõe um endpoint `/upload`. Ele recebe o upload do arquivo, salva-o temporariamente e publica uma mensagem na fila do RabbitMQ contendo o caminho do arquivo e a finalidade (ex: "livros", "alunos").
2.  **RabbitMQ:** Atua como um message broker, gerenciando a fila de trabalhos (`upload_processamento_fila`) para garantir que os processamentos ocorram de forma assín-crona e resiliente. É gerenciado via `docker-compose.yml`.
3.  **Worker (`worker/main.go`):** Um consumidor que escuta a fila do RabbitMQ. Ao receber uma mensagem, ele lê o arquivo correspondente, processa os dados (livros, alunos, etc.) e os envia para a API de backend Java de forma concorrente e controlada.

## Como Executar

1.  **Iniciar a Infraestrutura:**
    Certifique-se de ter o Docker e o Docker Compose instalados. Para iniciar o RabbitMQ, execute:
    ```sh
    docker-compose up -d
    ```

2.  **Executar a API de Upload:**
    Navegue até o diretório raiz do projeto e execute:
    ```sh
    go run main.go
    ```
    O servidor começará a escutar na porta 8081.

3.  **Executar o Worker:**
    Abra um novo terminal, navegue até o diretório `worker/` e execute:
    ```sh
    cd worker
    go run main.go
    ```
    O worker se conectará ao RabbitMQ e começará a aguardar por mensagens.

## Configuração

As principais configurações podem ser ajustadas através de constantes no topo dos arquivos `.go`:

*   **`worker/main.go`**:
    *   `maxCategoriaWorkers`, `maxLivroWorkers`, `maxTurmaWorkers`, `maxAlunoWorkers`: Controlam o número de requisições simultâneas para cada tipo de entidade, permitindo o ajuste fino da performance.
    *   `java...Endpoint`: Endereços da API Java de backend.
    *   `rabbitMQURL`, `queueName`: Configurações do RabbitMQ.

*   **`main.go`**:
    *   `rabbitMQURL`, `queueName`: Configurações do RabbitMQ.
    *   `uploadPath`: Diretório para salvar os arquivos temporários.
# **Assuming you have WSL2 installed:**

## Setting Up Ollama with GPU Support in WSL2

### Prerequisites

- WSL2 installed and configured
- Docker Desktop installed and running
- NVIDIA GPU with proper drivers installed

## GPU Configuration Check

Before starting, verify GPU access in WSL2:

```bash
nvidia-smi
```

If this command fails, you'll need to configure GPU support for WSL2 first (see separate guide).

## Directory Setup

Create the required directory structure:

```bash
mkdir -p ~/docker/ollama/{config,models,data,compose}
cd ~/docker/ollama/compose
```

## Docker Configuration

1. Create docker-compose file:

    ```bash
    nano docker-compose.yml
    ```

2. Add the following configuration:

    ```bash
      services:
        ollama:
          container_name: ollama
          image: ollama/ollama:latest
          ports:
            - "11435:11434"
          volumes:
            - ../models:/root/.ollama/models
            - ../data:/root/.ollama/data
          deploy:
            resources:
              reservations:
                devices:
                  - driver: nvidia
                    count: all
                    capabilities: [gpu]
          restart: unless-stopped

        open-webui:
          container_name: open-webui
          image: ghcr.io/open-webui/open-webui:main
          ports:
            - "8081:8080"
          environment:
            - OLLAMA_API_BASE_URL=http://ollama:11434/api
          volumes:
            - ../config:/app/config
          depends_on:
            - ollama
          restart: unless-stopped
    ```

## Management Script Setup

1. Create the management script:

    ```bash
      nano ~/docker/ollama/manage.sh
    ```

2. Add the following content:

    ```bash
    #!/bin/bash

    OLLAMA_DIR="$HOME/docker/ollama"

    case "$1" in
        "start")
            cd $OLLAMA_DIR/compose && docker-compose up -d
            ;;
        "stop")
            cd $OLLAMA_DIR/compose && docker-compose down
            ;;
        "restart")
            cd $OLLAMA_DIR/compose && docker-compose restart
            ;;
        "logs")
            cd $OLLAMA_DIR/compose && docker-compose logs -f
            ;;
        "pull")
            cd $OLLAMA_DIR/compose && docker-compose pull
            ;;
        "update")
            cd $OLLAMA_DIR/compose && docker-compose down
            docker-compose pull
            docker-compose up -d
            ;;
        *)
            echo "Usage: $0 {start|stop|restart|logs|pull|update}"
            exit 1
            ;;
    esac
    ```

3. Make the script executable:

    ```bash
    chmod +x ~/docker/ollama/manage.sh
    ```

4. Add alias to shell configuration:

    For Bash (default WSL2):

    ```bash
    echo 'alias ollama-manage="$HOME/docker/ollama/manage.sh"' >> ~/.bashrc
    source ~/.bashrc
    ```

    For Zsh:

    ```bash
    echo 'alias ollama-manage="$HOME/docker/ollama/manage.sh"' >> ~/.zshrc
    source ~/.zshrc
    ```

## Container Setup

1. Clean up existing containers:

    ```bash
    docker rm -f ollama open-webui
    docker container ls -a  # Verify removal
    docker network prune -f
    ```

    **Docker Desktop Restart** *(If Needed)*

    If you encounter any issues with container creation or port access:

    - Restart Docker Desktop

        - Right-click Docker Desktop icon in Windows system tray
        - Select "Restart"
        - Wait for Docker Desktop to fully restart

    - Verify Docker is running in WSL2:

        ```bash
        docker ps
        ```

    - If still having issues, try:

        ```bash
        # Check Docker service status

        docker info

        # Verify ports are free

        sudo lsof -i :11435
        sudo lsof -i :8081
        ```

    - After Docker restarts, proceed with starting containers...

2. Start the containers:

    ```bash
    ollama-manage start
    ```

3. Verify containers are running:

    ```bash
    docker ps
    ```

## Accessing the Services

- Web UI: <http://localhost:8081>
- Ollama API: <http://localhost:11435>

## Usage

- Common management commands:

    ```bash
    ollama-manage start    # Start services
    ollama-manage stop     # Stop services
    ollama-manage restart  # Restart services
    ollama-manage logs     # View logs
    ollama-manage update   # Update containers
    ```

## Next Steps

After successful installation:

1. Access the Web UI at <http://localhost:8081>
2. Login if you have already created an account or Sign Up
3. In the upper right you will see your profile icon (initials from your name given in sign up) select that then select Admin Panel from the dropdown
4. Select `Settings` then connections.
5. Turn the toggle for "OpenAI API" to the Off position
6. In Ollama API type <http://host.docker.internal:11434>
7. Go into the tab named `Models`
8. Under `Create a model`, simply enter the *model tag* (The name you would like the model created to be called, any name will work)
9. In the next section under `Create a model`, paste the .model file given or make adjustments if needed.
10. Clieck on `New Chat` in the upper left corner, then to the right of `New Chat`, select `Arena Model`, in the dropdown you should see your newly created model, select it.
11. Copy paste a SQL Server value into the `How can I help you today?` chat input in the center of the screen.

- For SQL Server to PostgreSQL conversion tasks, recommended models:

  - llama3.2:3b-instruct-q5_K_M
    - Best quality for SQL conversions
    - Uses q5 quantization for better accuracy
    - Ideal for complex SQL transformations
    - ~2.3GB size

  - llama3.2:3b-instruct-q4_K_M
    - Good balance of speed and accuracy
    - Slightly smaller memory footprint
    - Faster response times
    - ~2.0GB size

Note: First model download through the Web UI may take 5-15 minutes depending on your internet speed. The 'instruct' variants are specifically optimized for following detailed conversion instructions, making them ideal for SQL transformation tasks.

## Troubleshooting

- If services fail to start, check Docker Desktop is running
- Verify GPU access with nvidia-smi
- Check logs with ollama-manage logs
- Ensure ports 8081 and 11435 are not in use

## Reminder steps, created a script that should automatically do this

cd ~/docker/ollama
./setup.sh

### If the above does not work

- First clean up:
  - cd ~/docker/ollama/compose
  - docker-compose down
  - docker rm -f ollama open-webui
  - docker container ls -a  # Verify removal
  - docker network prune -f

- Then:
  - Start everything

    ```bash
    ollama-manage start
    ```

  - Wait 30 seconds

    ```bash
    sleep 30
    ```

- Pull mixtral if mpt present

    ```bash
    curl -X POST http://localhost:11435/api/pull -d '{"model": "mixtral"}'
    ```

- Verify it's working

    ```bash
    curl http://localhost:11435/api/tags
    ```

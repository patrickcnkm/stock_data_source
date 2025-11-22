.vscode/launch.json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "FastAPI: Run server (uvicorn)",
      "type": "python",
      "request": "launch",
      "module": "uvicorn",
      "args": [
        "app.main:app",
        "--reload",
        "--host", "127.0.0.1",
        "--port", "8000"
      ],
      "justMyCode": true,
      "envFile": "${workspaceFolder}/.env",
      "console": "integratedTerminal"
    },
    {
      "name": "Worker: Realtime Gateway (Futu → Redis)",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/workers/realtime_gateway.py",
      "args": [
        "--symbols", "00700.HK", "AAPL",
        "--with-orderbook"
      ],
      "envFile": "${workspaceFolder}/.env",
      "console": "integratedTerminal"
    },
    {
      "name": "Worker: Subscription Rotation",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/workers/sub_rotation_real.py",
      "envFile": "${workspaceFolder}/.env",
      "console": "integratedTerminal"
    }
  ],
  "compounds": [
    {
      "name": "Start: API + Realtime + Rotation",
      "configurations": [
        "FastAPI: Run server (uvicorn)",
        "Worker: Realtime Gateway (Futu → Redis)",
        "Worker: Subscription Rotation"
      ],
      "stopAll": true
    }
  ]
}

.vscode/tasks.json
{
  "version": "2.0.0",
  "tasks": [
    {
      "label": "Python: Create .venv",
      "type": "shell",
      "command": "python3 -m venv .venv",
      "problemMatcher": []
    },
    {
      "label": "Python: Install requirements",
      "type": "shell",
      "command": "${workspaceFolder}/.venv/bin/pip install -r requirements.txt || pip install -r requirements.txt",
      "dependsOn": ["Python: Create .venv"],
      "problemMatcher": []
    },
    {
      "label": "Docker: Up Redis (optional)",
      "type": "shell",
      "command": "docker compose up -d redis || docker-compose up -d redis",
      "problemMatcher": []
    }
  ]
}

.vscode/settings.json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.terminal.activateEnvironment": true,
  "terminal.integrated.env.osx": {
    "PYTHONPATH": "${workspaceFolder}"
  },
  "terminal.integrated.env.linux": {
    "PYTHONPATH": "${workspaceFolder}"
  },
  "terminal.integrated.env.windows": {
    "PYTHONPATH": "${workspaceFolder}"
  },
  "files.exclude": {
    "**/__pycache__": true,
    ".venv": true
  }
}

.vscode/extensions.json
{
  "recommendations": [
    "ms-python.python",
    "ms-toolsai.jupyter",
    "ms-python.vscode-pylance",
    "rangav.vscode-thunder-client"
  ]
}

# Audit report: assistant-teacher

## Structure

```
└── assistant-teacher
    ├── .env
    ├── .env.example
    ├── .git
    ├── .gitignore
    ├── README.md
    ├── data
    │   └── hf-cache
    ├── docker-compose.yml
    ├── infra
    │   ├── chunks.json
    │   ├── es-init.sh
    │   └── init.sql
    └── services
        ├── api
        │   ├── Dockerfile
        │   ├── app
        │   │   ├── __init__.py
        │   │   ├── __pycache__
        │   │   └── api
        │   │       ├── __pycache__
        │   │       ├── events.py
        │   │       ├── kafka.py
        │   │       ├── main.py
        │   │       ├── settings.py
        │   │       ├── storage.py
        │   │       └── translate.py
        │   └── requirements.txt
        ├── vectorizer
        │   ├── Dockerfile
        │   ├── app
        │   │   └── main.py
        │   ├── requirements.torch.txt
        │   └── requirements.txt
        └── worker
            ├── Dockerfile
            ├── app
            │   ├── __init__.py
            │   └── workers
            │       ├── generator_worker.py
            │       ├── grader_worker.py
            │       └── parser_worker.py
            └── requirements.txt
```

## Detected metadata

{
  "has_pyproject": false,
  "has_requirements": false,
  "has_package_json": false,
  "has_dockerfile": false,
  "has_readme": false
}

## File counts by extension

{
  "": 5,
  ".example": 1,
  ".yml": 1,
  ".md": 1,
  ".json": 1,
  ".sh": 1,
  ".sql": 1,
  ".txt": 4,
  ".py": 12
}

## LOC by extension

{
  "": 68,
  ".example": 5,
  ".yml": 200,
  ".md": 0,
  ".json": 12,
  ".sh": 64,
  ".sql": 8,
  ".txt": 24,
  ".py": 799
}

## Python syntax check

All .py files parse successfully.
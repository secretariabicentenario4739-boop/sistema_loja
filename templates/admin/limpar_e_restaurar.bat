@echo off
chcp 65001 > nul
title RESTAURAR BACKUP - SOLUCAO DEFINITIVA
color 0E

echo ============================================================
echo        RESTAURACAO DEFINITIVA DO BANCO
echo ============================================================
echo.

:: Configura senha
set PGPASSWORD=1kmSlZWYS4IywIdCGskYDkAnFF4mzyTL

:: DEFINA O CAMINHO DO PSQL AQUI - AJUSTE CONFORME SUA INSTALACAO
:: Verifica se existe nas pastas comuns
if exist "C:\Program Files\PostgreSQL\18\bin\psql.exe" (
    set PSQL_PATH=C:\Program Files\PostgreSQL\16\bin\psql.exe
) else if exist "C:\Program Files\PostgreSQL\15\bin\psql.exe" (
    set PSQL_PATH=C:\Program Files\PostgreSQL\15\bin\psql.exe
) else if exist "C:\Program Files\PostgreSQL\14\bin\psql.exe" (
    set PSQL_PATH=C:\Program Files\PostgreSQL\14\bin\psql.exe
) else if exist "C:\Program Files\PostgreSQL\13\bin\psql.exe" (
    set PSQL_PATH=C:\Program Files\PostgreSQL\13\bin\psql.exe
) else (
    echo [ERRO] PostgreSQL nao encontrado!
    echo.
    echo Por favor, instale o PostgreSQL ou adicione ao PATH.
    echo.
    echo Para instalar: https://www.postgresql.org/download/windows/
    echo.
    pause
    exit /b 1
)

echo [1/4] Usando PostgreSQL em: %PSQL_PATH%
echo.

:: Testa conexao
echo [2/4] Testando conexao...
"%PSQL_PATH%" -h localhost -p 5432 -U postgres -d sistema_maconico -c "SELECT 1;" > nul 2>&1
if errorlevel 1 (
    echo [ERRO] Nao foi possivel conectar ao PostgreSQL!
    echo.
    echo Verifique se:
    echo   1. O PostgreSQL esta rodando (services.msc)
    echo   2. O banco 'sistema_maconico' existe
    echo   3. A senha esta correta
    pause
    exit /b 1
)
echo       OK

:: Lista backups disponiveis
echo.
echo [3/4] Backups disponiveis:
echo.
dir /b backups\backup_*.sql 2>nul
if errorlevel 1 (
    echo Nenhum backup encontrado na pasta 'backups'
    pause
    exit /b 1
)

echo.
set /p arquivo=Digite o nome do arquivo de backup (ex: backup_20240326_143022.sql): 

if "%arquivo%"=="" (
    echo Arquivo nao especificado!
    pause
    exit /b 1
)

if not exist "backups\%arquivo%" (
    echo Arquivo nao encontrado: backups\%arquivo%
    pause
    exit /b 1
)

:: Confirma
echo.
echo ============================================================
echo ATENCAO: Isso vai APAGAR TODOS OS DADOS do banco!
echo ============================================================
set /p confirm="Digite 'SIM' para confirmar: "

if not "%confirm%"=="SIM" (
    echo Operacao cancelada!
    pause
    exit /b 0
)

:: Limpa tabelas
echo.
echo [4/4] Limpando tabelas e restaurando...
"%PSQL_PATH%" -h localhost -p 5432 -U postgres -d sistema_maconico -c "TRUNCATE TABLE atas RESTART IDENTITY CASCADE;" 2>nul
"%PSQL_PATH%" -h localhost -p 5432 -U postgres -d sistema_maconico -c "TRUNCATE TABLE assinaturas_ata RESTART IDENTITY CASCADE;" 2>nul
"%PSQL_PATH%" -h localhost -p 5432 -U postgres -d sistema_maconico -c "TRUNCATE TABLE produtos RESTART IDENTITY CASCADE;" 2>nul
"%PSQL_PATH%" -h localhost -p 5432 -U postgres -d sistema_maconico -c "TRUNCATE TABLE usuarios RESTART IDENTITY CASCADE;" 2>nul
echo       Tabelas limpas

:: Restaura backup
echo.
echo Restaurando backup: %arquivo%
"%PSQL_PATH%" -h localhost -p 5432 -U postgres -d sistema_maconico -f "backups\%arquivo%"

if errorlevel 1 (
    echo ERRO na restauracao!
    pause
    exit /b 1
)

:: Verifica resultado
echo.
echo ============================================================
echo VERIFICANDO RESULTADO:
echo ============================================================
"%PSQL_PATH%" -h localhost -p 5432 -U postgres -d sistema_maconico -c "SELECT COUNT(*) as total_atas FROM atas;"
"%PSQL_PATH%" -h localhost -p 5432 -U postgres -d sistema_maconico -c "SELECT COUNT(*) as total_assinaturas FROM assinaturas_ata;"

echo.
echo ============================================================
echo RESTAURACAO CONCLUIDA COM SUCESSO!
echo ============================================================
pause
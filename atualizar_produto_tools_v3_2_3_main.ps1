#requires -Version 5.1

[CmdletBinding()]
param(
    [string]$RepositoryUrl = "https://github.com/luizbicalho2024/produto_tools.git",
    [string]$Branch = "main",
    [string]$CommitMessage = "Adiciona raias dinamicas, selecao multipla e decisoes semanticas no Produto Tools 3.2.3",
    [switch]$SkipTests
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"

function Write-Step {
    param([Parameter(Mandatory = $true)][string]$Text)
    Write-Host ""
    Write-Host ("=== " + $Text + " ===") -ForegroundColor Cyan
}

function Assert-NativeSuccess {
    param([Parameter(Mandatory = $true)][string]$Message)
    if ($LASTEXITCODE -ne 0) {
        throw ($Message + " Exit code: " + $LASTEXITCODE)
    }
}

function Get-PythonLauncher {
    $PyCommand = Get-Command py -ErrorAction SilentlyContinue
    if ($null -ne $PyCommand) {
        return @{
            Command = [string]$PyCommand.Source
            Prefix = @("-3")
        }
    }

    $PythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($null -ne $PythonCommand) {
        return @{
            Command = [string]$PythonCommand.Source
            Prefix = @()
        }
    }

    return $null
}

function Invoke-PythonCommand {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Launcher,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $AllArguments = @()
    $AllArguments += $Launcher.Prefix
    $AllArguments += $Arguments
    $Executable = [string]$Launcher.Command

    & $Executable @AllArguments
    Assert-NativeSuccess $FailureMessage
}

$Source = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($Source)) {
    $Source = Split-Path -Parent $MyInvocation.MyCommand.Path
}

$Source = (Resolve-Path -LiteralPath $Source).Path
$Parent = Split-Path -Parent $Source
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$Destination = Join-Path $Parent ("produto_tools_publicacao_" + $Timestamp)
$OriginalLocation = Get-Location

try {
    Write-Step "Produto Tools 3.2.3 - Publicacao segura"
    Write-Host ("Origem: " + $Source)
    Write-Host ("Clone:  " + $Destination)

    $RequiredFiles = @(
        "login_app.py",
        "requirements.txt",
        "database.py",
        "pages\1_Gestao_de_Acesso.py",
        "pages\2_Central_de_Processos.py",
        "pages\3_Gestao_de_Projetos.py",
        "pages\4_Mapa_de_Relacoes.py",
        "pages\5_Editor_de_Fluxos.py",
        "components\flow_editor\frontend\main.js",
        "components\flow_editor\frontend\styles.css",
        "services\flowchart_repository.py",
        "services\project_repository.py",
        "services\flow_analytics.py",
        "tests\test_release_322.py",
        "tests\test_release_323.py",
        "schemas\flowchart_schema.py",
        "examples\sigyo_modular_project.zip"
    )

    foreach ($RelativePath in $RequiredFiles) {
        $FullPath = Join-Path $Source $RelativePath
        if (-not (Test-Path -LiteralPath $FullPath -PathType Leaf)) {
            throw ("Arquivo obrigatorio nao encontrado: " + $RelativePath)
        }
    }

    if (Test-Path -LiteralPath (Join-Path $Source "produto_tools")) {
        throw "Foi encontrada uma pasta produto_tools aninhada. Extraia o ZIP novamente na raiz."
    }

    $GitCommand = Get-Command git -ErrorAction SilentlyContinue
    if ($null -eq $GitCommand) {
        throw "Git nao encontrado. Instale o Git for Windows e abra um novo PowerShell."
    }

    $GitExe = [string]$GitCommand.Source
    & $GitExe --version | Out-Host
    Assert-NativeSuccess "Falha ao executar o Git."

    if (-not $SkipTests) {
        Write-Step "Validacoes locais"

        $PythonLauncher = Get-PythonLauncher
        if ($null -eq $PythonLauncher) {
            throw "Python nao encontrado. Instale o Python 3 ou execute novamente com -SkipTests."
        }

        Push-Location $Source
        try {
            Invoke-PythonCommand `
                -Launcher $PythonLauncher `
                -Arguments @("-m", "compileall", "-q", ".") `
                -FailureMessage "Falha na compilacao dos arquivos Python."

            Invoke-PythonCommand `
                -Launcher $PythonLauncher `
                -Arguments @("-c", "import zipfile; z=zipfile.ZipFile('examples/sigyo_modular_project.zip'); z.testzip() is None or (_ for _ in ()).throw(RuntimeError('ZIP invalido'))") `
                -FailureMessage "Falha na validacao do pacote de exemplo SIGYO."

            Invoke-PythonCommand `
                -Launcher $PythonLauncher `
                -Arguments @("-c", "from pathlib import Path; required=['pages/1_Gestao_de_Acesso.py','pages/2_Central_de_Processos.py','pages/3_Gestao_de_Projetos.py','pages/4_Mapa_de_Relacoes.py','pages/5_Editor_de_Fluxos.py']; missing=[p for p in required if not Path(p).is_file()]; missing and (_ for _ in ()).throw(RuntimeError('Paginas ausentes: '+', '.join(missing)))") `
                -FailureMessage "Falha na validacao das paginas do Streamlit."

            Invoke-PythonCommand `
                -Launcher $PythonLauncher `
                -Arguments @("-c", "from pathlib import Path; files=[Path('login_app.py'),Path('core/auth.py')]; bad=[str(p) for p in files if 'compact=True' in p.read_text(encoding='utf-8')]; bad and (_ for _ in ()).throw(RuntimeError('Chamadas incompativeis do seletor de tema: '+', '.join(bad)))") `
                -FailureMessage "Falha na validacao de compatibilidade do seletor de tema."

            Invoke-PythonCommand `
                -Launcher $PythonLauncher `
                -Arguments @("-c", "from pathlib import Path; required={'components/flow_editor/frontend/index.html':['autoFitLanes','navigation-warning','nav-save'],'components/flow_editor/frontend/main.js':['fitLanesToContent','selectedNodeIds','decisionEdgeSemantic','event.button === 2','hostDocument'],'components/flow_editor/frontend/styles.css':['edge-positive','edge-negative','multi-selected'],'core/styles.py':['stSidebarUserContent','aria-haspopup'],'pages/4_Mapa_de_Relacoes.py':['decision_edge_semantic','drawArrow','requestFullscreen']}; missing=[f'{path}: {token}' for path,tokens in required.items() for token in tokens if token not in Path(path).read_text(encoding='utf-8')]; missing and (_ for _ in ()).throw(RuntimeError('Recursos 3.2.3 ausentes: '+', '.join(missing)))") `
                -FailureMessage "Falha na validacao dos recursos da versao 3.2.3."

            $NodeCommand = Get-Command node -ErrorAction SilentlyContinue
            if ($null -ne $NodeCommand) {
                $NodeExe = [string]$NodeCommand.Source
                & $NodeExe --check "components/flow_editor/frontend/main.js"
                Assert-NativeSuccess "Falha na validacao do JavaScript."
            }
            else {
                Write-Host "Node.js nao encontrado. A validacao JavaScript local foi ignorada." -ForegroundColor Yellow
                Write-Host "O GitHub Actions executara essa validacao depois do push." -ForegroundColor Yellow
            }

            $PytestCheckArguments = @()
            $PytestCheckArguments += $PythonLauncher.Prefix
            $PytestCheckArguments += @("-c", "import pytest")
            $PythonExe = [string]$PythonLauncher.Command

            & $PythonExe @PytestCheckArguments 2>$null
            $PytestAvailable = ($LASTEXITCODE -eq 0)

            if ($PytestAvailable) {
                Invoke-PythonCommand `
                    -Launcher $PythonLauncher `
                    -Arguments @("-m", "pytest", "-q") `
                    -FailureMessage "Os testes automatizados falharam."
            }
            else {
                Write-Host "Pytest nao encontrado. Os testes locais foram ignorados." -ForegroundColor Yellow
                Write-Host "Para instalar, execute: py -3 -m pip install -r requirements-dev.txt" -ForegroundColor Yellow
                Write-Host "O GitHub Actions executara a suite completa depois do push." -ForegroundColor Yellow
            }
        }
        finally {
            Pop-Location
        }
    }

    Write-Step "Clonando o repositorio"
    & $GitExe clone --branch $Branch --single-branch $RepositoryUrl $Destination
    Assert-NativeSuccess "Nao foi possivel clonar o repositorio."

    $GitDirectory = Join-Path $Destination ".git"
    if (-not (Test-Path -LiteralPath $GitDirectory -PathType Container)) {
        throw "O clone nao possui a pasta .git."
    }

    Write-Step "Limpando a copia da branch"
    Get-ChildItem -LiteralPath $Destination -Force |
        Where-Object { $_.Name -ne ".git" } |
        ForEach-Object {
            Remove-Item -LiteralPath $_.FullName -Recurse -Force
        }

    Write-Step "Copiando o Produto Tools"
    $RoboCopyCommand = Get-Command robocopy -ErrorAction SilentlyContinue
    if ($null -eq $RoboCopyCommand) {
        throw "Robocopy nao encontrado no Windows."
    }

    $RoboCopyExe = [string]$RoboCopyCommand.Source
    $RoboCopyArguments = @(
        $Source,
        $Destination,
        "/E",
        "/COPY:DAT",
        "/DCOPY:DAT",
        "/R:2",
        "/W:1",
        "/NFL",
        "/NDL",
        "/NJH",
        "/NJS",
        "/NP",
        "/XD",
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        ".pytest_cache",
        "node_modules",
        "/XF",
        "*.pyc",
        "*.pyo",
        ".env",
        "secrets.toml",
        "users.db",
        "produto_tools.db"
    )

    & $RoboCopyExe @RoboCopyArguments | Out-Host
    $RoboCopyExitCode = $LASTEXITCODE
    if ($RoboCopyExitCode -ge 8) {
        throw ("Falha ao copiar os arquivos. Codigo do Robocopy: " + $RoboCopyExitCode)
    }

    $SecretsPath = Join-Path $Destination ".streamlit\secrets.toml"
    if (Test-Path -LiteralPath $SecretsPath) {
        throw "Foi encontrado .streamlit\secrets.toml. Remova as credenciais reais antes de publicar."
    }

    if (Test-Path -LiteralPath (Join-Path $Destination "produto_tools")) {
        throw "A copia criou uma pasta produto_tools duplicada. Processo interrompido."
    }

    if (-not (Test-Path -LiteralPath (Join-Path $Destination "login_app.py") -PathType Leaf)) {
        throw "login_app.py nao esta na raiz do clone."
    }

    Set-Location $Destination

    $InsideGitOutput = @(& $GitExe rev-parse --is-inside-work-tree)
    $GitCheckExitCode = $LASTEXITCODE
    if ($GitCheckExitCode -ne 0) {
        throw ("O destino nao foi reconhecido como repositorio Git. Exit code: " + $GitCheckExitCode)
    }

    $InsideGit = ($InsideGitOutput -join "").Trim()
    if ($InsideGit -ne "true") {
        throw "O destino nao esta dentro de um work tree Git."
    }

    $GitUserNameOutput = @(& $GitExe config user.name)
    $GitUserName = ($GitUserNameOutput -join "").Trim()

    $GitUserEmailOutput = @(& $GitExe config user.email)
    $GitUserEmail = ($GitUserEmailOutput -join "").Trim()

    if ([string]::IsNullOrWhiteSpace($GitUserName) -or [string]::IsNullOrWhiteSpace($GitUserEmail)) {
        Write-Host "A identidade do Git ainda nao esta configurada." -ForegroundColor Yellow
        Write-Host 'Execute: git config --global user.name "Luiz Bicalho"' -ForegroundColor Yellow
        Write-Host 'Execute: git config --global user.email "SEU_EMAIL_DO_GITHUB"' -ForegroundColor Yellow
        throw "Configure a identidade do Git e execute o script novamente."
    }

    Write-Step "Alteracoes encontradas"
    & $GitExe status --short | Out-Host
    Assert-NativeSuccess "Falha ao consultar o status do Git."

    & $GitExe add -A
    Assert-NativeSuccess "Falha ao adicionar as alteracoes."

    $Changes = @(& $GitExe status --porcelain)
    Assert-NativeSuccess "Falha ao consultar as alteracoes preparadas."

    if ($Changes.Count -eq 0) {
        Write-Host "Nenhuma alteracao encontrada. A branch ja esta atualizada." -ForegroundColor Yellow
        return
    }

    Write-Step "Criando o commit"
    & $GitExe commit -m $CommitMessage
    Assert-NativeSuccess "Falha ao criar o commit."

    Write-Step "Enviando para o GitHub"
    & $GitExe push origin $Branch
    Assert-NativeSuccess "Falha ao enviar as alteracoes para o GitHub."

    Write-Host ""
    Write-Host "Produto Tools 3.2.3 publicado com sucesso." -ForegroundColor Green
    Write-Host ("Repositorio: " + $RepositoryUrl)
    Write-Host ("Branch:      " + $Branch)
    Write-Host ("Clone local: " + $Destination)
    Write-Host "O Streamlit Cloud devera iniciar um novo deploy automaticamente."
}
catch {
    Write-Host ""
    Write-Host "PUBLICACAO INTERROMPIDA" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host "Nenhuma mensagem de sucesso foi emitida." -ForegroundColor Yellow
    exit 1
}
finally {
    Set-Location $OriginalLocation
}

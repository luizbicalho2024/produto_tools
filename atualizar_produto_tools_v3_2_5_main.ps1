#requires -Version 5.1

[CmdletBinding()]
param(
    [string]$RepositoryUrl = "https://github.com/luizbicalho2024/produto_tools.git",
    [string]$Branch = "main",
    [string]$CommitMessage = "Atualiza Produto Tools 3.2.5 - editor persistente e PDF legivel",
    [switch]$SkipTests,
    [switch]$StrictTests
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"

function Write-Step {
    param([Parameter(Mandatory = $true)][string]$Text)
    Write-Host ""
    Write-Host ("=== " + $Text + " ===") -ForegroundColor Cyan
}

function Get-PythonLauncher {
    $PyCommand = Get-Command py -ErrorAction SilentlyContinue
    if ($null -ne $PyCommand) {
        return @{ Command = [string]$PyCommand.Source; Prefix = @("-3") }
    }

    $PythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($null -ne $PythonCommand) {
        return @{ Command = [string]$PythonCommand.Source; Prefix = @() }
    }

    return $null
}

function Invoke-NativeCaptured {
    param(
        [Parameter(Mandatory = $true)][string]$Executable,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Label,
        [switch]$AllowFailure
    )

    $PreviousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $Output = @(& $Executable @Arguments 2>&1)
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousPreference
    }

    if ($Output.Count -gt 0) {
        $Output | ForEach-Object { Write-Host ([string]$_) }
    }

    if ($ExitCode -ne 0 -and -not $AllowFailure) {
        $Details = ($Output | ForEach-Object { [string]$_ }) -join [Environment]::NewLine
        if ([string]::IsNullOrWhiteSpace($Details)) {
            $Details = "Sem detalhes adicionais."
        }
        throw ($Label + " falhou. Exit code: " + $ExitCode + [Environment]::NewLine + $Details)
    }

    return @{ ExitCode = $ExitCode; Output = $Output }
}

function Invoke-PythonCaptured {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Launcher,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Label,
        [switch]$AllowFailure
    )

    $AllArguments = @()
    $AllArguments += $Launcher.Prefix
    $AllArguments += $Arguments

    return Invoke-NativeCaptured `
        -Executable ([string]$Launcher.Command) `
        -Arguments $AllArguments `
        -Label $Label `
        -AllowFailure:$AllowFailure
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
    Write-Step "Produto Tools 3.2.5 - Publicacao segura"
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
        "components\flow_editor\frontend\index.html",
        "components\flow_editor\frontend\main.js",
        "components\flow_editor\frontend\styles.css",
        "services\flowchart_repository.py",
        "services\project_repository.py",
        "services\flow_analytics.py",
        "services\report_export.py",
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
    Invoke-NativeCaptured -Executable $GitExe -Arguments @("--version") -Label "Validacao do Git" | Out-Null

    if (-not $SkipTests) {
        Write-Step "Validacoes locais"

        $PythonLauncher = Get-PythonLauncher
        if ($null -eq $PythonLauncher) {
            Write-Host "Python nao encontrado. Validacoes Python locais foram ignoradas." -ForegroundColor Yellow
            Write-Host "O GitHub Actions podera executar a suite apos o push." -ForegroundColor Yellow
        }
        else {
            Push-Location $Source
            try {
                Invoke-PythonCaptured `
                    -Launcher $PythonLauncher `
                    -Arguments @("-m", "compileall", "-q", ".") `
                    -Label "Compilacao Python" | Out-Null
                Write-Host "OK - compilacao Python" -ForegroundColor Green

                Invoke-PythonCaptured `
                    -Launcher $PythonLauncher `
                    -Arguments @("-c", "import zipfile; z=zipfile.ZipFile('examples/sigyo_modular_project.zip'); bad=z.testzip(); bad is None or (_ for _ in ()).throw(RuntimeError('Arquivo corrompido: '+str(bad)))") `
                    -Label "Validacao do pacote SIGYO" | Out-Null
                Write-Host "OK - pacote SIGYO" -ForegroundColor Green

                $FeatureScript = @'
from pathlib import Path
checks = {
    'components/flow_editor/frontend/index.html': ['Exportar visual'],
    'components/flow_editor/frontend/main.js': ['fitLanesToContent', 'selectedNodeIds', 'decisionEdgeSemantic', 'protectCurrentDocumentLocally', 'pendentes no banco'],
    'components/flow_editor/frontend/styles.css': ['top: -13px', 'translateX(-50%)'],
    'pages/5_Editor_de_Fluxos.py': ['Downloads do fluxo', 'flow_only_pdf', 'full_documentation_pdf', 'cached_export_bundle'],
    'services/report_export.py': ['flow_only_pdf', 'full_documentation_pdf', 'export_bundle', '_append_dense_flow_detail_pages', 'description_lines = _wrap_text_lines'],
}
missing = []
for file_name, tokens in checks.items():
    text = Path(file_name).read_text(encoding='utf-8')
    for token in tokens:
        if token not in text:
            missing.append(f'{file_name}: {token}')
if missing:
    raise RuntimeError('Recursos ausentes: ' + '; '.join(missing))
'@

                Invoke-PythonCaptured `
                    -Launcher $PythonLauncher `
                    -Arguments @("-c", $FeatureScript) `
                    -Label "Validacao dos recursos 3.2.5" | Out-Null
                Write-Host "OK - recursos da versao" -ForegroundColor Green

                $NodeCommand = Get-Command node -ErrorAction SilentlyContinue
                if ($null -ne $NodeCommand) {
                    Invoke-NativeCaptured `
                        -Executable ([string]$NodeCommand.Source) `
                        -Arguments @("--check", "components/flow_editor/frontend/main.js") `
                        -Label "Validacao JavaScript" | Out-Null
                    Write-Host "OK - JavaScript" -ForegroundColor Green
                }
                else {
                    Write-Host "Node.js nao encontrado. Validacao JavaScript local ignorada." -ForegroundColor Yellow
                }

                $DependencyProbe = Invoke-PythonCaptured `
                    -Launcher $PythonLauncher `
                    -Arguments @("-c", "import pytest, streamlit, pymongo, pandas, jsonschema, reportlab") `
                    -Label "Verificacao das dependencias de teste" `
                    -AllowFailure

                if ($DependencyProbe.ExitCode -eq 0) {
                    $TestRun = Invoke-PythonCaptured `
                        -Launcher $PythonLauncher `
                        -Arguments @("-m", "pytest", "-q") `
                        -Label "Testes automatizados" `
                        -AllowFailure

                    if ($TestRun.ExitCode -ne 0) {
                        if ($StrictTests) {
                            throw "Os testes automatizados falharam e -StrictTests foi informado."
                        }
                        Write-Host "ATENCAO - os testes locais falharam, mas a publicacao continuara." -ForegroundColor Yellow
                        Write-Host "Use -StrictTests para transformar falhas de teste em bloqueio." -ForegroundColor Yellow
                    }
                    else {
                        Write-Host "OK - testes automatizados" -ForegroundColor Green
                    }
                }
                else {
                    Write-Host "Dependencias de teste incompletas neste Windows. Pytest local foi ignorado." -ForegroundColor Yellow
                    Write-Host "Para instalar tudo: py -3 -m pip install -r requirements-dev.txt" -ForegroundColor Yellow
                    Write-Host "Use -StrictTests somente em um ambiente local preparado." -ForegroundColor Yellow
                }
            }
            finally {
                Pop-Location
            }
        }
    }
    else {
        Write-Host "Validacoes locais ignoradas por -SkipTests." -ForegroundColor Yellow
    }

    Write-Step "Clonando o repositorio"
    Invoke-NativeCaptured `
        -Executable $GitExe `
        -Arguments @("clone", "--branch", $Branch, "--single-branch", $RepositoryUrl, $Destination) `
        -Label "Clone do repositorio" | Out-Null

    $GitDirectory = Join-Path $Destination ".git"
    if (-not (Test-Path -LiteralPath $GitDirectory -PathType Container)) {
        throw "O clone nao possui a pasta .git."
    }

    Write-Step "Limpando a copia da branch"
    Get-ChildItem -LiteralPath $Destination -Force |
        Where-Object { $_.Name -ne ".git" } |
        ForEach-Object { Remove-Item -LiteralPath $_.FullName -Recurse -Force }

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

    $PreviousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $RoboCopyExe @RoboCopyArguments | Out-Host
        $RoboCopyExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousPreference
    }

    if ($RoboCopyExitCode -ge 8) {
        throw ("Falha ao copiar os arquivos. Codigo do Robocopy: " + $RoboCopyExitCode)
    }

    $SecretsPath = Join-Path $Destination ".streamlit\secrets.toml"
    if (Test-Path -LiteralPath $SecretsPath) {
        throw "Foi encontrado .streamlit\secrets.toml. Remova credenciais reais antes de publicar."
    }

    if (Test-Path -LiteralPath (Join-Path $Destination "produto_tools")) {
        throw "A copia criou uma pasta produto_tools duplicada. Processo interrompido."
    }

    if (-not (Test-Path -LiteralPath (Join-Path $Destination "login_app.py") -PathType Leaf)) {
        throw "login_app.py nao esta na raiz do clone."
    }

    Set-Location $Destination

    $InsideGitResult = Invoke-NativeCaptured `
        -Executable $GitExe `
        -Arguments @("rev-parse", "--is-inside-work-tree") `
        -Label "Validacao do repositorio Git"
    $InsideGit = (($InsideGitResult.Output | ForEach-Object { [string]$_ }) -join "").Trim()
    if ($InsideGit -ne "true") {
        throw "O destino nao esta dentro de um work tree Git."
    }

    $UserNameResult = Invoke-NativeCaptured `
        -Executable $GitExe `
        -Arguments @("config", "user.name") `
        -Label "Leitura do user.name" `
        -AllowFailure
    $GitUserName = (($UserNameResult.Output | ForEach-Object { [string]$_ }) -join "").Trim()

    $UserEmailResult = Invoke-NativeCaptured `
        -Executable $GitExe `
        -Arguments @("config", "user.email") `
        -Label "Leitura do user.email" `
        -AllowFailure
    $GitUserEmail = (($UserEmailResult.Output | ForEach-Object { [string]$_ }) -join "").Trim()

    if ([string]::IsNullOrWhiteSpace($GitUserName) -or [string]::IsNullOrWhiteSpace($GitUserEmail)) {
        Write-Host "A identidade do Git ainda nao esta configurada." -ForegroundColor Yellow
        Write-Host 'Execute: git config --global user.name "Luiz Bicalho"' -ForegroundColor Yellow
        Write-Host 'Execute: git config --global user.email "SEU_EMAIL_DO_GITHUB"' -ForegroundColor Yellow
        throw "Configure a identidade do Git e execute o script novamente."
    }

    Write-Step "Alteracoes encontradas"
    Invoke-NativeCaptured -Executable $GitExe -Arguments @("status", "--short") -Label "Git status" | Out-Null

    Invoke-NativeCaptured -Executable $GitExe -Arguments @("add", "-A") -Label "Git add" | Out-Null

    $ChangesResult = Invoke-NativeCaptured `
        -Executable $GitExe `
        -Arguments @("status", "--porcelain") `
        -Label "Consulta das alteracoes"
    $Changes = @($ChangesResult.Output)

    if ($Changes.Count -eq 0) {
        Write-Host "Nenhuma alteracao encontrada. A branch ja esta atualizada." -ForegroundColor Yellow
        return
    }

    Write-Step "Criando o commit"
    Invoke-NativeCaptured -Executable $GitExe -Arguments @("commit", "-m", $CommitMessage) -Label "Git commit" | Out-Null

    Write-Step "Enviando para o GitHub"
    Invoke-NativeCaptured -Executable $GitExe -Arguments @("push", "origin", $Branch) -Label "Git push" | Out-Null

    Write-Host ""
    Write-Host "Produto Tools 3.2.5 publicado com sucesso." -ForegroundColor Green
    Write-Host ("Repositorio: " + $RepositoryUrl)
    Write-Host ("Branch:      " + $Branch)
    Write-Host ("Clone local: " + $Destination)
    Write-Host "O Streamlit Cloud devera iniciar um novo deploy automaticamente."
}
catch {
    Write-Host ""
    Write-Host "PUBLICACAO INTERROMPIDA" -ForegroundColor Red
    Write-Host ("Motivo: " + $_.Exception.Message) -ForegroundColor Red
    if ($null -ne $_.InvocationInfo -and -not [string]::IsNullOrWhiteSpace($_.InvocationInfo.PositionMessage)) {
        Write-Host $_.InvocationInfo.PositionMessage -ForegroundColor DarkGray
    }
    Write-Host "Nenhuma mensagem de sucesso foi emitida." -ForegroundColor Yellow
    exit 1
}
finally {
    Set-Location $OriginalLocation
}

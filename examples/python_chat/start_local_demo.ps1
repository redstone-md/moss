$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$chatScript = Join-Path $repoRoot "examples\python_chat\moss_chat.py"

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "Set-Location '$repoRoot'; python '$chatScript' --nickname Alice --listen-port 41030 --room lobby --no-trackers"
)

Start-Sleep -Milliseconds 700

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "Set-Location '$repoRoot'; python '$chatScript' --nickname Bob --listen-port 41031 --peer 127.0.0.1:41030 --room lobby --no-trackers"
)

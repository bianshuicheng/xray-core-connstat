param([string]$Path, [int]$Last = 14)
$esc = [char]27
$c = Get-Content $Path -Raw
$c = $c -replace ($esc + '\[[0-9;]*[A-Za-z]'), "`n"
$lines = $c -split "`n" | Where-Object { $_.Trim() -ne '' }
$lines | Select-Object -Last $Last

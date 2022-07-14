Function New-CashHolding {
    New-Object PSObject -Property @{
        #Required Data
        PRIOR_DAY_NAV = ''
        AST_CLS = ''
        SEC_NAME = ''
        HOLDING_L = ''
        HOLDING_S = ''
        PRC_MLT = ''
        PRICE = ''
        FX_TO_BASE = ''
        CNTRCT_SZ = ''
        CASHDIV_L = ''
        CASHDIV_S = ''

        #Generated Values
        LONG_IBOR_QTY = ''
        SHORT_IBOR_QTY = ''
        LONG = ''
        SHORT = ''
        SHORT_PROCEEDS = ''
    }
}

Function New-PreliminaryCash {
    New-Object PSObject -Property @{
        AVG_PRIOR_DAY_NAV = ''
        SUM_LONG = ''
        SUM_SHORT = ''
        SUM_SHORT_PROCEEDS = ''
        PRELIMINARY_CASH = ''
    }
}


function nullCheck ($value) {
    if($value -eq "null" -or $value -eq $null) {
        return 0
    } else {
        return [double]$value
    }
}

$MN = import-csv -path "C:\Users\u65f\Documents\PowerShell\PreliminaryCash\Market Neutral Raw Data 27-Jan-2022.csv"

$jan21 = 21992958
$jan27 = 17264450

$holdingObjects = @()

foreach($holding in ($MN | Where{$_.PORT_CODE -eq "RR65"})) {
    $h = New-CashHolding

    $h.PRIOR_DAY_NAV = $holding.PRIOR_DAY_NAV
    $h.AST_CLS = $holding.AST_CLS
    $h.SEC_NAME = $holding.SEC_NAME
    $h.HOLDING_L = nullCheck($holding.HOLDING_L)
    $h.HOLDING_S = nullCheck($holding.HOLDING_S)
    $h.PRC_MLT = $holding.PRC_MLT
    $h.PRICE = $holding.PRICE
    $h.FX_TO_BASE = $holding.FX_TO_BASE
    $h.CNTRCT_SZ = $holding.CNTRCT_SZ
    $h.CASHDIV_L = nullCheck($holding.CASHDIV_L)
    $h.CASHDIV_L = nullCheck($holding.CASHDIV_S)

    if(($h.AST_CLS -eq 'EQ' -or ($h.AST_CLS -eq 'MM' -and  $h.SEC_NAME -eq 'SHORT PROCEEDS'))) {
        $h.LONG_IBOR_QTY = $h.HOLDING_L
        $h.SHORT_IBOR_QTY = $h.HOLDING_S
    }

    if($h.AST_CLS -eq 'EQ') {
        $h.LONG = ([double]$h.LONG_IBOR_QTY * [double]$h.PRC_MLT * [double]$h.PRICE * [double]$h.FX_TO_BASE * [double]$h.CNTRCT_SZ) + $h.CASHDIV_L
        $h.SHORT = ([double]$h.SHORT_IBOR_QTY * [double]$h.PRC_MLT * [double]$h.PRICE * [double]$h.FX_TO_BASE * [double]$h.CNTRCT_SZ) + $h.CASHDIV_S
    }

    if($h.SEC_NAME -eq 'SHORT PROCEEDS') {
        $h.SHORT_PROCEEDS = [System.Math]::Abs($h.LONG_IBOR_QTY) + [System.Math]::Abs($h.SHORT_IBOR_QTY)
    }

    $holdingObjects += $h
}

$p = New-PreliminaryCash
$p.AVG_PRIOR_DAY_NAV = ($holdingObjects.PRIOR_DAY_NAV | Measure-Object -Average).Average
$p.SUM_LONG = ($holdingObjects.LONG | Measure-Object -Sum).Sum
$p.SUM_SHORT = ($holdingObjects.SHORT | Measure-Object -Sum).Sum
$p.SUM_SHORT_PROCEEDS = ($holdingObjects.SHORT_PROCEEDS | Measure-Object -Sum).Sum

$p.PRELIMINARY_CASH = $p.AVG_PRIOR_DAY_NAV - $p.SUM_LONG + [System.Math]::Abs($p.SUM_SHORT) - $p.SUM_SHORT_PROCEEDS

Write-Host "
Jan 27 2022
Prior Day Nav is    : $($p.AVG_PRIOR_DAY_NAV)
Sum Long is         : $($p.SUM_LONG)
Sum Short is        : $($p.SUM_SHORT)
Short Proceeds is   : $($p.SUM_SHORT_PROCEEDS)

Preliminary Cash is : $($p.PRELIMINARY_CASH)
Actual value is     : $jan27
"



<#
Preliminary Cash = SUM({FIXED [PORT_CODE]: AVG([PRIOR_DAY_NAV]) - SUM([Long]) + SUM(ABS([Short])) - SUM([Short Proceeds])})


PRIOR_DAY_NAV = PREV_TOTAL_NET_ASSET


LONG = IF (AST_CLS = 'EQ') THEN Long Ibor Qty * PRC_MULTI * PRICE * FX_TO_BASE * CONTRACT_SZ + CASHDIV_L

Long Ibor Qty = IF (AST_CLS = 'EQ' or (AST_CLS = 'MM' And SEC_NAME = 'SHORT PROCEEDS')) THEN HOLDING_L


SHORT = IF (AST_CLS) = 'EQ' THEN Short Ibor Qty * PRC_MULTI * PRICE * FX_TO_BASE * CONTRACT_SZ + CASHDIV_S

Short Ibor Qty = IF (AST_CLS = 'EQ' or (AST_CLS = 'MM' And SEC_NAME = 'SHORT PROCEEDS')) THEN HOLDING_S


Short Proceeds = IF(SEC_NAME = 'SHORT PROCEEDS') THEN ABS(Long Ibor Qty) + ABS(Short Ibor Qty)


CONTRACT_SZ = CNTRCT_SZ
Short Proceeds is the holdings quantity
#>

 ############################################################################
 #
 # Copyright (c) 2018 The Vanguard Group of Investment Companies (VGI)
 # All rights reserved.
 #
 # This source code is CONFIDENTIAL and PROPRIETARY to VGI. Unauthorized
 # distribution, adaptation, or use may be subject to civil and criminal
 # penalties.
 #
 ############################################################################
 
 #Author: Saisimha Thippasani
 #Job Names: AXIND305
 #Description: This job will generate a .csv file containing the global holdings for APA to read
 #Example Command Line:PowerShell.exe -file ApaGlobalHoldingsFlatFileGenScript.ps1 -rte CAT -path 11 -outputFilePath C:\users\uvdu\desktop\dumpingGround\ApaHoldingsFiles -accounts "Funds+Benchmarks.csv" -fundType "GBLF"
 #TODOS
    #Find a way to clean up all the try/catches. I think that error handling should be there, but it makes the code look messy.


#path variable is used in CAT only
[CmdletBinding()]
Param(
  [Parameter(Mandatory=$False,Position=1)]
   $rte="local",

   [Parameter(Mandatory=$False,Position=2)]
   $outputFilePath="C:\Temp\apa\ApaHoldings",

   [Parameter(Mandatory=$False,Position=3)]
   $paramFilePath="C:\Temp\apa\data\util\parameters.json",

   [Parameter(Mandatory=$False,Position=4)]
   $jobName="APAND300",

   [Parameter(Mandatory=$False,Position=5)]
   $fundType="USAF",

   [Parameter(Mandatory=$False,Position=6)]
   $odate="20220603"
)

$Global:inputSRMFilePath = $null
$Global:inputPositionFilePath = $null
$Global:polarisFilePath = $null
$Global:logFile = $null
$Global:mappingFile = $null
$Global:fundFilePath = $null
$Global:transactionFilePath = $null

#SRM hash table positions
$POS_VNUM = 0
$POS_TICKER = 1
$POS_CONTRACTSIZE = 2
$POS_SECNAME = 3
$POS_SECTYPE = 4
$POS_SECDATE = 5
$POS_ASSETID = 6


#Position object for data read from the Holdings file
Function New-Position {
  New-Object PSObject -Property @{
      acctCd = '' # PORT_CODE
      assetId = ''
      qtyEod = ''
      currency = ''
      vnum = ''
      mktVal = ''
      secType = ''
      ticker = ''
      USDValue = ''
      secName = ''
      date = ''
      marketValueLocalCurrency = '' #for forwards
      primaryAssetID = '' #for swaps
      BaseCurrency = '' #for swaps
      marketValueBaseCurrency = '' #for swaps
      legTypeCode = '' #for swaps
      odate = $odate
      secMainType = '' #AST_CLS
      secshrtdesc = '' #SEC_NAME
      prc_mlt = 1 #PRC_MLT - it is default value and didn't load into position object please make that change 
      cntrct_sz = 1 #CNTRCT_SZ - it is default value and didn't load into position object please make that change 
      fx_to_base = 1 #FX_TO_BASE  - it is default value and didn't load into position object please make that change 
      ls_indicator = '' # HOLDING_L, HOLDING_S (#basically this value will look like L 0r S) 

  }
}

#Security Object for data read from the SRM files
Function New-Security {
  New-Object PSObject -Property @{
      assetId = ''
      vnum = ''
      contractSize = ''
      ticker = ''
      secName = ''
      secType = ''
      date = ''
  }
}

#Polaris Object for data read from the Cash Neutral files
Function New-Cash {
  New-Object PSObject -Property @{
      amount = '' #PRIOR_DAY_NAV
  }
}

#Transaction Object for data read from the Transaction files
Function New-Transaction {
  New-Object PSObject -Property @{
      curr = '' # Currency will be same for both L & S Columns
      nettradecurr = '' #PRICE
      # Below is the sample output how we get values, 'curr' column will be same but nettradecurr will be there only for L or S in this case we have L so we have to read L, or else S'
      transdesccode = '' # CASHDIV , CASHDIV
      code = '' #(L, S will be coming from thius column, we have to join with CASHDIV_L or CASHDIV_S)
      # Sample output 
      # transdesccode   code   curr     nettradecurr
      # CASHDIV          L     123.55     7546.2
      # CASHDIV          S     123.55          

  }
}

#Mapping object for the data read from the mappings file
Function New-Mapping {
    New-Object PSObject -Property @{
        rootTicker = ''
        exchSymbol = ''
    }
}

Function Parse-Ticker($t, $sn){
    Write-Log("Parsing Ticker for $($t)")
    $ticker = $t
    $matureDate = $sn.Substring($sn.Length-5, 5) # parse the maturity date out of the secName
    $lastTwo = $matureDate.Substring($matureDate.Length -2, 2) # get the two letter year code
    $month = $matureDate.Substring(0,3) # get the 3 letter month code

    $monthCodes = @{
        Jan = 'F'
        Feb = 'G'
        Mar = 'H'
        Apr = 'J'
        May = 'K'
        Jun = 'M'
        Jul = 'N'
        Aug = 'Q'
        Sep = 'U'
        Oct = 'V'
        Nov = 'X'
        Dec = 'Z'

    }

    $apaFutureCode = "$($ticker.Substring(0, $ticker.Length -2))-$($monthCodes[$month])-$($lastTwo)"
    Write-Log("Ticker Mapping for $($sn): $($t) -> $($apaFutureCode)`n")
    $apaFutureCode = $apaFutureCode -replace '\s', ''
    return $apaFutureCode
}

Function Create-SRM-Objects($SRMFilesToCheck) {
    Write-Log("Reading SRM data from csv file")
    $securityObjects = @{}
    foreach($file in $SRMFilesToCheck)
    {
        Write-Log("The SRM file being read is: $file")
        try {
            $secs = Import-Csv -path $file -Delimiter "|" -ErrorAction Stop
    
            #Create Security objects and stuff
            Write-Log("Creating security objects...")
            foreach($s in $secs)
            {
                $security = New-Security
                $vnum = $s.VANGUARD_ID
                $ticker = $s.TICKER 
                $contractSize = $s.CONTRACT_SIZE
                $secName = $s.SHORT_DES
                $secType = $s.SEC_TYP_NX

                $security.date = ''
                if($s.SRM_REGIONAL_EFF_DT){$date = $s.SRM_REGIONAL_EFF_DT}
                if($s.SRM_REGIONAL_EFF_DATE){$date = $s.SRM_REGIONAL_EFF_DATE}

                if($s.VANGUARD_ID){
                    if($s.SEDOL1_ID){
                        $assetId = $s.SEDOL1_ID
                    }elseif($s.CUSIP_ID){
                        $assetId = $s.CUSIP_ID
                    }elseif($s.ISIN_ID){
                        $assetId = $s.ISIN_ID
                    }elseif($s.TICKER){
                        $assetId = $s.TICKER
                    }
                    if($assetId -and $date){
                        $securityObjects[$s.VANGUARD_ID] = ($vnum,$ticker,$contractSize,$secName,$secType,$date,$assetId) #using a hash table saves time
                    }
                }
            }#Done creating Security objects

        } catch { 
            Write-Log("Failed to read $file, skipping over it")
            Write-Log("$($PSItem)")
        }
    }#End of checking each SRM File
    
    return $securityObjects
}


##############################
#           MAIN
##############################

Write-Host "Sourcing in the helper script..."
. $PSScriptRoot\ApaGeneralHelperFunctions.ps1

$param = Get-Content $paramFilePath | Out-String | ConvertFrom-Json
Set-FilePaths -param $param -rte $rte #Call this first Since this function sets the log file up. 

Write-Log("************************************************************************************************************************************")
Write-Log("Starting Script...")

if($odate) {
    Write-Log("Running for $odate")
    $today = $odate
} else {
    $today = get-date -format "yyyyMMdd"
} 

#create a generic collection for the objects
$positionObjects = @()
$securityObjects = @()

#Collection of Identifiers/AssetID's/SEDOLS that we care about
$sedolsWeCareAbout = @()

#Collection of foriegn currencies with their exchange rates
$currencyObjects = @()

#collection of Mappings to replace certain tickers with
$mappingObjects = @()

#Get the most recent holdings file
try {
    Write-Log("The input path $($inputPositionFilePath)")

    #If $odate is supplied, get that date. Otherwise set odate to the latest file date
    if(!$odate) {
        try {
            $newestFile = Get-ChildItem "$($inputPositionFilePath)" -ErrorAction Stop | sort CreationTime -Descending | Select name `
            | Select-String -Pattern "VG_IBOR_.*$($odate)" | Where-Object {$_ -match "AMER"} | Select -First 1

            $odate = ($newestFile -replace '\D+(\d+)','$1').Substring(0,8)
            Write-Log("Set odate to $odate")
        } catch {
            Write-Log("No signal files in directory for type $signalType, odate not set")
            Write-Log("$($PSItem)")
            Exit 1
        }
    }

    $positionFileNames = Get-ChildItem "$($inputPositionFilePath)" -name -ErrorAction Stop | Select-String -Pattern "VG_IBOR_.*$($odate)" `
    | Where-Object {$_ -match "AMER"}

    if(!$positionFileNames) {
        Write-Log("Could not find positions files.")
        exit 1
    }

    Write-Log("Following files for processing: $($positionFileNames)")
} catch { 
    Write-Log("Failed to read $($inputPositionFilePath)")
    Write-Log("$($PSItem)")
    Exit 1
}

#Check File age here
#possibly move check file age to where odate is assigned. Right now it does nothing
foreach($fileName in $positionFileNames) {
    $isFromToday = Check-FileAge -fileToCheck "$($inputPositionFilePath)`\$($fileName)" -odate $today
    if($isFromToday -eq "True")
    {
        Write-Log("$($inputPositionFilePath)\$($fileName) is from today.")

    }else {
        Write-Log("$($inputPositionFilePath)\$($fileName) is too old")
        Write-Log("Please reach out to RMG Technology Solutions and ask them if the CRD->Factset export worked.")
        Exit 1
    }
}

#Get the most recent market neutral cash file
try {
    Write-Log("The input path $($polarisFilePath)")

    #If $odate is supplied, get that date. Otherwise set odate to the latest file date
    if(!$odate) {
        try {
            $newFile = Get-ChildItem "$($polarisFilePath)" -ErrorAction Stop | sort CreationTime -Descending | Select name `
            | Select-String -Pattern "POL_FUND_NUETRAL_.*$($odate)" | Select -First 1

            $odate = ($newFile -replace '\D+(\d+)','$1').Substring(0,8)
            Write-Log("Set odate to $odate")
        } catch {
            Write-Log("No polaris files in directory for type $signalType, odate not set")
            Write-Log("$($PSItem)")
            Exit 1
        }
    }

    $polarisFileNames = Get-ChildItem "$($polarisFilePath)" -name -ErrorAction Stop | Select-String -Pattern "POL_FUND_NUETRAL_.*$($odate)" `

    if(!$polarisFileNames) {
        Write-Log("Could not find polaris files.")
        exit 1
    }

    Write-Log("Following files for processing: $($polarisFileNames)")
} catch { 
    Write-Log("Failed to read $($polarisFilePath)")
    Write-Log("$($PSItem)")
    Exit 1
}

#Check File age here
#possibly move check file age to where odate is assigned. Right now it does nothing
foreach($polfileName in $polarisFileNames) {
    $isFromCurrentday = Check-FileAge -fileToCheck "$($polarisFilePath)`\$($polfileName)" -odate $today
    if($isFromCurrentday -eq "True")
    {
        Write-Log("$($polarisFilePath)\$($polfileName) is from today.")

    }else {
        Write-Log("$($polarisFilePath)\$($polfileName) is too old")
        Write-Log("Please reach out to RMG Technology Solutions and ask them if the CRD->Factset export worked.")
        Exit 1
    }
}

#Get the most transaction file
try {
    Write-Log("The input path $($transactionFilePath)")

    #If $odate is supplied, get that date. Otherwise set odate to the latest file date
    if(!$odate) {
        try {
            $newtransFile = Get-ChildItem "$($transactionFilePath)" -ErrorAction Stop | sort CreationTime -Descending | Select name `
            | Select-String -Pattern "VG_IBOR_AMER_EXT_D_EOD_Delta_Transactions_ALLInternalEquityPortfolios_Generic_.*$($odate)_.*" | Select -First 1

            $odate = ($newtransFile -replace '\D+(\d+)','$1').Substring(0,8)
            Write-Log("Set odate to $odate")
        } catch {
            Write-Log("No signal files in directory for type $signalType, odate not set")
            Write-Log("$($PSItem)")
            Exit 1
        }
    }

    $transactionFileNames = Get-ChildItem "$($transactionFilePath)" -name -ErrorAction Stop | Select-String -Pattern "VG_IBOR_AMER_EXT_D_EOD_Transactions_ALLInternalEquityPortfolios_Generic_.*$($odate)_.*" `

    if(!$transactionFileNames) {
        Write-Log("Could not find positions files.")
        exit 1
    }

    Write-Log("Following files for processing: $($transactionFileNames)")
} catch { 
    Write-Log("Failed to read $($transactionFilePath)")
    Write-Log("$($PSItem)")
    Exit 1
}

#Check File age here
#possibly move check file age to where odate is assigned. Right now it does nothing
foreach($transfileName in $transactionFileNames) {
    $isFromToday = Check-FileAge -fileToCheck "$($transactionFilePath)`\$($transfileName)" -odate $today
    if($isFromToday -eq "True")
    {
        Write-Log("$($transactionFilePath)\$($transfileName) is from today.")

    }else {
        Write-Log("$($transactionFilePath)\$(transfileName) is too old")
        Write-Log("Please reach out to RMG Technology Solutions and ask them if the CRD->Factset export worked.")
        Exit 1
    }
}

#Get funds from Funds+Benchmarks.csv
Write-Log("Parsing input CSV...")
try {
    $fundsCSV = "$($Global:fundFilePath)"
    Write-Log "Reading $fundType funds from $fundsCSV"

    #Imports the csv file. The first pipe filters based on the fundType that's passed in. The second pipe only selects Fund Column from the file
    $fundList = Import-Csv -Path $fundsCSV | Where-Object {$fundType -eq $_.Category} | Select ("Fund","NeoXamID","SingleImport")
    Write-Log("$($fundList.Fund)")
} catch {
    Write-Log("Failed to read $($Global:fundFilePath)")
    Write-Log("$($PSItem)")
    Exit 1
}

#Read the holdings file and create position objects for the accounts that were passed in
$distinctList = @()
foreach($fileName in $positionFileNames) {
    Write-Log("Loading CRD position data from $fileName...")
    try {
        $distinctList += Import-Csv "$($inputPositionFilePath)`\$($fileName)" -Delimiter ',' | Where-Object {($fundlist.NeoXamID).contains($_.Port_ID)}
        #Write-Log("Read CRD position data for $accounts")
    
    } catch { 
        Write-Log("Failed to read $($inputPositionFilePath)\$($fileName)")
        Write-Log("$($PSItem)")
        Exit 1
    }
}

#Read the holdings file and create position objects for the accounts that were passed in
$poldistinctList = @()
foreach($polfileName in $polarisFileNames) {
    Write-Log("Loading polaris cash data from $polarisFileNames...")
    try {
        $poldistinctList += Import-Csv "$($polarisFilePath)`\$($polfileName)" -Delimiter ',' | Where-Object {($fundlist.NeoXamID).contains($_.advisor_id)}
    
    } catch { 
        Write-Log("Failed to read $($polarisFilePath)\$($polfileName)")
        Write-Log("$($PSItem)")
        Exit 1
    }
}

#Read the holdings file and create transaction objects for the accounts that were passed in
$transdistinctList = @()
foreach($transfileName in $transactionFileNames) {
    Write-Log("Loading Transactions  cash data from $transfileName...")
    try {
        $transdistinctList += Import-Csv "$($transactionFilePath)`\$($transfileName)" -Delimiter ',' | Where-Object {($fundlist.NeoXamID).contains($_.Advisor_ID)}
    
    } catch { 
        Write-Log("Failed to read $($transactionFilePath)\$($transfileName)")
        Write-Log("$($PSItem)")
        Exit 1
    }
}

#Load SRM data
try {
    Write-Log("Getting SRM from $($Global:inputSRMFilePath)")
    $filesNamesToday = @()
    $fileNamesYesterday = @()
    $SRMFilesToCheckToday = @()
    $SRMFilesToCheckYesterday = @()

    #get file names that match the odate
    $fileNamesToday = Get-ChildItem "$Global:inputSRMFilePath" -name -ErrorAction Stop | Select-String -Pattern "nx_srm.*_$odate" `
    | Sort-Object -Descending
   
    foreach($filename in $filenamesToday) {
        $SRMFilesToCheckToday += "$($Global:inputSRMFilePath)\$($filename)"
    }     

    Write-Log("Following SRM files will be used: ($SRMFilesToCheckToday)")

    #find the most recent previous date
    $founddate = $false
    $lookback = 1
    while($founddate -eq $false -and $lookback -le 5) {
        $previousdate = (([datetime]::parseexact($odate, 'yyyyMMdd', $null)).addDays(-$lookback)).toString('yyyyMMdd')
                
        $fileNamesYesterday += Get-ChildItem "$Global:inputSRMFilePath" -name -ErrorAction Stop | Select-String -Pattern "nx_srm.*_$previousdate" `
        | Sort-Object -Descending

        if($fileNamesYesterday) {
            $founddate = $true
        } else {
            $lookback++
        }
    }

    foreach($filename in $filenamesYesterday) {
        $SRMFilesToCheckYesterday += "$($Global:inputSRMFilePath)\$($filename)"
    }     

    Write-Log("Following SRM files will be used: ($SRMFilesToCheckYesterday)")

} catch {
    Write-Log("Failed to load SRM from $($Global:inputSRMFilePath)")
    Write-Log("$($PSItem)")
    Exit 1
}

Write-Log("Doing vnum lookup for $odate")
$SRMToday = @{}
$SRMToday = Create-SRM-Objects($SRMFilestoCheckToday)

Write-Log("Doing vnum lookup for $previousdate")
$SRMYesterday = @{}
$SRMYesterday = Create-SRM-Objects($SRMFilestoCheckYesterday)

$positionObjects = @()
Write-Log("Creating postion objects...")
#$total = $distinctList.Length
#$count = 0
foreach($p in $distinctList)
{
    #Set up position
    $position = New-Position
            
    #From fund mapping
    $PortID = $fundlist | Where-Object {($_.NeoXamID).contains($p.Port_ID)}
    $position.acctCd = $PortID.Fund

    #From positions
    $position.qtyEod = $p.Quantity
    $position.currency = $p.Local_Currency
    $position.marketValueLocalCurrency = $p.Market_Value_Local_Currency #for forwards
    $position.primaryAssetID = $p.Primary_Asset_ID #for swaps
    $position.baseCurrency = $p.Base_Currency #for swaps
    $position.marketValueBaseCurrency = $p.Market_Value_Base_Currency #for swaps
    $position.secType = $p.NX_Security_Type
    $position.vnum = $p.Vanguard_ID_VID
    $position.legTypeCode = $p.Leg_Type_Code
    $position.secMainType = $p.Security_Main_Type
    $position.secshrtdesc = $p.SEC_NAME
    $position.ls_indicator  = $p.Long_Short_Indicator

    #Total return swaps
    if(("CT.TOTRETSWAP") -contains $position.secType) {
        if($position.legTypeCode -match "R") { 
            $position.assetID = "$($position.primaryAssetID)_$($position.BaseCurrency)"
            $position.qtyEod = $position.marketValueBaseCurrency
            $positionObjects += $position
        } else {
            #Ignore other legs
        }
    #currency forwards and cash positions
    } elseif (("CT.FOREX","CT.NDF") -contains $position.secType) { 
        $position.assetID = "CSH_$($position.currency)"
        $position.qtyEod = $position.marketValueLocalCurrency
        $positionObjects += $position

    #regular positions
    } else { 
        try {
            #From SRM
            $sec = @()
            $sec = $SRMToday.Get_Item($position.vnum)

            if($sec[$POS_SECDATE] -ne $odate) {
                $sec = $SRMYesterday.Get_Item($position.vnum)
            }

            if($sec[$POS_SECDATE] -eq $odate -and $sec[$POS_VNUM] -eq $position.vnum ) { 
                $position.assetId = $sec[$POS_ASSETID]
                $position.secType = $sec[$POS_SECTYPE]
                $position.secName = $sec[$POS_SECNAME]
                $position.vnum = $sec[$POS_VNUM]
                $position.date = $sec[$POS_SECDATE] #remove later

                #$position.ticker = $p.Identifier.Split()[0]
                <#
                if($position.secType -eq "MM.TBILL"){
                    Write-Log("Counting $($position.assetId) as Cash USD")
                    $position.assetId = "CSH_USD"
                }
                #>
                $sedol = $sec.assetID
    
                #If the Identifier has "Index" at the end of it, this will remove it.
                try{
                $sedolsWeCareAbout += $sedol.SubString(0, $sedol.LastIndexOf(' '))
                $position.ticker = $sedol.SubString(0, $sedol.LastIndexOf(' '))
                } catch {
                    $sedolsWeCareAbout += $p.Identifier
                    $position.ticker = $p.Identifier
                }

                #Add new Position object to list
                $positionObjects += $position 
            } else {
                if($p.Vanguard_ID_VID) {
                    Write-Log("No matching SRM for $odate or $previousdate for $($p.Vanguard_ID_VID) - skipping")
                } else {
                    #Write-Log("No VID for position of type $($p.NX_Security_Type) - skipping")
                }
            }
        } catch {
            if($p.Vanguard_ID_VID) {
                Write-Log("No matching SRM for $odate or $previousdate for $($p.Vanguard_ID_VID) - skipping")
            } else {
                #Write-Log("No VID for position of type $($p.NX_Security_Type) - skipping")
            }
        }
    }
}#End of for loop over distinct list

##Load Polaris cash data
$polObjects = @()
Write-Log("Creating Polaris cash objects...")

foreach($c in $poldistinctList | Where-Object {($_.advisor_id -eq "AAVT") -And ($_.attribute_code -eq "TNA")})
{

$cash = New-Cash
           $cash.amount = $c.amount
           $polObjects += $cash

}

##Load Transaction data
$transObjects = @()
Write-Log("Creating Transaction objects...")

$t = $transdistinctList | Where-Object {($_.Advisor_ID -eq "AAVT" -and $_.Transaction_Type_Description_Code -eq "CASHDIV")}
$curr = ($t.Corporate_Action_Market_Value_Base_Currency | Measure-Object -Sum).Sum

foreach ($t in $transdistinctList | Where-Object {($_.Advisor_ID -eq "AAVT" -and $_.Transaction_Type_Description_Code -eq "CASHDIV")}) {
   $transaction = New-Transaction
           
   $transaction.nettradecurr = $t.Net_Trade_Amount__Base_Currency
   $transaction.transdesccode = $t.Transaction_Type_Description_Code
   $transaction.code = $t.Long_Short_Indicator
   $transaction.curr = $curr
   $transObjects += $transaction 
}

# Perform operations on IDX FUT
# if assetclass == Index Future, then do Holdings math

$indexPositionObjects = $positionObjects | ConvertTo-Csv -NoTypeInformation | ConvertFrom-Csv

#Index Futures
Write-Log("Multiplying Index Futures....")
foreach($p in $indexPositionObjects | Where-Object {$_.SecType -eq "DE.IND" -or $_.SecType -eq "DE.COMM"})
{
    #From SRM
    $srm = @()
    $srm = $SRMToday.Get_Item($p.vnum)

    if($srm[$POS_SECDATE] -ne $odate) {
        $srm = $SRMYesterday.Get_Item($p.vnum)
    }

    if($srm[$POS_SECDATE] -eq $odate -and $srm[$POS_VNUM] -eq $p.vnum ) { 
        Write-Log("Computing FUT QTY for SEDOL/CUSIP: $($p.assetId) TICKER: $($srm[$POS_TICKER]) using contract size of $($srm[$POS_CONTRACTSIZE])")
        Write-Log("and the secType is $($p.sectype)")
        $intqty = [int]$p.qtyEod
        $intMultiplyer = [int]$srm[$POS_CONTRACTSIZE]
        $p.qtyEod = $intqty#/$intMultiplyer

        #Parse the ticker here
        $p.ticker = Parse-Ticker -t $srm[$POS_TICKER] -sn $srm[$POS_SECNAME]
        $p.assetId = $p.ticker
    } else {
        Write-Log("No SRM data found for $($p.assetID)`n")
    }
}

#Read in Mapping file
Write-Log("Reading in the Mappings File from $($Global:mappingFile)`n")
try{
$mappings = Import-Csv "$($Global:mappingFile)" -Delimiter ','
}catch {
    Write-Log("unable to read the mappings file")
    throw
}

#Create Mapping objects and add to collection
$mappingObjects = @()
foreach($m in $mappings)
{
    $map = New-Mapping
    $map.rootTicker = $m.rootTicker
    $map.exchSymbol = $m.exchSymbol
    $mappingObjects += $map
}

#$mappingObjects | ConvertTo-Json | Out-File ".\mappingObjects.json"

#Export to CSV
Write-Log("Generating .csv file")

$outputFileName = "$($fundType)_$($odate).csv"
    
$fileToWrite = $indexPositionObjects | Select-Object -Property acctCd, odate, assetId, qtyEod | Sort-Object -Property acctCd
    
#Loop over each line in the final output file
foreach($line in $fileToWrite)
{
    #Get the Asset ID as a ticker
    $ticker = $line.assetID
        
    #Get the first part of the ticker before the hyphen
    try{
        #Many of the tickers don't have a hyphen so indexOf returns -1 and and we get an exception from the substring method. Hence the try/catch.
        $toReplace = $ticker.substring(0, $ticker.indexOf("-"))
            
        $m = $mappingObjects | Where-Object {$_.rootTicker -eq "$toReplace"} #| Select-Object -Property exchSymbol
        $replaceWith = $m.exchSymbol

        if($replaceWith -ne $null)
        {
            Write-Log("replacing $($toReplace) with $($replaceWith) in the ticker: $($ticker)")
            $ticker = $ticker.replace("$($toReplace)", "$($replaceWith)")

            $line.assetID = $ticker
        } 
    }catch{
        #Do nothing if there's nothing to replace
    }
}#end of for-loop over output file


try {
    Write-Log("Writing file for $($fundType) ...")
    #don't use Export-Csv its slow and adds double quotes to the file that you have to remove later.
    $fileToWrite | ConvertTo-Csv -NoTypeInformation | Select -Skip 1 | ForEach-Object { $_ -replace '"' } | Out-File "$($outputFilePath)\$($outputFileName)" -Encoding ASCII

    #Export individual files where specified in Funds+Benchmarks.csv
    foreach($singleFile in ($fundList | Where {$_.singleImport})) {
        Write-Log("Writing file for $($singleFile.Fund) ...")
        $fileToWrite | Where {$_.acctCd -eq $singlefile.Fund} | Select-Object -Property assetId, qtyEod | ConvertTo-Csv -NoTypeInformation | Select -Skip 1 | ForEach-Object { $_ -replace '"' } `
        | Out-File "$($outputFilePath)\$($singleFile.Fund)_$($odate).csv" -Encoding ASCII
    }

} catch {
    Write-Log("Failed to write to $($outputFilePath)\$($outputFileName)")
    Write-Log("$($PSItem)")
    throw
}

Write-Log("Script Complete")
Exit 0

<#
This script will optimize a Purview CDS export for RelOne ingestion by doing the following:
	1) Extract the zip(s)
	2) Import key fields from load file(s)
	3) Identify and suppress descendants of Cloud Attachments, segregating and deleting the extracted copies
		a) Items where $_.Input_file_id -ne $_.File_id
	4) Decrypt any AIP-protected items
    5) Lookup custodian Last, First values
	6) Replace meeting recording json transcripts with versions optimized for review
		a) The script will detect, extract, and reformat the transcript json, then delete it from the zip.
		b) The reformatted transcript txt will be named "[file_id of parent zip] [json filename]_transcripttext.txt" and will be moved to the Custodian_LastFirst folder.
		c) A new row will be added to the load file to facilitate overlay.
		d) The extracted json is moved to a subdirectory called "SUPPRESS."
	7) Create subfolders of custodian Last, First values
	8) Move actual top-level items into custodian folders, recreating the native directory structure but leaving the filenames as the Purview file_ids.
	9) Move non-top-level items to a subdirectory called "SUPPRESS"
    10) Generate streamlined csv containing just the metadata necessary for overlay, which can be matched via the Relativity native file name.

#>

Write-host "Start Time`: $(get-date)"

$7z = "C:\Program Files\7-Zip\7z.exe"

if (!(Test-Path $7z)) {
    Write-host -ForegroundColor Red "Please install 7zip first. It must be installed at C:\Program Files\7-Zip\7z.exe.`r`n`r`nYou can download it from https://www.7-zip.org/download.html`r`n"
    Read-host "Hit Enter to exit"
    Break
}

Set-Alias 7z $7z


$ExportRoot = (Read-host "Enter the directory containing your export file(s)").trim('"')

$jobID = (Get-Date).tostring("yyyyMMddHHmmss")
$transcriptLocation = "$($ExportRoot)/$($jobID) Prep Exodia Output for RelOne import transcript.txt"
Start-Transcript $transcriptLocation | Out-Null

$HaikuTranslation = Read-host "Is this a Haiku translation job? Y/N"

if ($HaikuTranslation -eq 'Y') {
    $HaikuPath = (Read-host "Enter the path to the Haiku load file").trim('"')
    $Haiku = import-csv $haikuPath -Encoding UTF8
    if (!$Haiku.count) {
        Read-host "Something is wrong. You should hit ctrl+c to quit and get help"
    }
} else {

    Write-host -foregroundColor Cyan "`r`nThis will process all zips at $($exportroot), extracting them to the that exact directory. `r`n`r`nPlease make sure there are no undesired zips in that location.`r`n"
    Read-host "Hit Enter to continue"

    if ($ExportRoot -like "*.zip") {
        $ExportRoot = $ExportRoot -replace "\\[^\\]*$" -replace "\/[^\/]*$" 
    }

    $zips = @(gci $ExportRoot -file -Filter *.zip)

    # Validate drive space
    $i = 1
    $SpaceNeeded = 0
    foreach ($zip in $zips) {
        Write-host -ForegroundColor Gray "Verifying sufficient disk space to extract the zip`: $($i) of $($zips.count)"
        if ($zipReport) {clv zipReport}
        $zipReport = 7z l $zip.fullname
        [System.Int64]$totalSize = 0
        foreach ($line in $zipReport) {
            if ($line -match "^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+\.\.\.\.\.\s+(\d+)\s+\d+\s+.+$") {
                $fileSize = [System.Int64]$matches[1]
                # Debug Jon's issue
                Write-host -ForegroundColor Cyan "FileSize = $($fileSize)`r`n0+FileSize`: $(0 + $filesize)`r`n`r`n"
                $totalSize += $fileSize
            }
        }
        $SpaceNeeded += $totalSize 
        $i++
    }

    $AvailableCapacity = (Get-WmiObject Win32_LogicalDisk -ComputerName $($env:COMPUTERNAME) -Filter "DeviceID='$($ExportRoot -replace "\\.*")'" | Select-Object FreeSpace).FreeSpace
    if ($AvailableCapacity - $SpaceNeeded  -lt 1000000000) {
        Write-Host -ForegroundColor Red "`r`nInsufficient drive space on $($ExportDir)`r`n`tFREE SPACE`: $($AvailableCapacity) ($("{0:N2}" -f ($AvailableCapacity/1gb))GB)`r`n`tREQUIRED SPACE`: $($SpaceNeeded) ($("{0:N2}" -f ($SpaceNeeded/1gb))GB)"
        Read-host "Move these files to a drive with adequate capacity then rerun the script.`r`n`r`nHit Enter to exit"
        Break
    } else {
        Write-Host -ForegroundColor Green "`r`Sufficient drive space at $($ExportRoot)`r`n`tFREE SPACE`: $($AvailableCapacity) ($("{0:N2}" -f ($AvailableCapacity/1gb))GB)`r`n`tREQUIRED SPACE`: $($SpaceNeeded) ($("{0:N2}" -f ($SpaceNeeded/1gb))GB)"
    
    }

    # Sign in to enable decryption
    Write-host -ForegroundColor Cyan "`r`nSign in with your SC-ALT to enable decryption"
    Set-AIPAuthentication

    # Extract zips
    foreach ($z in $zips) {
        if ($zips.count -gt 1) {Write-host -ForegroundColor Gray "Unpacking archive $($zips.indexOf($z)+1) of $($zips.count)`: $($z.name)"} else {Write-host -ForegroundColor Gray "Unpacking archive`: $($z.name)"}
        & 7z x $($z.fullname) -o"$($ExportRoot)" -y #Added for script to continue even if there are dupes in natives folder in multi-part zip
    }
}

# Identify and ingest load file(s)
$LoadFiles = @(GCI -Path $ExportRoot -File -Filter *.csv | where{$_.BaseName -like "Export_Loadfile*"})
$i = 1
$data = @()
foreach ($LoadFile in $LoadFiles) {
    Write-host -ForegroundColor Gray "Importing load file $($i) of $($loadfiles.count)"
    $data += import-csv $LoadFile.fullname -Encoding UTF8 | select File_ID, `
        Input_File_ID, `
        Custodian, `
        Location, `
        Location_Name, `
        File_Class, `
        Native_Extension, `
        Export_Native_Path, `
        Compound_Path, `
        Date, `
        Email_Participants, `
        Email_Participant_Domains, `
        Email_subject, `
        Error_code, `
        Group_Id, `
        IsModernAttachment, `
        Item_class, `
        ModernAttachment_ParentId, `
        Native_file_name, `
        Native_size, `
        Tags, `
        Native_MD5, `
        Native_Sha_256, `
        Parent_ID, `
        Family_ID, `
        Immutable_ID
    $i++
}

# Check for items to decrypt and decrypt them
#Get-AIPFileStatus $($ExportRoot) | ?{$_.IsRMSProtected -eq 1 -and $_.filename -notmatch "Extracted_Text_Files"} | Set-AIPFileLabel -RemoveProtection -JustificationMessage "EDISCOVERY" -ErrorAction Continue
# Get-FileStatus $($ExportRoot) | ?{$_.IsRMSProtected -eq "True" -and $_.filename -notmatch "Extracted_Text_Files"} | Set-FileLabel -LabelId "f42aa342-8706-4288-bd11-ebb85995028c" -JustificationMessage "EDISCOVERY" -ErrorAction Continue


<#
# Suppress extracted children
filter Suppress-Children {
    if (($_.isModernAttachment -eq "TRUE" -and $_.file_id -ne $_.input_file_id) -or ($_.file_class -eq "Attachment" -and $_.isModernAttachment -eq "FALSE")) {$_}
}

Write-host -ForegroundColor Gray "Identifying extracted child items to suppress"
$data | Suppress-Children | Add-Member NoteProperty -Name Suppress -Value "1"
#>

# Name NCDS items
Write-host -ForegroundColor Gray "Starting Custodian/NCDS Name Normalization"
$NCDS = $data.where{!$_.custodian}
$NCDS | Add-member NoteProperty -Name NCDS -Value 1
    
# Handle issue where Location_Name value is blank for SP sources
if ($NCDS.count -gt 1 -and $NCDS.where{!$_.location_name}) {
    Write-Progress -Activity "Fixing location_name values for items without values" -id 1
    Write-host -ForegroundColor Gray "Fixing location_name values for items without values. Start time`: $(get-date)" 
    $NCDS.where{!$_.location_name} | %{$_ | Add-Member NoteProperty -Name Location_Name -Value $($_.Compound_path -replace "https://microsoft.sharepoint.com/" -replace "https://microsoft.sharepoint-df.com/" -replace '^(.*?)\/' -replace "\/.*") -Force}
    $NCDSnames = $($NCDS.location_name | sort -Unique | ?{$_})
}

$NCDSnames = $($NCDS.location_name | sort -Unique | ?{$_})

filter lcfilter {
    param ( 
        [string] $lc 
    ) 

    if ($_.location_name -eq $lc) {$_}
}

$NCDSmap = @()
foreach ($N in $NCDSnames) {
    if ($newname) {clv newname}
    Write-host -ForegroundColor Yellow "`r`nSource`: $($N)`r`n`r`nProposed Name`: $($N -replace "https://microsoft.sharepoint.com/teams/" -replace "https://microsoft.sharepoint.com/sites/" -replace "https://microsoft.sharepoint-df.com/teams/" -replace "https://microsoft.sharepoint-df.com/sites/")`r`n"
    $newname = Read-host "Enter another name, if appropriate, or hit Enter to continue.`r`n"
    Write-host -ForegroundColor DarkGray "newname`: $($newname)"
    if (!$newname) {$newname = $($N -replace "https://microsoft.sharepoint.com/teams/" -replace "https://microsoft.sharepoint.com/sites/" -replace "https://microsoft.sharepoint-df.com/teams/" -replace "https://microsoft.sharepoint-df.com/sites/")}
    $NCDSmap += New-object psobject -Property @{
        Location_Name = $N;
        Custodian = $newname
    }
}

if ($NCDS) {
    Write-host -ForegroundColor Yellow "`r`nNCDS Mapping`:"
    $NCDSmap | Sort -Property Custodian | ft Custodian,Location_Name

    Read-host "Hit Enter to proceed, or ctrl+c to stop and restart the script if there are any errors"

    foreach ($N in $NCDSmap) {
        Write-Progress -Activity "Updating $($NCDSmap.indexOf($N)+1) of $($NCDSmap.count) NCDS sources" -id 1
        Write-host -ForegroundColor Gray "Updating $($NCDSmap.indexOf($N)+1) of $($NCDSmap.count) NCDS sources. Start time`: $(get-date)" 
        if ($NCDSgroup) {clv NCDSgroup}
        $NCDSgroup = $NCDS | lcfilter -lc $N.Location_Name 
        $NCDSgroup | Add-member NoteProperty -Name Custodian -Value $($N.Custodian -replace "\/","_") -Force
        $NCDSgroup | Add-member NoteProperty -Name Custodian_LastFirst -Value $($N.Custodian -replace "\/","_") -Force
    }
}

# Handle content for ncds@microsoft.com
$data.where{$_.custodian -eq "ncds@microsoft.com"} | Add-member NoteProperty -Name NCDS -Value 1
foreach ($1 in $data.where{$_.custodian -eq "ncds@microsoft.com" -and $_.tags -match "custodian"}) {
    $1 | Add-Member NoteProperty -Name Custodian_LastFirst -Value $(($($1.tags -replace ",custodian.*" -split ',').TrimEnd().TrimStart() | Select -last 1) -replace "\/","_") -force
}
$data.where{$_.custodian -eq "ncds@microsoft.com" -and $_.tags -notmatch "custodian"} | Add-Member NoteProperty -Name Custodian_LastFirst -Value "NCDS" # If there is no Custodian tag, default to "NCDS"
##########

##### Generate Custodian Last, First format values
$custodians = @($data.where{$_.ncds -ne 1}.custodian | sort -Unique | ?{$_})
$deduped_custodians_table = @()
if ($custodians) {
    # Connect to EXO
    try {
        \\lca-lit\litigation\EDISCOVERY\SCRIPTS\PSConnection\Connect_to_EXO.ps1
    } catch {
        Connect-ExchangeOnline
    }
    
    foreach ($custodian in $custodians) {
        Write-Progress -Activity "Looking up Custodian Last, First format values" -Status "$($custodians.indexof($custodian)+1) of $($custodians.count)`: $($custodian)" -id 1 -ErrorAction SilentlyContinue
        if ($c) {clv c}
        if ($lastfirst) {clv lastfirst}
        $c = get-recipient $($custodian.TrimStart(".")) -IncludeSoftDeletedRecipients | select -first 1
        if ($c.firstname) {
            $lastfirst = $((($c.LastName -replace '\(.*' -replace '\[.*').trim()) + ', ' + $($c.FirstName))
        } else {
            $lastfirst = Read-host "Cannot find $($custodian) in EXO. Please enter the name in Last, First format"
            Write-host -ForegroundColor DarkGray "lastfirst`: $($lastfirst)"
        }
        if ($custodian -like "lit0*" -and $lastfirst -notmatch ', ') {
            $lastfirst = Read-host "Please enter the appropriate name for this Litigation Archive mailbox in Last, First format`: $($custodian)"
            Write-host -ForegroundColor DarkGray "lastfirst`: $($lastfirst)"
        }
        if ($data | where {$_.custodian -eq $custodian -and $_.Custodian_LastFirst}) {$lastfirst = $(($data | where {$_.custodian -eq $custodian -and $_.Custodian_LastFirst} | select -first 1).Custodian_LastFirst)}
        $deduped_custodians_table += new-object psobject -Property @{
            UserPrincipalName = $custodian;
            Custodian_LastFirst = $lastfirst
        }
    }

    Get-PSSession | Remove-PSSession
}
# Add Custodian_LastFirst value
foreach ($custodian in $deduped_custodians_table.where{$custodians -contains $_.UserPrincipalName}) {
    $data.where{$_.custodian -eq $custodian.UserPrincipalName} | Add-Member NoteProperty -Name Custodian_LastFirst -Value $($custodian.Custodian_LastFirst)
}

# Define the illegal characters and the replacement character
$illegalChars = [RegEx]::Escape('\:*?"<>|')
#$replacementChar = '_'
<#

%
&
+
=
[
]
{
}

#>

# Function to replace illegal characters in a given path name
function Replace-IllegalCharacters {
    param (
        [string]$path
    )

    # Replace illegal characters with the replacement character
    $sanitizedPath = $path -replace "[$illegalChars]" #, $replacementChar

    return $sanitizedPath
}

function Get-LocalSharePointPath {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Url
    )
    Add-Type -AssemblyName System.Web

    # 1) Decode the entire URL to handle HTML entities (&amp;, etc.).
    $decodedUrl = [System.Web.HttpUtility]::HtmlDecode($Url)

    # 2) Validate that this is a SharePoint-style URL (including *.my.sharepoint.com).
    if ($decodedUrl -notmatch '^https?://[\w\-\.]+\.sharepoint\.com' -and $decodedUrl -notmatch '^https?://[\w\-\.]+\.sharepoint-df\.com') {
        throw "Not a valid SharePoint URL: $Url"
    }

    # 3) Remove any '#' fragment if present.
    $decodedUrl = $decodedUrl -split '#' | Select-Object -First 1

    # 4) Strip off everything after '?'.
    $noQueryUrl = $decodedUrl -split '\?' | Select-Object -First 1

    # 5) Convert that portion into a [Uri] object.
    $uri       = [Uri]$noQueryUrl
    $scheme    = $uri.Scheme
    $hostname  = $uri.Host

    # 6) Decode the absolute path and remove any trailing slash.
    $path = [System.Web.HttpUtility]::UrlDecode($uri.AbsolutePath).TrimEnd('/')

    # 7) Handle short-link patterns like /:x:/t/<site>, /:f:/p/<user>, etc.
    if ($path -match '^/:[abcdefghijklmnopqrstuvwxyz]{1,2}:/(?<letter>[abcdefghijklmnopqrstuvwxyz])/(?<name>[^/]+)') {
        $letter = $matches['letter']
        $name   = $matches['name']
        $path   = "/$letter/$name"
    }

    # 8) If there's a file extension at the end of the path, remove it.
    if ($path -match '/[^/]+\.[A-Za-z0-9]{1,10}$') {
        $path = $path -replace '/[^/]+\.[A-Za-z0-9]{1,10}$', ''
        $path = $path.TrimEnd('/')
    }

    # 9) Decide whether to keep the scheme in the output.
    #    As per examples, many URLs with '-my.sharepoint.com' omit the "https://"
    $useScheme = $true
    if ($hostname -like '*-my.sharepoint.com' -or $hostname -like '*-my.sharepoint-df.com') {
        $useScheme = $false
    }

    # 10) Construct the final path, avoiding the PowerShell read-only $host variable.
    #     We'll store the result in $finalPath.
    if ($useScheme) {
        $finalPath = "$($scheme)://$($hostname)$($path)"
    } else {
        # Omit the scheme
        $finalPath = "$($hostname)$($path)"
    }

    # 11) Replace any backslashes with forward slashes.
    $finalPath = $finalPath -replace '\\', '/'

    # 12) Remove extra consecutive slashes, but don't break "https://".
    $finalPath = $finalPath -replace '(?<!(?:http|https|file|ftp)):/{2,}', '/' -replace "^https://"

    # 13) Return the final path, trimming any trailing slash.
    return $finalPath.TrimEnd('/')
}

if ($Haiku) {
    # Rename natives and extracted text files with File ID values if this is a Haiku
    $i = 1
    $total = $data.count
    foreach ($1 in $data) {
        Write-host -ForegroundColor Gray "Renaming Haiku files $($i) of $($total)`: $($1.file_id)"
        $haikuMatch = $haiku | where{$_."File ID" -eq $1.file_id}
        if ($($HaikuMatch.'target path' -replace '^(.*[\\])') -match "[a-zA-Z0-9]" -and (test-path "$($ExportRoot)/NativeFiles/$($HaikuMatch.'target path' -replace '^(.*[\\])')")) {
            Rename-Item -Path "$($ExportRoot)/NativeFiles/$($HaikuMatch.'target path' -replace '^(.*[\\])')" -NewName "$($1.file_id).$($1.native_extension)"
        }
        if ($($HaikuMatch.'extracted text path' -replace '^(.*[\\])') -match "[a-zA-z0-9]" -AND (test-path "$($ExportRoot)/Extracted_Text_Files/$($HaikuMatch.'extracted text path' -replace '^(.*[\\])')")) {
            Rename-Item -Path "$($ExportRoot)/Extracted_Text_Files/$($HaikuMatch.'extracted text path' -replace '^(.*[\\])')" -NewName "$($1.file_id)_text.txt"
        }
        $i++

    }

} else {
    # Generate a list of natives actually exported
    Write-host -ForegroundColor Cyan "Confirming expected natives exist in the $($ExportRoot)\NativeFiles directory"
    # Build hash set for ActuallyExported basenames
        $ActuallyExported = gci "$($exportRoot)/NativeFiles" -file -Recurse
		$exportedBaseNames = @{}
		foreach ($file in $ActuallyExported) { $exportedBaseNames[$file.BaseName] = $true }
		$DownloadFailures = $Data | Where-Object { -not $exportedBaseNames.ContainsKey($_.file_id) }
    if ($DownloadFailures) {
        Write-host -foregroundcolor Magenta "Looks like at least one native failed to export. Failure count`: $(@($DownloadFailures.count))."
        $DownloadFailures | Select * | Export-csv "$($ExportRoot)\Export_Loadfile_$($JobID)_Download_Failures.csv" -encoding UTF8 -notypeinformation
    }
}

# Decrypt items with an "RPMSG" indication in the Tags value
Write-host -ForegroundColor Cyan "Checking for items that need to be decrypted..."
$ToDecrypt = $Data | where{$_.Tags -match "rpmsg"}
if ($ToDecrypt) {
    $ToDecryptPaths = $ActuallyExported | where{$ToDecrypt.File_ID -contains $_.basename}
    if ($ToDecryptPaths) {
        Write-Host -ForegroundColor Cyan "Found $(@($ToDecryptPaths).count) item(s) to decrypt."
        foreach ($1 in $ToDecryptPaths) {
            Write-Host -ForegroundColor DarkCyan "Decrypting $($1.file_id)"
            Remove-Filelabel -File $1.fullname -RemoveProtection -ErrorAction Continue
        }
    }
}


# Generate simplified transcripts
filter Find-MeetingZips {
    if ($_.compound_path -like "*Meeting Recording.zip") {$_}
}
Write-host -ForegroundColor Gray "Identifying Meeting Zips to process"
$MeetingZips = @($data | Find-MeetingZips)

if ($MeetingZips) {
    Write-host -ForegroundColor Gray "Found $($MeetingZips.count) Meeting Zips to process"
    $i = 1
    $total = $MeetingZips.count
    foreach ($M in $MeetingZips) {
        Write-host -ForegroundColor Gray "Processing Meeting Zip $($i) of $($total)`: $($M.file_id)"
        if (test-path "$($ExportRoot)\$($M.Export_native_path)") {
            if ($transcriptPath) {clv transcriptPath}
            $transcriptPath = (7z l "$($ExportRoot)\$($M.Export_native_path)") -split " " | ?{$_ -like "*transcript*.json"}
            if ($transcriptPath) {
                Write-host -ForegroundColor DarkCyan "Found transcript json`: $($transcriptPath)"
                7z e "$($ExportRoot)\$($M.Export_native_path)" $($transcriptPath) -o"$($ExportRoot)" -y
                if (test-path "$($ExportRoot)\$($transcriptPath -replace '^(.*[\\])')") {
                    $isJSON = $false
                    $transcriptJSON = Get-Content "$($ExportRoot)\$($transcriptPath -replace '^(.*[\\])')"
                    if($transcriptJSON -like 'WEBVTT*') {
                        $transcriptText = $transcriptJSON
                    }
                    else {
                        $transcriptJSON = $transcriptJSON |ConvertFrom-Json
                        $isJSON = $true
                    }
              
                    if($isJSON) {
                        $transcriptArray = @()
                        $transcriptJSON.entries | ForEach-Object {
                            $transcriptArray += New-Object psobject -Property @{
                              "SpeakerDisplayName" = $_.SpeakerDisplayName;
                              "StartOffset" = $_.StartOffset;
                              "Text" = $_.Text
                            }
                        }
                      $transcriptJSON.events | ForEach-Object{
                            $transcriptArray += New-Object psobject -Property @{
                              "UserDisplayName" = $_.UserDisplayName;
                              "eventType" = $_.EventType;
                              "StartOffset" = $_.StartOffset
                            }
                        }
                      $transcriptText = ""
                      $transcriptArray | Sort-Object -Property startOffset | ForEach-Object{
                            if ($_.SpeakerDisplayName) {
                              $transcriptText += "$($_.SpeakerDisplayName)`r`n$($_.startOffset)`r`n$($_.text)`r`n`r`n"
                            } else {
                              $transcriptText += "$($_.UserDisplayName)`r`n$($_.startOffset)`r`n$($_.eventType)`r`n`r`n"
                            }
                        }
                    }
              
                    $transcriptText | Out-File "$($ExportRoot)\$($m.File_ID) $($transcriptPath -replace '^(.*[\\])')_transcripttext.txt"
                }

                # If successful, delete the original json from the zip and insert the new transcript
                if ($verification) {clv verification}
                $verification = gci $($ExportRoot) -filter "$($m.File_ID) $($transcriptPath -replace '^(.*[\\])')_transcripttext.txt"
                if ($verification.Length -gt 0) {
                    Write-host -ForegroundColor DarkGreen "Transcript generation successful. Deleting original json and adding new transcript to the zip."
                    [System.IO.File]::SetCreationTime($verification.FullName, [datetime]($M.Date))
                    [System.IO.File]::SetLastWriteTime($verification.FullName, [datetime]($M.Date))
                    7z d "$($ExportRoot)\$($M.Export_native_path)" $transcriptPath
                    #7z a "$($ExportRoot)\$($M.Export_native_path)" "$($verification.fullname)" -si"$($verification.name)" -mx=1 # IT TAKES WAY TOO LONG TO ADD THE FILE BACK TO THE ZIP
                    $data += new-object psobject -property @{
                        File_ID = $verification.name;
                        Group_id = $($M.Group_id);
                        Location_name = $m.Location_name;
                        Compound_path = "$($m.Compound_path)/$($transcriptPath).txt";
                        Date = $m.date;
                        IsModernAttachment = $m.IsModernAttachment;
                        ModernAttachment_ParentId = $m.ModernAttachment_ParentId;
                        NCDS = $m.NCDS;
                        Custodian = $m.Custodian;
                        Custodian_LastFirst = $m.Custodian_LastFirst;
                        Input_file_id = $m.File_id;
                        File_class = $m.File_class;
                        Native_extension = "txt"
                    }
                    if (!(test-path "$($ExportRoot)\$($m.Custodian_LastFirst)")) {
                        New-item -ItemType Directory -Path $ExportRoot -Name $($m.Custodian_LastFirst)
                    }
                    #Move-Item -Path $verification.FullName -Destination "$($ExportRoot)\$($m.Custodian_LastFirst)"
                    if ($DestPath) {clv DestPath}
                    #$DestPath = Replace-IllegalCharacters $M.Compound_Path -replace "https//" -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?'
                    try {
                        $DestPath = "$(Get-LocalSharePointPath -Url $M.Compound_path)/$($M.native_file_name)"
                    } catch {
                        $DestPath = (Replace-IllegalCharacters $(($M.Compound_Path -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*") -replace "https//").trimEnd().trimEnd("/"))
                    }
                    Write-host -ForegroundColor Gray "Compound_Path`:$($M.Compound_path)`r`n`r`nDestPath`: $($DestPath)`r`n`r`n"
                    $Robocopy = Robocopy "$($verification.FullName -replace "\\[^\\]*$" )" "$($ExportRoot)\$($m.Custodian_LastFirst)\$($m.Location)\$($DestPath)" $verification.Name /mov | Out-Null

                    #Read-host "Want to debug`?"
                }
            }
        }
        $i++
    }

    # Move extracted jsons to SUPPRESS directory
    robocopy $ExportRoot "$($ExportRoot)\SUPPRESS\ProcessedMeetingJsons" *.json /mov | Out-Null
}



# Move top-level items to Custodian_LastFirst folders, recreating the original directory structure but leaving the native file names as the Purview File_ID values
Write-host -ForegroundColor Gray "Moving top-level items to Custodian subdirectories"
#$uploadScope = $data | where{!$_.SUPPRESS -and $_.Export_native_path}
$uploadScope = $data | where{$_.Export_native_path}
$uploadCount = $uploadScope.count
$i = 1
$ItemsToCheck = @()
$CopyFailures = @()
foreach ($1 in $uploadScope) {
    Write-host -ForegroundColor Gray "Moving $($i) of $($uploadCount)`: $($1.file_id)"
    if ($Dest) {clv Dest}
    # If there is no Custodian_LastFirst value, move the files to a subfolder called "Microsoft"
    if ($1.Custodian_LastFirst) {$Dest = $1.Custodian_LastFirst} else {$Dest = "Microsoft"}
    if (!(test-path "$($ExportRoot)\$($Dest)")) {
        New-item -ItemType Directory -Path $ExportRoot -Name $Dest
    }
    if (test-path "$($ExportRoot)/$($1.Export_native_path -replace '.loop','.loop.html' -replace '.whiteboard','.whiteboard.html')") {
        #Move-Item -Path "$($ExportRoot)/$($1.Export_native_path)" -Destination "$($ExportRoot)/$($Dest)"
        if ($DestPath) {clv DestPath}
        if ($1.File_class -eq "Conversation") { 
            $DestPath = "$($1.Location_name)/M365 Services/Teams/Exchange"
        } else {
            if ($1.Location -eq "SharePoint") {
                try {
                    $DestPath = $(Get-LocalSharePointPath -Url $1.Compound_path)
                } catch { 
                    $DestPath = ((Replace-IllegalCharacters $($1.Compound_Path -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "$($1.Email_subject -replace "`r" -replace "`n" -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")" -replace "$($1.Native_file_name -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")")) -replace "https//").trimEnd().trimEnd("/")
                }
            } else {
                if ($1.compound_path -like "https://forms.office.com*") {
                    $DestPath = "Forms"
                } else {
                    #$DestPath = ((Replace-IllegalCharacters $($1.Compound_Path  -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "'","`'" -replace "$($1.Email_subject -replace "`r" -replace "`n" -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "'","`'")" -replace "$($1.Native_file_name -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "'","`'")")) -replace "https//").trimEnd().trimEnd("/")
                    if ($String) {clv String}
                    if ($escapedSubstring) {clv escapedSubstring}
                    try {
                        $String = $1.Compound_Path
                        if ($1.Email_Subject) {
                            $escapedSubstring = [Regex]::Escape($($1.Email_Subject))   
                        } else {
                            $escapedSubstring = [Regex]::Escape($($1.Native_file_name))  
                        }
                        $DestPath = $(($String -replace $escapedSubstring) -replace "https//").trimEnd().trimEnd("/")
                        if ($DestPath -like "*/RE:" -or $DestPath -like "*/RE: ") {$DestPath = $DestPath.trimEnd("/RE:").trimEnd()}
                        if ($DestPath -match "[`:|\*|\?|`"|<|>|\|]" ) {$DestPath = $DestPath -replace ":" -replace "\*" -replace "\?" -replace '"' -replace "<" -replace ">" -replace "\|"}
                    } catch {
                        $DestPath = ((Replace-IllegalCharacters $($1.Compound_Path  -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "$($1.Email_subject -replace "`r" -replace "`n" -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")" -replace "$($1.Native_file_name -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")")) -replace "https//").trimEnd().trimEnd("/")
                    }
                }
            }
        }

        if ($DestPath -like "https://*") {$DestPath = $DestPath -replace "^https://"}
        
        if ($1.native_extension -eq "Loop" -or $1.native_extension -eq "Whiteboard") {
            $Robocopy = Robocopy "$($ExportRoot)/$($1.Export_native_path -replace "\/[^\/]*$" )" "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)" "$($1.Export_native_path -replace '^(.*[\/])' -replace '.loop','.loop.html' -replace '.whiteboard','.whiteboard.html')" /mov /r:0 /w:0 | out-null # /create /xf * 
            <#
            # Confirm the item was copied
            if (!(test-path "\\?\$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)\$($1.Export_native_path -replace '^(.*[\/])' -replace '.loop','.loop.html' -replace '.whiteboard','.whiteboard.html')")) { #Happy Path
                if (!(test-path -LiteralPath "\\?\$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath  -replace "/","\")\$($1.Export_native_path -replace '^(.*[\/])' -replace '.loop','.loop.html' -replace '.whiteboard','.whiteboard.html')" -ErrorAction SilentlyContinue)) { # Confirm it's not just a longpath issue
                    $1 | Add-Member NoteProperty -Name IntendedPath -Value "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)\$($1.Export_native_path -replace '^(.*[\/])' -replace '.loop','.loop.html' -replace '.whiteboard','.whiteboard.html')" -Force
                    $CopyFailures += $1
                    Write-host -ForegroundColor Red "Failed to copy $($1.file_id). Intended destination`: $($1.IntendedPath)"
                    #$1.Compound_path | set-clipboard
                    #Read-host "Want to debug?"
                }
            }
            #>
        } else {
            $Robocopy = Robocopy "$($ExportRoot)/$($1.Export_native_path -replace "\/[^\/]*$" )" "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)" "$($1.Export_native_path -replace '^(.*[\/])')" /mov /r:0 /w:0 | out-null # /create /xf * 
            <#
            # Confirm the item was copied
            if (!(test-path "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)\$($1.Export_native_path -replace '^(.*[\/])')")) { # Happy Path
                if (!(test-path -LiteralPath "\\?\$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath -replace "/","\")\$($1.Export_native_path -replace '^(.*[\/])')" -ErrorAction SilentlyContinue)) { # Confirm it's not just a longpath issue
                    $1 | Add-Member NoteProperty -Name IntendedPath -Value "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)\$($1.Export_native_path -replace '^(.*[\/])')" -Force
                    $CopyFailures += $1
                    Write-host -ForegroundColor Red "Failed to copy $($1.file_id). Intended destination`: $($1.IntendedPath)"
                    #$1.Compound_path | set-clipboard
                    #Read-host "Want to debug?"
                }
            }
            #>
        }
        #Move-Item -Path "$($ExportRoot)/$($1.Export_native_path)" -Destination "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)" 
    } else {
        # Check to see if there is a match for the file_id in the NativeFiles directory, in light of the bug that appends additional characters to the actual file name
        if ($native) {clv native}
        if ($1.file_id.length -ge 36) {
            $native = Get-ChildItem "$($ExportRoot)\NativeFiles" -file -Recurse | where{$_.basename -like "$($1.file_id)*"}
            #Read-host "Want to debug?"
            if ($native.fullname) {

                if ($DestPath) {clv DestPath}
                if ($1.File_class -eq "Conversation") { 
                    $DestPath = "$($1.Location_name)/M365 Services/Teams/Exchange"
                } else {
                    if ($1.Location -eq "SharePoint") {
                        try {
                            $DestPath = $(Get-LocalSharePointPath -Url $1.Compound_path)
                        } catch {
                            $DestPath = ((Replace-IllegalCharacters $($1.Compound_Path -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "$($1.Email_subject -replace "`r" -replace "`n" -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?')" -replace "$($1.Native_file_name -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?')")) -replace "https//").trimEnd().trimEnd("/")
                        }
                    } else {
                        if ($1.compound_path -like "https://forms.office.com*") {
                            $DestPath = "Forms"
                        } else {
                            #$DestPath = ((Replace-IllegalCharacters $($1.Compound_Path  -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "$($1.Email_subject -replace "`r" -replace "`n" -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?')" -replace "$($1.Native_file_name -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?')")) -replace "https//").trimEnd().trimEnd("/")
                            if ($String) {clv String}
                            if ($escapedSubstring) {clv escapedSubstring}
                            try {
                                $String = $1.Compound_Path
                                if ($1.Email_Subject) {
                                    $escapedSubstring = [Regex]::Escape($($1.Email_Subject))   
                                } else {
                                    $escapedSubstring = [Regex]::Escape($($1.Native_file_name))  
                                }
                                $DestPath = $(($String -replace $escapedSubstring) -replace "https//").trimEnd().trimEnd("/")
                            } catch {
                                $DestPath = ((Replace-IllegalCharacters $($1.Compound_Path  -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "$($1.Email_subject -replace "`r" -replace "`n" -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")" -replace "$($1.Native_file_name -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")")) -replace "https//").trimEnd().trimEnd("/")
                            }
                        }
                    }
                }

                if ($DestPath -like "https://*") {$DestPath = $DestPath -replace "^https://"}
        
                $Robocopy = Robocopy "$($ExportRoot)/$($1.Export_native_path -replace "\/[^\/]*$" )" "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)" "$($native.Name)" /mov /r:0 /w:0 | out-null # /create /xf * 
                # Rename the file to support metadata mapping
                Rename-Item "\\?\$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath -replace "/","\")\$($native.name)" -NewName "$($1.file_id)$($native.Extension)"

                <#
                # Confirm the item was copied
                if (!(test-path "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)\$($native.Name)")) { # Happy Path
                    if (!(test-path -LiteralPath "\\?\$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath -replace "/","\")\$($native.Name)" -ErrorAction SilentlyContinue)) { # Confirm it's not just a longpath issue                
                        $1 | Add-Member NoteProperty -Name IntendedPath -Value "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)\$($1.Export_native_path -replace '^(.*[\/])')" -Force
                        $CopyFailures += $1
                        Write-host -ForegroundColor Red "Failed to copy $($1.file_id). Intended destination`: $($1.IntendedPath)"
                        $ItemsToCheck += $1
                        Write-host -ForegroundColor DarkRed "Could not find native for $($1.file_id)"
                        #$1.Compound_path | set-clipboard
                        #Read-host "Want to debug?"   
                    }
                } else {
                    Start-sleep 1
                    # If copy is successful, rename the file to support metadata mapping
                    Rename-Item "\\?\$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath -replace "/","\")\$($native.name)" -NewName "$($1.file_id)$($native.Extension)"
                }
                #>            
            } else {
                $ItemsToCheck += $1
                #Write-host -ForegroundColor DarkRed "Could not find native for $($1.file_id)"
            }
        } else { 
            $ItemsToCheck += $1
            #Write-host -ForegroundColor DarkRed "Could not find native for $($1.file_id)"
        }
    }

    $i++
}


# Check remaning Natives to verify whether they should be in scope
$SuppressedNatives = Get-ChildItem "$($ExportRoot)/NativeFiles" -File -Recurse

# Build hash sets for $data.file_id and $data.input_file_id
$dataFileIDs = @{}
$dataInputFileIDs = @{}
foreach ($item in $data) {
    if ($item.file_id) { $dataFileIDs[$item.file_id] = $true }
    if ($item.input_file_id) { $dataInputFileIDs[$item.input_file_id] = $true }
}

$UnlistedItems = $SuppressedNatives | Where-Object { 
    $_.BaseName -and (-not $dataFileIDs.ContainsKey($_.BaseName))
}

$InputFileIdCheck = $SuppressedNatives | Where-Object { 
    $_.BaseName -and $dataInputFileIDs.ContainsKey($_.BaseName)
}

# For Concern, use hash sets for fast lookups
$unlistedSet = @{}
$inputFileIdSet = @{}
foreach ($item in $UnlistedItems) { $unlistedSet[$item.BaseName] = $true }
foreach ($item in $InputFileIdCheck) { $inputFileIdSet[$item.BaseName] = $true }
$Concern = $SuppressedNatives | Where-Object {
    $_.BaseName -and (
        (-not $unlistedSet.ContainsKey($_.BaseName)) -or ($inputFileIdSet.ContainsKey($_.BaseName))
    )
}
   if ($Concern) {
    $retrySet = @{}
    foreach ($item in $Concern) { $retrySet[$item.BaseName] = $true }
    $Retry = $Data | Where-Object {
        $retrySet.ContainsKey($_.file_id) -or $retrySet.ContainsKey($_.input_file_id)
	}
    if ($Retry.file_id) {
        $i = 1
        $RetryCount = @($Retry).count
        foreach ($1 in $Retry) {
                Write-host -ForegroundColor Gray "Retrying $($i) of $($RetryCount)`: $($1.file_id)"
                if ($Dest) {clv Dest}
                # If there is no Custodian_LastFirst value, move the files to a subfolder called "Microsoft"
                if ($1.Custodian_LastFirst) {$Dest = $1.Custodian_LastFirst} else {$Dest = "Microsoft"}
                if (!(test-path "$($ExportRoot)\$($Dest)")) {
                    New-item -ItemType Directory -Path $ExportRoot -Name $Dest
                }
                #Move-Item -Path "$($ExportRoot)/$($1.Export_native_path)" -Destination "$($ExportRoot)/$($Dest)"
                if ($DestPath) {clv DestPath}
                if ($1.File_class -eq "Conversation") { 
                    $DestPath = "$($1.Location_name)/M365 Services/Teams/Exchange"
                } else {
                    if ($1.Location -eq "SharePoint") {
                        try {
                            $DestPath = $(Get-LocalSharePointPath -Url $1.Compound_path)
                        } catch { 
                            $DestPath = ((Replace-IllegalCharacters $($1.Compound_Path -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "$($1.Email_subject -replace "`r" -replace "`n" -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")" -replace "$($1.Native_file_name -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")")) -replace "https//").trimEnd().trimEnd("/")
                        }
                    } else {
                        if ($1.compound_path -like "https://forms.office.com*") {
                            $DestPath = "Forms"
                        } else {
                            #$DestPath = ((Replace-IllegalCharacters $($1.Compound_Path  -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "'","`'" -replace "$($1.Email_subject -replace "`r" -replace "`n" -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "'","`'")" -replace "$($1.Native_file_name -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "'","`'")")) -replace "https//").trimEnd().trimEnd("/")
                            if ($String) {clv String}
                            if ($escapedSubstring) {clv escapedSubstring}
                            try {
                                $String = $1.Compound_Path
                                if ($1.Email_Subject) {
                                    $escapedSubstring = [Regex]::Escape($($1.Email_Subject))   
                                } else {
                                    $escapedSubstring = [Regex]::Escape($($1.Native_file_name))  
                                }
                                $DestPath = $(($String -replace $escapedSubstring) -replace "https//").trimEnd().trimEnd("/")
                            } catch {
                                $DestPath = ((Replace-IllegalCharacters $($1.Compound_Path  -replace '\[' -replace '\]' -replace '\&' -replace "`r" -replace "`n" -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*" -replace "$($1.Email_subject -replace "`r" -replace "`n" -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")" -replace "$($1.Native_file_name -replace '\[' -replace '\]' -replace '\&' -replace '\(' -replace '\)' -replace '\+' -replace '\%' -replace '=' -replace '{' -replace '}' -replace '\#' -replace ';' -replace ':' -replace '\?' -replace "\*","\*")")) -replace "https//").trimEnd().trimEnd("/")
                            }
                        }
                    }
                }

                if ($DestPath -like "https://*") {$DestPath = $DestPath -replace "^https://"}
                
                if ($target) {clv target}
                if ($type) {clv type}
                $target = $Concern | where{$_.basename -eq $1.file_id}
                $type = 1
                if ($target -eq $null) {
                    $target = $Concern | where{$_.basename -eq $1.Input_file_id}
                    if ($target) {$type = 2}
                }

                if ($target -ne $null) {
                    if (Test-Path "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)") {
                        if ($type -eq 2) {
                            # Rename the item first so longpaths aren't an issue
                            Rename-Item "$($target.FullName)" -NewName "$($1.file_id).$($1.native_extension)"
                            Move-Item "$($target.Directory)\$($1.file_id).$($1.native_extension)" -Destination "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)" -Force  
                        } else {
                            Move-Item "$($target.fullname)" -Destination "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)" -Force                        
                        }
                    } else {
                        if ($type -eq 2) {
                            # Rename the item first so longpaths aren't an issue
                            Rename-Item "$($target.FullName)" -NewName "$($1.file_id).$($1.native_extension)"
                            Robocopy "$($target.directory)" "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)" "$($1.file_id).$($1.native_extension)" /mov
                        } else {
                            Robocopy "$($target.directory)" "$($ExportRoot)\$($Dest)\$($1.Location)\$($DestPath)" "$($target.name)" /mov
                        }
                    }                
                }
                $i++
            }

        }
    }
    # Ensure $UnlistedItems is always treated as an array
	if ($null -ne $UnlistedItems) {
    # Force array context
    $UnlistedItemsArr = @($UnlistedItems) | Where-Object { $_ -ne $null }
    $unlistedCount = $UnlistedItemsArr.Count
    Write-Host "DEBUG: `$UnlistedItemsArr.Count = $unlistedCount"
    if ($unlistedCount -gt 0) {
        Write-Host -ForegroundColor Cyan "Found $unlistedCount item(s) not listed on the load file in the NativeFiles directory."
        $UnlistedItemsArr | Select-Object BaseName, Name, FullName | Export-Csv "$ExportRoot\$jobID Items not listed on load file.csv" -Encoding UTF8 -NoTypeInformation

        $suppressPath = "$ExportRoot/SUPPRESS/UnlistedItems"
        if (-not (Test-Path $suppressPath)) {
            New-Item -ItemType Directory -Path $suppressPath -Force | Out-Null
        }
        foreach ($U in $UnlistedItemsArr) {
            Move-Item -Path $U.FullName -Destination $suppressPath
        }
    } else {
        Write-Host "DEBUG: No unlisted items to report."
    }
} else {
    Write-Host "DEBUG: `$UnlistedItems is `$null."
}



<# We do not need these reports because we now have the Download Failures report and the Unlisted Items report, and the MissingNativesReport is often full of false positives resulting from test-path implementation problems.
if ($ItemsToCheck) {
    $ItemsToCheck | select * | Export-csv "$($ExportRoot)\MissingNativesReport.csv" -NoTypeInformation -Encoding UTF8 -Append
}

if ($CopyFailures) {
    $CopyFailures | select * | Export-csv "$($ExportRoot)\MissingNativesReport.csv" -NoTypeInformation -Encoding UTF8 -Append
}
#>

#$data | where{$_.SUPPRESS} | Select * | Export-csv "$($ExportRoot)\SuppressedItems.csv" -NoTypeInformation -Encoding UTF8 -Append

$data | Select File_ID, `
        Input_File_ID, `
        Custodian, `
        Location, `
        Location_Name, `
        File_Class, `
        Native_Extension, `
        Export_Native_Path, `
        Compound_Path, `
        Date, `
        Email_Participants, `
        Email_Participant_Domains, `
        Email_subject, `
        Error_code, `
        Group_Id, `
        IsModernAttachment, `
        Item_class, `
        ModernAttachment_ParentId, `
        Native_file_name, `
        Native_size, `
        Tags, `
        Native_MD5, `
        Native_Sha_256, `
        Parent_ID, `
        Family_ID, `
        Immutable_ID | Export-csv "$($ExportRoot)\$((get-date).tostring("yyyyMMdd_HHmm")) Upload Overlay Foundation.csv" -NoTypeInformation -Encoding UTF8 -Append

# Move remaining items to SUPPRESS folder
if (test-path "$($ExportRoot)/NativeFiles") {
    robocopy "$($ExportRoot)/NativeFiles" "$($ExportRoot)/SUPPRESS/NativeFiles" * /mov | out-null
    Remove-Item -Path "$($ExportRoot)/NativeFiles"
    $Investigate = gci "$($ExportRoot)/SUPPRESS/NativeFiles" -File -Recurse
    if (@($Investigate).count -gt 0) {
        Write-Host -ForegroundColor Magenta "Unhandled exceptions remain at $($ExportRoot)/SUPPRESS/NativeFiles. Please investigate."
    }
}
if (test-path "$($ExportRoot)/Error_Files") {
    robocopy "$($ExportRoot)/Error_Files" "$($ExportRoot)/SUPPRESS/Error_Files" * /mov | out-null
    Remove-Item -Path "$($ExportRoot)/Error_Files"
}
if (test-path "$($ExportRoot)/Extracted_text_files") {
    robocopy "$($ExportRoot)/Extracted_text_files" "$($ExportRoot)/SUPPRESS/Extracted_text_files" * /mov | out-null
    Remove-Item -Path "$($ExportRoot)/Extracted_text_files"
}

Write-host "End Time`: $(get-date)"

Read-host "Done! Hit Enter to close"
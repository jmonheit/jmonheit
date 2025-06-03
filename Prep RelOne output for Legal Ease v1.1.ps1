<#
    This script will connect to EOP and present a filterable list for the user to identify the corresponding Purview case related to the export set.
    Then it will ingest the Relativity dat and convert it to a csv that matches our standard AEDOutput fields.
    Then it will derive some statistics from the dat and add a row for the job to the AEDOutputStats table.
    Finally, it will tell the user precisely where the export set must be uploaded in celalitedpexports for Legal Ease ingestion.
#>


if ("$($env:psmodulepath)".ToLower().Contains("\\lca-lit\litigation\ediscovery\modules") -eq $false)
{
    write-warning "Adding '\\lca-lit\litigation\ediscovery\modules' to your PSModulePath"
    $env:PSModulePath = "$($env:PSModulePath);\\lca-lit\litigation\ediscovery\modules"
}

write-host -ForegroundColor Cyan -NoNewline "Loading celalit module..."
Import-module celalit -WarningAction SilentlyContinue -Verbose:$false
write-host -ForegroundColor Green "Loaded"

#Import-Module cela-ediscovery

#Connect-AzAccount -Subscription "CELA eDiscovery" -TenantId "72f988bf-86f1-41af-91ab-2d7cd011db47" -UseDeviceAuthentication
$connectedToAzure = $false
$azToken = Get-AzAccessToken -ErrorAction SilentlyContinue
if ($azToken -ne $null)
{
    $azAccount = Get-AzContext -ErrorAction SilentlyContinue
    if ($azAccount -ne $null)
    {
        if ($azAccount.Account -ne $null)
        {
            if ([string]::IsNullOrEmpty($azAccount.Account.Id) -eq $false)
            {
                write-host -ForegroundColor Cyan -NoNewline "You are logged into Azure as "
                write-host -ForegroundColor Yellow -NoNewline "'"
                write-host -ForegroundColor Green -NoNewline "$($azAccount.Account.Id)"
                write-host -ForegroundColor Yellow -NoNewline "'"
                write-host ""
                $azSub = Select-AzSubscription "9421132f-2f25-462b-a8b9-70a01fc20bfa"
                $connectedToAzure = $true
            }
        }
    }
}
if ($connectedToAzure -eq $false)
{
    write-host -ForegroundColor Yellow "Please connect to azure..."
    $azAccount = Connect-AzAccount -TenantId "72f988bf-86f1-41af-91ab-2d7cd011db47" -Subscription "9421132f-2f25-462b-a8b9-70a01fc20bfa" -ErrorAction SilentlyContinue

    if ($azAccount -eq $nul)
    {
        write-warning "Failed to connect to Azure."
        return $false
    } else
    {
        $azAccount = Get-AzContext -ErrorAction SilentlyContinue
        if ($azAccount -ne $null)
        {
            if ($azAccount.Account -ne $null)
            {
                if ([string]::IsNullOrEmpty($azAccount.Account.Id) -eq $false)
                {
                    write-host -ForegroundColor Cyan -NoNewline "You are logged into Azure as "
                    write-host -ForegroundColor Yellow -NoNewline "'"
                    write-host -ForegroundColor Green -NoNewline "$($azAccount.Account.Id)"
                    write-host -ForegroundColor Yellow -NoNewline "'"
                    write-host ""
                    $azSub = Select-AzSubscription "9421132f-2f25-462b-a8b9-70a01fc20bfa"
                    $connectedToAzure = $true
                }
            }
        }
    }
}

$currentWarningLevel = "$($WarningPreference)"
$WarningPreference = "SilentlyContinue"
$currentIaaSVM = Get-CELALIT-IaaS-Metadata -WarningAction SilentlyContinue -Verbose:$false
$WarningPreference = "$($currentWarningLevel)"
if ($currentIaaSVM -ne $null)
{
    write-host -ForegroundColor Cyan -NoNewLine "Running on IaaS VM "
    write-host -ForegroundColor Yellow -NoNewLine "'"
    write-host -ForegroundColor Green -NoNewLine "$($currentIaaSVM.name)"
    write-host -ForegroundColor Yellow -NoNewLine "'"
    write-host ""
} else
{
    write-host -ForegroundColor Magenta "Not running on an IaaS VM"
}


write-host -ForegroundColor Cyan -NoNewLine "Connecting to exchange online..."
Connect-IPPSSession
write-host -ForegroundColor Green "Connected"

write-host "Getting Active AdvancedeDiscovery cases..."
$cases = get-complianceCase -caseType AdvancedeDiscovery | where{$_.Status -eq "Active"}
write-host "Got $($cases.Count.ToString('#,###')) active AdvancedeDiscovery cases."

Write-host -ForegroundColor Cyan "Select the corresponding case from the list and hit OK."
do {
    $case = $cases | select Name, Identity, CreatedDateTime, LastModifiedBy | Out-GridView -PassThru -Title "Select the corresponding case from the list and hit OK."
} until ($case)

Write-host -ForegroundColor Gray "Case name`: $($case.name)`r`nCase ID`: $($case.identity)"

$jobID = (get-date).ToString("yyyyMMdd-HHmm")

 Write-Host -ForegroundColor Magenta "For automated metrics, please indicate whether this job is being run for test purposes only."
do {
    $testOnlyUserInput = (Read-host "Is this a test case? Y/N").ToUpper()
} until ($testOnlyUserInput -in ("Y", "N"))

$dat = (Read-host "Enter the path to your .dat").trim('"')

#$dat = "C:\test\RelOne\Legal Ease ingestion test 1\SavedSearch_1422142_export.dat"

$ExportRoot = $dat -replace "\\[^\\]*$" -replace "\/[^\/]*$" 

#$row1 = gc $dat | select -first 1


function Import-DatFileWithQualifier {
    param(
        [Parameter(Mandatory=$true)]
        [string]$Path,

        [char]$Delimiter = [char]0x14,
        [char]$TextQualifier = 'þ'
    )

    if (-not (Test-Path $Path)) {
        throw "File not found: $Path"
    }

    # Read the file and remove the text qualifiers
    $processedLines = Get-Content $Path | ForEach-Object {
        $_.Trim() -replace [regex]::Escape($TextQualifier), ''
    }

    # Join the processed lines into a single string for Import-Csv
    $processedCsv = $processedLines -join "`n"

    # Convert the processed string into objects using Import-Csv
    $importedData = $processedCsv | ConvertFrom-Csv -Delimiter $Delimiter

    return $importedData
}


$data = Import-DatFileWithQualifier -Path $dat 

$data | select @{l="Row_number"; e={$_."Control Number"}}, `
    #@{l="File_ID"; e={$_."Purview CDS Filename" -split "\." | Select -first 1}}, `
    @{l="File_ID"; e={$_."Control Number"}}, `
    @{l="Immutable_ID"; e={$_."Control Number"}}, `
    @{l="File_class"; e={$_."Purview File Class"}}, `
    @{l="Family_ID"; e={$_."Family Group"}}, `
    @{l="Native_MD5"; e={$_."MD5 Hash"}}, `
    @{l="Native_SHA_256"; e={$_."SHA256 Hash"}}, `
    Location_name, `
    Location, `
    Custodian, `
    @{l="Compound_path"; e={$_."Purview Compound Path"}}, `
    Container_ID, `
    @{l="Input_file_ID"; e={$_."Parent Document ID"}}, `
    Input_path, `
    Load_ID, `
    @{l="Date"; e={$_."Primary Date/Time"}}, `
    @{l="Item_class"; e={$_."Purview Item Class"}}, `
    Message_kind, `
    @{l="Email_to"; e={$_."Email To"}}, `
    @{l="Email_cc"; e={$_."Email CC"}}, `
    @{l="Email_bcc"; e={$_."Email BCC"}}, `
    @{l="Email_subject"; e={$_."Email Subject"}}, `
    @{l="Email_date_sent"; e={$_."Email Sent Date"}}, `
    @{l="Email_sender"; e={$_."Email From"}}, `
    @{l="Email_sender_domain"; e={$_."Sender Domain"}}, `
    Email_recipients, `
    Email_recipient_domains, `
    Email_participants, `
    Email_participant_domains, `
    Thread_participants, `
    Thread_participant_domains, `
    @{l="Email_date_received"; e={$_."Email Sent Date"}}, `
    @{l="Email_action"; e={$_."Email Action"}}, `
    @{l="Email_has_attachment"; e={$_."Email Has Attachments"}}, `
    @{l="Email_importance"; e={$_."Email Sensitivity"}}, `
    Email_security, `
    @{l="Email_sensitivity"; e={$_."Email Sensitivity"}}, `
    @{l="Email_read_receipt_requested"; e={$_."Email Read Receipt Requested"}}, `
    Email_delivery_receipt_requested, `
    Email_internet_headers, `
    @{l="Email_message_ID"; e={$_."Message ID"}}, `
    @{l="In_reply_to_ID"; e={$_."Email In Reply to ID"}}, `
    @{l="Recipient_count"; e={$_."Email Recipient Count"}}, `
    Family_size, `
    @{l="Conversation_index"; e={$_."Conversation Index"}}, `
    @{l="Email_conversation_ID"; e={$_."Conversation"}}, `
    @{l="Meeting_start_date"; e={$_."Meeting Start Date/Time"}}, `
    @{l="Meeting_end_date"; e={$_."Meeting End Date/Time"}}, `
    @{l="Email_set"; e={$_."ETND::Email Thread Group"}}, `
    Family_duplicate_set, `
    Email_level, `
    @{l="Email_thread"; e={$_."ETND::Email Thread Hash"}}, `
    @{l="Inclusive_type"; e={$_."ETND::Inclusive Reason"}}, `
    Parent_node, `
    Set_order_inclusives_first, `
    @{l="Native_file_name"; e={$_."File Name"}}, `
    @{l="Native_type"; e={$_."File Type"}}, `
    @{l="Native_extension"; e={$_."File Extension"}}, `
    @{l="Native_size"; e={$_."File Size"}}, `
    @{l="Doc_date_modified"; e={$_."Last Modified Date/Time"}}, `
    @{l="Doc_date_created"; e={$_."Created Date/Time"}}, `
    @{l="Doc_modified_by"; e={$_."System Last Modified By"}}, `
    @{l="Doc_authors"; e={$_."Author"}}, `
    @{l="Doc_comments"; e={$_."Microsoft Comments"}}, `
    Doc_keywords, `
    Doc_version, `
    @{l="Doc_subject"; e={$_."Document Subject"}}, `
    Doc_template, `
    @{l="Doc_title"; e={$_."Unified Title"}}, `
    Doc_company, `
    Doc_last_saved_by, `
    O365_date_modified, `
    O365_date_created, `
    O365_modified_by, `
    O365_authors, `
    O365_created_by, `
    @{l="File_system_date_modified"; e={$_."System Last Modified On"}}, `
    @{l="File_system_date_created"; e={$_."System Created On"}}, `
    Marked_as_pivot, `
    @{l="Similarity_percent"; e={$_."Textual Near Duplicate Similarity"}}, `
    @{l="Pivot_ID"; e={$_."Textual Near Duplicate Principal"}}, `
    Set_ID, `
    @{l="ND_set"; e={$_."Textual Near Duplicate Group"}}, `
    Duplicate_subset, `
    @{l="Dominant_theme"; e={$_."Conceptual Index"}}, `
    Themes_list, `
    ND_ET_sort_excl_attach, `
    ND_ET_sort_incl_attach, `
    Tags, `
    Potentially_privileged, `
    Extracted_content_type, `
    Compliance_labels, `
    @{l="Deduped_custodians"; e={$_."DeDuped Custodians"}}, `
    Deduped_file_IDs, `
    @{l="Deduped_compound_path"; e={$_."DeDuped Paths"}}, `
    @{l="Extracted_text_length"; e={$([int][decimal]($_."Extracted Text Size in KB")*1024)}}, `
    Has_text, `
    Word_count, `
    Error_Ignored, `
    Error_code, `
    Was_Remediated, `
    @{l="Is_representative"; e={$_."ETND::Inclusive Email"}}, `
    @{l="Export_native_path"; e={$($_."FILE_PATH" -replace '\\','/').trimstart('\/.')}}, `
    Converted_file_path, `
    Redacted_file_path, `
    @{l="Extracted_text_path"; e={$($_."Extracted Text" -replace '\\','/').trimstart('\/.')}}, `
    Redacted_text_path, `
    Original_input_path, `
    Original_file_extension, `
    @{l="Group_Id"; e={$_."Purview Group ID"}}, `
    @{l="ModernAttachment_ParentId"; e={$_."Purview Group ID"}}, `
    Version_GroupId, `
    Version_Number, `
    TeamName, `
    Channel_Name, `
    ConversationName, `
    ConversationType, `
    ContainsDeletedMessage, `
    ContainsEditedMessage, `
    TeamsAnnoucementTitle, `
    SPOUniqueId, `
    SPOPreservationOriginalDocumentUniqueId, `
    @{l="True_Family_ID"; e={$_."Family Group"}}, `
    @{l="CELA_File_ID"; e={$_."Purview CDS Filename" -split "\." | Select -first 1}}, `
    #@{l="CELA_File_ID"; e={$_."Control Number"}}, `
    @{l="CELA_Family_ID"; e={$_."Family Group"}}, `
    CELA_Set_ID, `
    CELA_Sort_Order, `
    CELA_Family_Size, `
    REPROCESS, `
    SUPPRESS, `
    FOR_REVIEW, `
    @{l="Custodian_LastFirst"; e={$_."Custodian"}}, `
    @{l="ALL_CUSTODIANS"; e={$_."All Custodians"}}, `
    CaseID, `
    ReviewSetID, `
    ExportID, `
    @{l="Is_Parent"; e={$_."Is Parent"}}, `
    Is_Container, `
    From_Container, `
    Missing_Native, `
    NCDS, `
    Text_Port_Path | Export-csv -Encoding UTF8 "$($ExportRoot)\$($jobID)_Load_File.csv" -NoTypeInformation


$ReviewableItems = $data.count
Write-host -ForegroundColor Gray "Identifying total reviewable volume"
$logFile = [System.IO.Path]::GetTempFileName()
$matchingDirs = Get-ChildItem -Path $ExportRoot -Directory -Recurse | Where-Object { $_.Name -eq "Natives" }
$totalBytes = 0
foreach ($dir in $matchingDirs) {
    robocopy.exe $dir.FullName NULL /mt:128 /L /E /NJH /BYTES /FP /NC /NDL /XJ /R:0 /W:0 /LOG:$logFile | Out-Null
    $bytesLine = Select-String -Path $logFile -Pattern "Bytes\s+:.*" | Select-Object -First 1
    if ($bytesLine -and $bytesLine.Line -match "Bytes\s+:\s+(\d+)") {
        [Int64]$dirBytes = [Int64]$matches[1]
        $totalBytes += $dirBytes
    }
}  
$ReviewGB = [math]::Round($totalBytes / 1GB, 2)
Remove-Item -Path $logFile -Force
if ([int]$((get-date).ToString("MM")) -ge 7) {$FY = $([int]((get-date).toString("yyyy"))+1)} else {$FY = $([int]((get-date).toString("yyyy")))} 

$storageAccountName = "celalitedpexports"
$Container = "$($case.name.Replace(' ','').Replace('--','-').Replace('.','').Replace('_','').ToLower())" 
$ExportID = $jobID
$CaseID = $case.Identity
$CaseName = $case.name
$LastModifiedDate = $(get-date)
$StorageLocation = "https://$($storageAccountName)/$($container)"
$ModuleVersion = "r1v1"
if ($testOnlyUserInput -eq "Y") {$test = '1'} else {$test = '0'}
$IndividualItemCount = $ReviewableItems
$IndividualItemBytes = $totalBytes
$Custodians = ($Data.custodian | sort -Unique) -join '; '
$PM = $env:username


$SQLStatement = @"
INSERT INTO AEDOutputStats (PM, CaseName, Date, Export_Batch, Reviewable_Items, Review_GB, Custodians, Individual_Item_Count, Individual_Item_Volume_in_Bytes, Case_ID, Test, FiscalYear, ModuleVersion, StorageLocationURL)
VALUES ('$PM', '$CaseName', '$(($LastModifiedDate).toshortdatestring())', '$ExportID', '$ReviewableItems', '$ReviewGB', '$($Custodians -replace "'","''")', '$IndividualItemCount', '$IndividualItemBytes', '$CaseID', '$test', '$FY', '$ModuleVersion', '$StorageLocation')
"@

write-host -ForegroundColor Cyan -NoNewline "Updating SQL..."
$sqlResult = Execute-CELALIT-SQLStatement `
    -SQLServerName "edisc.database.windows.net" `
    -SQLDatabaseName "AEDOutput" `
    -SQLStatement "$($SQLStatement)"

if ($sqlResult -ne $null)
{
    if ($sqlResult.Successful -eq $true)
    {
        write-host -ForegroundColor Green "SQL Updated"
    } else
    {
        write-host -ForegroundColor Red "SQL Update failed"
        read-host "Press [enter]"
    }
} else
{
    write-host -ForegroundColor Red "SQL Update failed"
    read-host "Press [enter]"
}

Write-host -ForegroundColor Yellow "Required Storage Container:
    Storage Account`:`tcelalitedpexports
    Container Name`:`t`t$($Container)
    Job Folder`:`t`t`t$($jobID)

It is ABSOLUTELY CRITICAL that the Container name is exactly what is written above, and that the data is uploaded to a subfolder in the root of the container called `"$($jobID).`"

If the load file is not in the root of the $($jobID) folder and the directory structure is not precisely as downloaded from Relativity, the Legal Ease ingestion will not work.
" 


"Required Storage Container:
    Storage Account`:`tcelalitedpexports
    Container Name`:`t`t$($Container)
    Job Folder`:`t`t`t$($jobID)

It is ABSOLUTELY CRITICAL that the Container name is exactly what is written above, and that the data is uploaded to a subfolder in the root of the container called `"$($jobID).`"

If the load file is not in the root of the $($jobID) folder and the directory structure is not precisely as downloaded from Relativity, the Legal Ease ingestion will not work.
" | Out-file "$($ExportRoot)\$((get-date).tostring("yyyyMMdd-HHmm"))_ContainerUploadDetails.txt"

<#
### Do we want to allocate the storageaccount/container and upload automagically?
write-host -ForegroundColor Cyan -NoNewline "Looking for storage account "
write-host -ForegroundColor Yellow -NoNewline "'"
write-host -ForegroundColor Green -NoNewline "$($storageAccountName)"
write-host -ForegroundColor Yellow -NoNewline "'"
write-host -ForegroundColor Cyan -NoNewline "..."

$currentWarningLevel = "$($WarningPreference)"
$WarningPreference = "SilentlyContinue"

$storageAccountObj = Get-CELALIT-Storage-Account `
    -StorageAccountName "$($storageAccountName)"

$WarningPreference = "$($currentWarningLevel)"

if ($storageAccountObj -ne $null)
{
    write-host -ForegroundColor Green -NoNewline "Found"
    write-host ""

    write-host -ForegroundColor Cyan -NoNewline "Looking for container "
    write-host -ForegroundColor Yellow -NoNewline "'"
    write-host -ForegroundColor Green -NoNewline "$($Container)"
    write-host -ForegroundColor Yellow -NoNewline "'"
    write-host -ForegroundColor Cyan -NoNewline "..."

    $currentWarningLevel = "$($WarningPreference)"
    $WarningPreference = "SilentlyContinue"

    $storageAccountContainerObj = Get-CELALIT-Storage-Container `
        -StorageAccountName "$($storageAccountName)" `
        -ContainerName "$($Container)"

    $WarningPreference = "$($currentWarningLevel)"

    if ($storageAccountContainerObj -ne $null)
    {
        write-host -ForegroundColor Green -NoNewline "Found"
        write-host ""
    } else
    {
        write-host -ForegroundColor Yellow -NoNewline "Not Found"
        write-host ""

        $createContainer = $false
        while ($true)
        {
            write-host -ForegroundColor Cyan -NoNewline "Do you want to create the container "
            write-host -ForegroundColor Yellow -NoNewline "'"
            write-host -ForegroundColor Green -NoNewline "$($Container)"
            write-host -ForegroundColor Yellow -NoNewline "'"
            write-host -ForegroundColor Cyan -NoNewline "? "
            $create = read-host "(Yes/[No])"
            if ([string]::IsNullOrEmpty("$($create)") -eq $false)
            {
                if ($create -iin @("y","yes","n","no"))
                {
                    if ($create -iin @("y","yes"))
                    {
                        $createContainer = $true
                        break;
                    }

                    break;
                }

                write-warning "Invalid value specified - enter 'Yes' or 'No'"
            }
        }

        if ($createContainer -eq $true)
        {  
            write-host -ForegroundColor Cyan -NoNewline "Creating container "
            write-host -ForegroundColor Yellow -NoNewline "'"
            write-host -ForegroundColor Green -NoNewline "$($Container)"
            write-host -ForegroundColor Yellow -NoNewline "'"
            write-host -ForegroundColor Cyan -NoNewline "..."

            $currentWarningLevel = "$($WarningPreference)"
            $WarningPreference = "SilentlyContinue"

            $containerObj = Create-CELALIT-Storage-Container `
                -StorageAccountName "$($storageAccountName)" `
                -ContainerName "$($Container)" `
                -Metadata @{
                    "CreatedBy"="$($azAccount.Account.Id)";
                    "CaseID"="$($CaseID)"
                    "CaseName"="$($CaseName)"
                }
            
            $WarningPreference = "$($currentWarningLevel)"
          
            if ($containerObj -ne $null)
            {
                write-host -ForegroundColor Green -NoNewline "Created"
                write-host ""

                $currentWarningLevel = "$($WarningPreference)"
                $WarningPreference = "SilentlyContinue"

                $storageAccountContainerObj = Get-CELALIT-Storage-Container `
                    -StorageAccountName "$($storageAccountName)" `
                    -ContainerName "$($Container)"

                $WarningPreference = "$($currentWarningLevel)"

            } else
            {
                write-host -ForegroundColor Yellow -NoNewline "Failed"
                write-host ""
                read-host "Press [enter]"
            }
        }
    }

    if ($storageAccountContainerObj -ne $null)
    {
        ### upload content?

    }

} else
{
    write-host -ForegroundColor Red -NoNewline "Not Found"
    write-host ""
    read-host "Press [enter]"
}

#>

Read-host "Hit Enter to exit"